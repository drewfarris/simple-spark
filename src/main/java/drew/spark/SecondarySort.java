package drew.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/** Read the data file containing the top most-populous cities in the United States and
 *  produce the top 5 cities, in sorted order, for each state.
 *
 *  A few takeaways:
 *  - Use a partitioner to control grouping of objects so that the all get sent to the same partition.
 *  - Use a priority queue in a reducer (TopNFunction), to keep only the highest scoring objects.
 *  - When creating a new collection from a priority queue, via constructor (which calls Collections.addAll(),
 *    it is necessary to sort the results.
 *
 */
public class SecondarySort {

    static final Logger log = LoggerFactory.getLogger(SecondarySort.class);

    @Parameter(names = "-i", description = "Input File")
    String inputPath = "data/state-city-pop.csv";

    @Parameter(names = "-o", description = "Output File")
    String outputPath = "output";

    @Parameter(names = "-p", description = "Number of partitions")
    int partitions = 1;

    @Parameter(names = "-t", description = "Number of top results")
    int topCount = 5;

    @Parameter(names = "-m", description = "Master")
    String master = "local[*]";

    @Parameter(names = "-n", description = "Name")
    String appName = "SecondarySort";

    final SparkSession spark;
    final JavaSparkContext sparkContext;

    public SecondarySort(String[] args) {
        JCommander.newBuilder()
                .addObject(this)
                .build()
                .parse(args);

        spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        sparkContext = new JavaSparkContext(spark.sparkContext());
    }

    public void execute() {
        JavaRDD<String> input = sparkContext.textFile(inputPath);
        input.repartition(10);

        JavaRDD<String[]> rawDataArray = input.map(line -> line.split(","));
        // a pair is necessary for the `partitionBy` call below.
        JavaPairRDD<LocationKey,Void> locationData = rawDataArray.flatMapToPair(LocationKey::flatMap);

        int sortPartitions = Math.min(locationData.getNumPartitions(), partitions);
        JavaPairRDD<LocationKey,Void> sortedLocationData = locationData.partitionBy(new StatePartitioner(sortPartitions));

        final CountComparator countComparator = new CountComparator();
        JavaPairRDD<String, PriorityQueue<LocationKey>> stateByLocation = sortedLocationData
                .mapToPair(
                        new ExtractStateAndCountToQueue(countComparator)
                )
                .reduceByKey(
                        new TopNFunction<>(topCount, countComparator)
                );

        JavaRDD<LocationKey> finalResults = stateByLocation.flatMap(new CleanupResult(countComparator.reversed()));
        finalResults.saveAsTextFile(outputPath);
    }

    /** Extract results from the priority queue and produce tuples of LocationKey for the final results */
    public static class CleanupResult implements FlatMapFunction<Tuple2<String, PriorityQueue<LocationKey>>, LocationKey> {

        public Comparator<LocationKey> comparator;

        public CleanupResult(Comparator<LocationKey> comparator) {
            this.comparator = comparator;
        }

        @Override
        public Iterator<LocationKey> call(Tuple2<String, PriorityQueue<LocationKey>> t) {
            final List<LocationKey> k = new ArrayList<>(t._2());
            k.sort(comparator);
            return k.iterator();
        }
    }

    /** Retain the top N elements in a PriorityQueue use with foldByKey or reduceByKey */
    public static class TopNFunction<N> implements Function2<PriorityQueue<N>,PriorityQueue<N>,PriorityQueue<N>> {
        final int topCount;
        final Comparator<N> comparator;

        public TopNFunction(int topCount, Comparator<N> comparator) {
            this.topCount = topCount;
            this.comparator = comparator;
        }

        @Override
        public PriorityQueue<N> call(PriorityQueue<N> s1, PriorityQueue<N> s2) {
            final int expectedSize = s1.size() + s2.size();
            final PriorityQueue<N> merged = new PriorityQueue<>(expectedSize, comparator);
            merged.addAll(s1);
            merged.addAll(s2);
            while (merged.size() > topCount) {
                merged.remove();
            }
            return merged;
        }
    }

    /** In preparation for the reduceByKey, we need to generate an input pair rdd with the priority queue objects
     *  in place */
    public static class ExtractStateAndCountToQueue implements PairFunction<Tuple2<LocationKey, Void>, String, PriorityQueue<LocationKey>> {
        final Comparator<LocationKey> comparator;

        public ExtractStateAndCountToQueue(Comparator<LocationKey> comparator) {
            this.comparator = comparator;
        }

        @Override
        public Tuple2<String, PriorityQueue<LocationKey>> call(Tuple2<LocationKey, Void> t) {
            final PriorityQueue<LocationKey> singletonQueue = new PriorityQueue<>(1, comparator);
            singletonQueue.add(t._1);
            return new Tuple2<>(t._1.state,singletonQueue);
        }
    }

    /** For sorting a priority queue, we want the smallest values first, so they will get
     *  popped off the list first when manually trimming the priority queue to max size */
    public static class CountComparator implements Serializable, Comparator<LocationKey> {
        @Override
        public int compare(LocationKey o1, LocationKey o2) {
            return Long.compare(o1.count, o2.count);
        }
    }

    public static class StatePartitioner extends Partitioner {
        int numPartitions;
        public StatePartitioner(int numPartitions) {
            this.numPartitions = numPartitions;
        }
        public int numPartitions() {
            return numPartitions;
        }
        public int getPartition(Object key) {
            LocationKey locationKey = (LocationKey) key;
            return locationKey.state.hashCode() % numPartitions;
        }
    }

    public static class LocationKey implements Serializable {

        final String state;
        final String locality;
        final Long count;

        /** convienience method to parse a LocationKey from an array */
        public static Iterator<Tuple2<LocationKey,Void>> flatMap(String[] arr) {
            List<Tuple2<LocationKey,Void>> result = new ArrayList<>();
            if (arr.length >= 3) {
                result.add(new Tuple2<>(new LocationKey(arr),null));
            }
            return result.iterator();
        }

        public LocationKey(String[] arr) {
            if (arr.length > 0) {
                state = arr[0];
            }
            else {
                state = "N/A";
            }

            if (arr.length > 1) {
                locality = arr[1];
            }
            else {
                locality = "N/A";
            }

            long tmpCount = 0L;
            if (arr.length > 2) {
                try {
                    tmpCount = Long.parseLong(arr[2]);
                }
                catch (NumberFormatException nfe) {
                    log.warn("Number Format Exception for input: " + Arrays.toString(arr));
                }
            }
            count = tmpCount;
        }

        public String toString() {
            return String.join(", ", state, locality, String.valueOf(count));
        }
    }

    public static void main(String[] args) {
        SecondarySort ss = new SecondarySort(args);
        ss.execute();
    }
}
