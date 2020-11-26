package drew.spark.sort;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import drew.spark.Driver;
import drew.spark.data.StateLocalityPopulation;
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
public class SecondarySort implements Driver.Executable {

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
        JavaPairRDD<StateLocalityPopulation,Void> locationData = rawDataArray.flatMapToPair(StateLocalityPopulation::flatMap);

        int sortPartitions = Math.min(locationData.getNumPartitions(), partitions);
        JavaPairRDD<StateLocalityPopulation,Void> sortedLocationData = locationData.partitionBy(new StatePartitioner(sortPartitions));

        final CountComparator countComparator = new CountComparator();
        JavaPairRDD<String, PriorityQueue<StateLocalityPopulation>> stateByLocation = sortedLocationData
                .mapToPair(
                        new ExtractStateAndCountToQueue(countComparator)
                )
                .reduceByKey(
                        new TopNFunction<>(topCount, countComparator)
                );

        JavaRDD<StateLocalityPopulation> finalResults = stateByLocation.flatMap(new CleanupResult(countComparator.reversed()));
        finalResults.saveAsTextFile(outputPath);
    }

    /** Extract results from the priority queue and produce tuples of LocationKey for the final results */
    public static class CleanupResult implements FlatMapFunction<Tuple2<String, PriorityQueue<StateLocalityPopulation>>, StateLocalityPopulation> {

        public Comparator<StateLocalityPopulation> comparator;

        public CleanupResult(Comparator<StateLocalityPopulation> comparator) {
            this.comparator = comparator;
        }

        @Override
        public Iterator<StateLocalityPopulation> call(Tuple2<String, PriorityQueue<StateLocalityPopulation>> t) {
            final List<StateLocalityPopulation> k = new ArrayList<>(t._2());
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
    public static class ExtractStateAndCountToQueue implements PairFunction<Tuple2<StateLocalityPopulation, Void>, String, PriorityQueue<StateLocalityPopulation>> {
        final Comparator<StateLocalityPopulation> comparator;

        public ExtractStateAndCountToQueue(Comparator<StateLocalityPopulation> comparator) {
            this.comparator = comparator;
        }

        @Override
        public Tuple2<String, PriorityQueue<StateLocalityPopulation>> call(Tuple2<StateLocalityPopulation, Void> t) {
            final PriorityQueue<StateLocalityPopulation> singletonQueue = new PriorityQueue<>(1, comparator);
            singletonQueue.add(t._1);
            return new Tuple2<>(t._1.getState(),singletonQueue);
        }
    }

    /** For sorting a priority queue, we want the smallest values first, so they will get
     *  popped off the list first when manually trimming the priority queue to max size */
    public static class CountComparator implements Serializable, Comparator<StateLocalityPopulation> {
        @Override
        public int compare(StateLocalityPopulation o1, StateLocalityPopulation o2) {
            return Long.compare(o1.getPopulation(), o2.getPopulation());
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
            StateLocalityPopulation locationKey = (StateLocalityPopulation) key;
            return locationKey.getState().hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) {
        SecondarySort ss = new SecondarySort(args);
        ss.execute();
    }
}
