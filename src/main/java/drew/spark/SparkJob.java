package drew.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;

public class SparkJob {
    @Parameter(names = "-n", description = "Spark Job Name")
    String appName = "Generic SparkJob";

    @Parameter(names = "-m", description = "Spark Master to Use")
    String master = "local[*]";

    @Parameter(names = "-i", description = "Input File")
    String inputPath = "data/state-city-pop.csv";

    final SparkSession spark;
    final JavaSparkContext sparkContext;

    public SparkJob(String[] args) {
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
        JavaPairRDD<LongWritable, Text> input = sparkContext.newAPIHadoopFile(
                inputPath,
                TextInputFormat.class, LongWritable.class, Text.class,
                sparkContext.hadoopConfiguration()
        );

      Metrics metrics = new Metrics(sparkContext.sc());

        JavaRDD<String> text = input
                .map((Function<Tuple2<LongWritable, Text>, String>) tuple -> tuple._2().toString().trim())
                .filter((Function<String, Boolean>) t -> !t.isEmpty());

        JavaRDD<CityStatePop> kv = text.flatMap(new FlatMapToCityStatePop(metrics));

        for(CityStatePop k: kv.collect()) {
            System.out.println(k);
        }

        System.out.println(metrics);
    }

    public static class Metrics implements Serializable {
        final LongAccumulator numberFormatExceptions;
        final LongAccumulator formatErrors;
        final LongAccumulator successfulRecords;

        public Metrics(SparkContext sc) {
            numberFormatExceptions = sc.longAccumulator("Number Format Exceptions");
            formatErrors = sc.longAccumulator("Formatting Problems");
            successfulRecords = sc.longAccumulator("Successful Records");
        }

        public String toString() {
            return "numberFormatExceptions: " + numberFormatExceptions.count() + "; " +
                    "formatErrors: " + formatErrors.count() + "; " +
                    "successfulRecords: " + successfulRecords.count();
        }
    }

    public static class FlatMapToCityStatePop implements FlatMapFunction<String, CityStatePop> {

        final Metrics metrics;

        public FlatMapToCityStatePop(Metrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public Iterator<CityStatePop> call(String s) throws Exception {
            try {
                String[] data = s.split(",", 3);
                if (data.length == 3) {
                    metrics.successfulRecords.add(1);
                    return Collections.singleton(new CityStatePop(data[0], data[1], Long.parseLong(data[2]))).iterator();
                }
                else {
                    metrics.formatErrors.add(1);
                }
            }
            catch (NumberFormatException e) {
                metrics.numberFormatExceptions.add(1);
            }

            return Collections.emptyIterator();
        }
    }

    public static class CityStatePop implements Serializable {
        final String state;
        final String city;
        final long population;

        public CityStatePop(String state, String city, long population) {
            this.state = state;
            this.city = city;
            this.population = population;
        }

        @Override
        public String toString() {
            return String.join(",", state, city, String.valueOf(population));
        }
    }

    public static void main(String[] args) {
        SparkJob job = new SparkJob(args);
        job.execute();
    }

}
