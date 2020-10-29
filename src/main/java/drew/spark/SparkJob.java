package drew.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class SparkJob {
    @Parameter(names = "-n", description = "Spark Job Name")
    String appName = "Generic SparkJob";

    @Parameter(names = "-m", description = "Spark Master to Use")
    String master = "local[*]";

    @Parameter(names = "-i", description = "Input File")
    String inputPath = "input-file.txt";

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

        JavaRDD<String> text = input
                .map((Function<Tuple2<LongWritable, Text>, String>) tuple -> tuple._2().toString().trim())
                .filter((Function<String, Boolean>) t -> !t.isEmpty());

        JavaRDD<Map<String,String>> kv =
                text.flatMap((FlatMapFunction<String, Map<String, String>>) s -> Collections.emptyIterator());

        for(Map<String,String> k: kv.collect()) {
            System.out.println(k);
        }
    }

    public static void main(String[] args) {
        SparkJob job = new SparkJob(args);
        job.execute();
    }

}
