package drew.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import drew.spark.data.StateLocalityPopulation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleDatasetJob implements Driver.Executable {
    @Parameter(names = "-n", description = "Spark Job Name")
    String appName = "Generic SparkJob";

    @Parameter(names = "-m", description = "Spark Master to Use")
    String master = "local[*]";

    @Parameter(names = "-i", description = "Input File")
    String inputPath = "data/state-city-pop.csv";

    @Parameter(names = "-o", description = "Output File")
    String outputPath = "data/top-state-city-pop";

    final SparkSession spark;
    final JavaSparkContext sparkContext;

    public SimpleDatasetJob(String[] args) {
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
        JavaRDD<StateLocalityPopulation> input = sparkContext.textFile(inputPath).map(StateLocalityPopulation::new);
        Dataset<Row> populationDF = spark.createDataFrame(input, StateLocalityPopulation.class);
        populationDF.createOrReplaceTempView("population");

        Dataset<Row> milPopulation = spark.sql("SELECT state, locality, population FROM population WHERE population > 999999 order by population desc");
        milPopulation.show();

        Dataset<Row> topCities = spark.sql("SELECT state,locality,population FROM \n" +
                "   (SELECT state,locality,population,dense_rank() \n" +
                "         OVER (PARTITION BY state ORDER BY population DESC) as rank \n" +
                "    FROM population) tmp \n" +
                "WHERE rank <= 2 ORDER BY state, population desc");
        topCities.repartition(1).write().csv(outputPath);
    }

    public static void main(String[] args) {
        SimpleDatasetJob j = new SimpleDatasetJob(args);
        j.execute();
    }
}
