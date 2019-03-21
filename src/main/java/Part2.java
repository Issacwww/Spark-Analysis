import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Part2 {
    private final static String inputPath = "hdfs:///user/student/input/user_artists.dat";
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("user artists count").getOrCreate();
        Dataset<Row> df = spark.read().format("csv")
                .option("sep","\t").option("inferSchema","true")
                .option("header","true").load(inputPath);
        df.createOrReplaceTempView("count");
        Dataset<Row> sqlDF = spark.sql("SELECT artistID, sum(weight) as counts FROM count GROUP BY artistID ORDER BY counts DESC");
        sqlDF.show();
        //write to file
//        sqlDF.coalesce(1).write().csv("result");
        spark.stop();

    }
}
