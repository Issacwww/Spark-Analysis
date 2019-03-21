import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;


public class HitsOnItemRDD {
    // part3 task1 & 2 using cached RDD

    private final static String inputPath = "hdfs:///user/student/input/access_log";
    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("Hits On Item with RDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int count1 = 0;
        int count2 = 0;
        long startTime = System.nanoTime();
        //load data
        JavaRDD<String> records = sc.textFile(inputPath).cache();

        List<Tuple2<String, Integer>> output = records.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")[6]).iterator()).mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1)).reduceByKey((i1, i2) -> i1 + i2).collect();

        for (Tuple2<String, Integer> t : output) {
            if (t._1().equals("/assets/img/loading.gif"))
                count1 = t._2();
        }
        long first = System.nanoTime();

        List<Tuple2<String, Integer>> output2 = records.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")[6]).iterator()).mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1)).reduceByKey((i1, i2) -> i1 + i2).collect();

        for (Tuple2<String, Integer> t : output2) {
            if (t._1().equals("/assets/js/lightbox.js"))
                count2 = t._2();
        }
        System.out.println("----------------------------\nHits On Item with cache\n");

        System.out.println("hits on /assets/img/loading.gif: " + count1);
        System.out.println("Searching for /assets/img/loading.gif takes time: " + (float)(first-startTime) / 1000000000 + " sec\n");

        System.out.println("hits on /assets/js/lightbox.js: " + count2);
        System.out.println("Searching for /assets/js/lightbox.js takes time: " + (float)(System.nanoTime() - first) / 1000000000 + " sec");

        System.out.println("\n----------------------------");

        sc.stop();
    }
}
