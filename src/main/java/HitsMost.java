import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class HitsMost {
    // part3 task3&4
    private final static String inputPath = "hdfs:///user/student/input/access_log";
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Hit Most");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String IP="",path="";
        int IPHits=0,pathHits=0;

        long startTime = System.nanoTime();
        //load data
        JavaRDD<String> records = sc.textFile(inputPath).cache();

        //get IP info and MapReduce
        List<Tuple2<String, Integer>> IPSet = records.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")[0]).iterator()).mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1)).reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2).collect();

        //get top1 and display
        for (Tuple2<String, Integer> e : IPSet ) {
            if (e._2() > IPHits) {
                IP = e._1();
                IPHits = e._2();
            }
        }
        long IPtime = System.nanoTime();

        //get Path info and MapReduce
        List<Tuple2<String, Integer>> PathSet = records.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")[6]).iterator()).mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1)).reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2).collect();

        for (Tuple2<String, Integer> e : PathSet ) {
            if (e._2() > pathHits) {
                path = e._1();
                pathHits = e._2();
            }
        }
        long Pathtime = System.nanoTime();

        System.out.println("----------------------------\nHits Most\n");
        System.out.println("IP: "+ IP + " hit most, hit " + IPHits + " times.");
        System.out.println("Searching for IP takes time: " + (float)(IPtime - startTime) / 1000000000 + " sec\n");

        System.out.println("Path: "+ path + " has been hit most, for " + pathHits + " times.");
        System.out.println("Searching for path takes time: " + (float)(Pathtime - IPtime) / 1000000000 + " sec");
        System.out.println("\n----------------------------");

        sc.stop();
    }
}
