package spark.javaVersion.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class WordCountFlatMap {
    public static void WordCountFlatMap(){
        SparkConf conf = new SparkConf()
                .setAppName("cn.spark.study.WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> lineList = Arrays.asList("hello word","hello me","hello world");
        JavaRDD<String> lines = sc.parallelize(lineList);
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );

        words.foreach(
                new VoidFunction<String>() {
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                }
        );
        sc.close();


    }
}
