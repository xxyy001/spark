package spark.javaVersion.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class WordCountLocal {
    public static void WordCountLocal(){
        SparkConf conf = new SparkConf()
                .setAppName("cn.spark.study.WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/spark.txt");
//        JavaRDD<String> words = lines.flatMap(
//                new FlatMapFunction<String, String>() {
//                    public Iterable<String> call(String line) throws Exception {
//                        return Arrays.asList(line.split(" "));
//                    }
//        });
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                }
        );
        JavaPairRDD<String,Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word,1);
                    }
                });
        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                });
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1 +" appeared " + wordcount._2 + " times");
            }
        });
        sc.close();


    }
}
