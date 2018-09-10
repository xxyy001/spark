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

public class sortWordCount {
    public static void sortWordCount(){
        SparkConf conf = new SparkConf()
                .setAppName("cn.spark.study.WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/spark.txt");

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
        JavaPairRDD<Integer,String> countWords = wordCounts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<Integer,String>(t._2,t._1);
                    }
        });
        JavaPairRDD<Integer,String> sortedCountWords =  countWords.sortByKey(false);
        JavaPairRDD<String,Integer> sortedWordsCount = sortedCountWords.mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                        return new Tuple2<String,Integer>(t._2,t._1);
                    }
                }
        );

        sortedWordsCount.foreach(
                new VoidFunction<Tuple2<String, Integer>>() {
                    public void call(Tuple2<String, Integer> t) throws Exception {
                        System.out.println(t._1+" appeared " +t._2+" times");
                    }
                }
        );
        sc.close();
    }

}
