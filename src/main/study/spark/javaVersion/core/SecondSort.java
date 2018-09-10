package spark.javaVersion.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSort {
    public static void SecondSort(){
        SparkConf conf = new SparkConf()
                .setAppName("cn.spark.study.WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/sort.txt");
        JavaPairRDD<SecondarySortKey,String> pairs = lines.mapToPair(
                new PairFunction<String, SecondarySortKey, String>() {
                    @Override
                    public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                        String[] lineSplited = line.split(" ");
                        SecondarySortKey key = new SecondarySortKey(
                                Integer.valueOf(lineSplited[0]),
                                Integer.valueOf(lineSplited[1])
                        );

                        return new Tuple2<SecondarySortKey, String>(key,line);
                    }
                }
        );
        JavaPairRDD<SecondarySortKey,String> sortedPairs = pairs.sortByKey();
        JavaRDD<String> sortedLines = sortedPairs.map(
                new Function<Tuple2<SecondarySortKey, String>, String>() {
                    @Override
                    public String call(Tuple2<SecondarySortKey, String> pair) throws Exception {
                        return pair._2;
                    }
        });
        sortedLines.foreach(
                new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                }
        );

        sc.close();
    }
}
