package spark.javaVersion.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {
    public static void TransformationOperation(){
        SparkConf conf = new SparkConf()
                .setAppName("GroupByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",75),
                new Tuple2<String, Integer>("class1",85),
                new Tuple2<String, Integer>("class1",82)
        );
        JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);

        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> ite = t._2.iterator();
                while (ite.hasNext()){
                    System.out.println(ite.next());
                }
                System.out.println("***********************");
            }
        });

    }




}
