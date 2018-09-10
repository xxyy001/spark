package spark.javaVersion.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class GroupTop3 {
    public static void GroupTop3(){
        SparkConf conf = new SparkConf()
                .setAppName("cn.spark.study.WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/groupSort.txt");
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] lineSplited = s.split(" ");
                        return new Tuple2<String, Integer>(lineSplited[0],Integer.valueOf(lineSplited[1]));
                    }
                }
        );
        JavaPairRDD<String,Iterable<Integer>> groupPairs = pairs.groupByKey();
        JavaPairRDD<String,Iterable<Integer>> top3Score = groupPairs.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                    @Override
                    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                        Integer[] top3 = new Integer[3];
                        String className = t._1;
                        Iterator<Integer> scores = t._2.iterator();
                        while (scores.hasNext()){
                            Integer score = scores.next();
                            for (int i = 0;i < 3;i++){
                                if(top3[i] == null){
                                    top3[i] = score;
                                    break;
                                }else if(score > top3[i]){
                                    for (int j = top3.length-1;j>i;j--){
                                        top3[j] = top3[j-1];
                                    }
                                    top3[i] = score;
                                    break;
                                }
                                }

                        }
                        return new Tuple2<String, Iterable<Integer>> (className, Arrays.asList(top3));
                    }
                }
        );
        top3Score.foreach(
                new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                    @Override
                    public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                        System.out.println("class: "+t._1);
                        Iterator<Integer> scoresList = t._2.iterator();
                        while (scoresList.hasNext()){
                            Integer score = scoresList.next();
                            System.out.println(score);
                        }
                        System.out.println("***********************");
                    }
                }
        );

    }
}
