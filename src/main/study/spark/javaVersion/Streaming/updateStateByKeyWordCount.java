package spark.javaVersion.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class updateStateByKeyWordCount {
    public static void updateStateByKeyWordCount() throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("updateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("hdfs://xy:8021/wordcount_checkpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost",9999);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                }
        );
        JavaPairDStream <String,Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word,1);
                    }
                });

        JavaPairDStream<String,Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if (state.isPresent()){
                    newValue = state.get();
                }
                for (Integer value :values){
                    newValue += value;
                 }
                return Optional.of(newValue);
            }
        });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }
}
