package spark.javaVersion.Streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformBlackList {
    public static void TransformBlackList() throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("TransformBlackList");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));


        List<Tuple2<String,Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("tom",true));

        final JavaPairRDD<String,Boolean> blackListRDD = jssc.sparkContext().parallelizePairs(blackList);

        JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("localhost",9999);

        JavaPairDStream<String,String> userAdsClickLogDStream  = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<String, String>(adsClickLog.split(" ")[1],adsClickLog);
            }
        });

        JavaDStream<String> validuserAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> JoinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD);

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = JoinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });


                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {

                        return tuple._2._1;
                    }
                });
                return validAdsClickLogRDD;
            }
        });


        validuserAdsClickLogDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
