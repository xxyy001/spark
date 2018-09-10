package spark.scalaVersion.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlackList {
  def TransformBlackList(): Unit ={
    val conf =new SparkConf()
      .setMaster("local[2]")
      .setAppName("TransformBlackList")

    val ssc = new StreamingContext(conf,Seconds(5))

    val blackList = Array(("tom",true))

    val blackListRDD = ssc.sparkContext.parallelize(blackList,5)

    val adsClickLogDStream = ssc.socketTextStream("localhost",9999)

    val userAdsClickLogDStream = adsClickLogDStream.map(addClickLog => (addClickLog.split(" ")(1),addClickLog))

    val validuserAdsClickLogDStream = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
      val JoinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD)

      val filteredRDD = JoinedRDD.filter(tuple => {
        if (tuple._2._2.getOrElse(false)){
          false
        }else{
          true
        }
      })

      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      validAdsClickLogRDD
    })


    validuserAdsClickLogDStream.print()
    ssc.start()
    ssc.awaitTermination()



  }

}
