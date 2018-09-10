package spark.scalaVersion.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateByKeyWordCount {
  def updateStateByKeyWordCount(): Unit ={
    val conf =new SparkConf()
        .setMaster("local[2]")
      .setAppName("updateStateByKeyWordCount")

    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("hdfs://xy:8021/wordcount_checkpoint")
    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordcounts = pairs.updateStateByKey((values:Seq[Int],state:Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values){
        newValue += value
      }
      Option(newValue)
    })

    wordcounts.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
