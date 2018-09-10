package spark.scalaVersion.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def WordCount(){

    val conf =new SparkConf()
          .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://xy:8021/spark.txt")
    val words = lines.flatMap( line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times"))


  }
}
