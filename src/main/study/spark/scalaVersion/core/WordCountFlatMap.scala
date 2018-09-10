package spark.scalaVersion.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountFlatMap {
  def WordCountFlatMap(){

    val conf =new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = Array("hello word","hello me","hello world")

    val words = lines.flatMap( line => line.split(" "))

    words.foreach(word => println(word))


  }

}
