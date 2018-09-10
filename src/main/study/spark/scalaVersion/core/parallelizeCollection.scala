package spark.scalaVersion.core

import org.apache.spark.{SparkConf, SparkContext}

object parallelizeCollection {

  def parallelizeCollection() {
    val conf = new SparkConf()
      .setAppName("parallelizeCollection")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val sum = nums.reduce(_ + _)

    println(sum)
  }
}
