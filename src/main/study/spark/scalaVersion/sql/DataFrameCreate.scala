package spark.scalaVersion.sql

import org.apache.spark.{SparkConf, SparkContext, sql}

object DataFrameCreate {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val sqlContext  = new sql.SQLContext(sc)
    val ds = sqlContext.read.json("hdfs://xy:8021/students.json")
    ds.show()
  }

}
