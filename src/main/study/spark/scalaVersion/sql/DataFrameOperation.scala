package spark.scalaVersion.sql

import org.apache.spark.{SparkConf, SparkContext, sql}

object DataFrameOperation {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val sqlContext  = new sql.SQLContext(sc)
    val ds = sqlContext.read.json("hdfs://xy:8021/students.json")
    ds.show()
    ds.printSchema()
    ds.select("name").show()
    ds.select(ds.col("name"),ds.col("age").plus(1)).show()
    ds.filter(ds.col("age").gt(18)).show()
    ds.groupBy(ds.col("age")).count().show()
  }
}
