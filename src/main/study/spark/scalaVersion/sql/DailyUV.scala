package spark.scalaVersion.sql


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._

object DailyUV {

  def main(args: Array[String]){
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DailyUV")
    val sc = new SparkContext(conf)
    val sqlContext  = new sql.SQLContext(sc)

    import sqlContext.implicits._

    val userAccessorLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1123",
      "2015-10-02,1123")

    val userAccessorLogRDD = sc.parallelize(userAccessorLog,5)

    val userAccessorLogROWRDD = userAccessorLogRDD
      .map(log => Row(log.split(",")(0),log.split(",")(1).toInt))

    val structType = StructType(
      Array(
        StructField("date",StringType,true),
        StructField("userid",IntegerType,true)
      ))

    val userAccessorLogDF = sqlContext.createDataFrame(userAccessorLogROWRDD,structType)
    userAccessorLogDF.show()

    val a = userAccessorLogDF.groupBy("date")
      .agg('date,countDistinct('userid)).rdd
      .map(row => Row(row(1),row(2)))
      .collect()
      .foreach(println)


  }
}
