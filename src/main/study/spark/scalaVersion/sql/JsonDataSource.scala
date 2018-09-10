package spark.scalaVersion.sql


import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object JsonDataSource {
  def JsonDataSource(): Unit ={
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val sqlContext  = new SQLContext(sc)
    val studentScoresDF = sqlContext.read.json("hdfs://xy:8021/studentScore.json")

    studentScoresDF.registerTempTable("students_score")

    val goodStudentNameDF = sqlContext.sql("select name,score from students_score where score >= 80")

    val goodStudentNames = goodStudentNameDF.rdd.map(row => row(0)).collect()

    val studentInfoJson = Array("{\"name\":\"leo\",\"age\":18}",
      "{\"name\":\"jack\",\"age\":17}",
      "{\"name\":\"marry\",\"age\":19}")
    val studentInfoJsonsRDD = sc.parallelize(studentInfoJson,3)

    val studentInfosDF = sqlContext.read.json(studentInfoJsonsRDD)

    studentInfosDF.registerTempTable("students_info")

    var sql = "select name,age from students_info where name in ("

    for (i <- 0 until goodStudentNames.length){
      sql += "'"+goodStudentNames(i)+"'"
      if (i < goodStudentNames.length-1){
        sql += ","
      }
    }
    sql += ")"

    val goodStudentInfoDF = sqlContext.sql(sql)

    val RDD1 = goodStudentNameDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("score")))

    val RDD2 = goodStudentInfoDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("age")))

    val goodStudentsRDD = RDD1.join(RDD2)

    val goodStudentsRowRDD = goodStudentsRDD.map(info => Row(info._1,info._2._1.toString.toInt,info._2._2.toString.toInt))

    val structType = StructType(Array(
      StructField("name",StringType,true),
      StructField("score",IntegerType,true),
      StructField("age",IntegerType,true)
    ))


//    元素类型为Row的RDD才ok

    val goodStudentsDF = sqlContext.createDataFrame(goodStudentsRowRDD,structType)

    goodStudentsDF.write.format("json").save("hdfs://xy:8021/goodStudent_scala")







  }

}
