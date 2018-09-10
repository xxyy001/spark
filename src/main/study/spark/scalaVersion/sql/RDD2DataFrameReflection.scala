package spark.scalaVersion.sql

import org.apache.spark.{SparkConf, SparkContext, sql}
//基于反射的RDD到DF需要用extends app 不能用def main方法
object RDD2DataFrameReflection extends App {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext  = new sql.SQLContext(sc)

    import sqlContext.implicits._

    case class Student(id:Int,name:String,age:Int)
    val students =  sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/students.txt")
      .map(line => line.split(",")).map(arr => Student(arr(0).trim().toInt,arr(1),arr(2).trim().toInt))




    val studentDF = students.toDF()

    studentDF.registerTempTable("students")

    val teenagerDF = sqlContext.sql("select * from students where age <= 18")

    val teenagerRDD = teenagerDF.rdd

//  按照顺序排列，java顺序改变了
    teenagerRDD.map(row => Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt)).collect()
      .foreach(stu => println(stu.id,stu.name,stu.age))

//row getAs得到指定的列
    teenagerRDD.map(row => Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age"))).collect()
      .foreach(stu => println(stu.id,stu.name,stu.age))

//  row getValueMap 得到指定的几列，返回是个map
    teenagerRDD.map(row =>
      {
        val map =  row.getValuesMap[Any](Array("id","name","age"));
        Student(map("id").toString.toInt,map("name").toString,map("age").toString.toInt)

      }).collect().foreach(stu => println(stu.id,stu.name,stu.age))
  
}
