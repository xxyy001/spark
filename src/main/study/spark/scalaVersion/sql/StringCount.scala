package spark.scalaVersion.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._



class StringCount extends UserDefinedAggregateFunction{
  //输入
  override def inputSchema: StructType = {
    StructType(
      Array(
        StructField("str",StringType,true)
      ))
  }
  //中间聚合所需要处理的数据类型
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("count",IntegerType,true)
      ))
  }
  //返回数据的数据类型
  override def dataType: DataType =  {
    IntegerType
  }

  override def deterministic: Boolean = {
    true
  }

//  为每个分组饿数据进行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

//  每个分组，有新的值进来时，如何进行分组对应的聚合值计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

//  一个分组在各个节点的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) +buffer2.getAs[Int](0)
  }

//  一个分组的聚合值，如何通过中间的缓存聚合值，得到最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
