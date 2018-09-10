package spark.javaVersion.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;


public class DataFrameCreate {
    public static void DataFrameCreate(){
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset ds = sqlContext.read().json("hdfs://xy:8021/students.json");
        ds.show();
        ds.printSchema();
        ds.select("name").show();
        ds.select(ds.col("name"),ds.col("age").plus(1)).show();
        ds.filter(ds.col("age").gt(18)).show();
        ds.groupBy(ds.col("age")).count().show();
    }
}
