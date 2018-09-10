package spark.javaVersion.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import java.util.List;

public class RDD2DataFrameReflection {
    public static void RDD2DataFrameReflection(){
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameReflection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("/Users/xy/Desktop/spark_study/src/main/resources/students.txt");
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(lineSplited[0].trim()));
                stu.setName(lineSplited[1]);
                stu.setAge(Integer.valueOf(lineSplited[2].trim()));
                return stu;
            }
        });
//        使用反射方式，将RDD转化为DataFrame（将class传入）
        Dataset studentDF = sqlContext.createDataFrame(students,Student.class);
//        注册临时表
        studentDF.registerTempTable("students");
        Dataset teenagerDF = sqlContext.sql("select * from students where age <= 18");
//        将查询出来的DF转化为RDD
        JavaRDD<Row>  teenagerRDD = teenagerDF.javaRDD();
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setAge(row.getInt(0));
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                return stu;
            }
        });

        List<Student> studentList = teenagerStudentRDD.collect();

        for (Student stu:studentList){
            System.out.println(stu);
        }
    }
}
