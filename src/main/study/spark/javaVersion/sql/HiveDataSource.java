package spark.javaVersion.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;



public class HiveDataSource {
    public static void HiveDataSource(){
        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("DROP TABLE IF EXISTS  student_infos");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING,age INT )");

        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH  "
                + "\"/Users/xy/Desktop/spark_study/src/main/resources/student_info\""
                +" INTO TABLE student_infos"
        );



        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING,score INT )");

        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH  "
                + "\"/Users/xy/Desktop/spark_study/src/main/resources/student_score\""
                +" INTO TABLE student_scores"
        );


        Dataset goodStudentDF = hiveContext.sql("select si.name, si.age,ss.score " +
                "FROM student_infos si " +
                "JOIN student_scores ss " +
                "ON si.name == ss.name " +
                "WHERE ss.score >= 80");

        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");



        //版本变化不存在saveAsTable方法了,因此采用注册表后写入
//        goodStudentDF.saveAsTable("good_student_infos");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS good_student_infos (name STRING,score INT, age INT)");

        goodStudentDF.registerTempTable("table1");
        hiveContext.sql("insert into good_student_infos select name,age,score from table1");

        Dataset goodStudentsRows = hiveContext.table("good_student_infos");
        goodStudentsRows.show();

//        for (Row goodStudentsrow:goodStudentsRows){
//            System.out.println(goodStudentsrow);
//        }



        sc.close();



    }
}
