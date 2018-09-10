package spark.javaVersion.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;



public class JsonDataSource {
    public static void JsonDataSource(){
        SparkConf conf = new SparkConf()

                .setAppName("JsonDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset studentScoreDF = sqlContext.read().json("hdfs://xy:8021/studentScore.json");
        studentScoreDF.registerTempTable("students_score");


        Dataset goodStudentNameDF = sqlContext.sql("select name,score from students_score where score >= 80");

//        goodStudentNameDF.show();

        JavaRDD<Row> goodStudentNameRDD = goodStudentNameDF.javaRDD();
        JavaRDD<String> goodStudentNameStringRDD = goodStudentNameRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        });
        List<String> goodStudentNames = goodStudentNameStringRDD.collect();


        List<String> studentInfoJson = new ArrayList<String>();
        studentInfoJson.add("{\"name\":\"leo\",\"age\":18}");
        studentInfoJson.add("{\"name\":\"jack\",\"age\":17}");
        studentInfoJson.add("{\"name\":\"marry\",\"age\":19}");
        JavaRDD<String> studentInfosRDD = sc.parallelize(studentInfoJson);

        Dataset studentInfoDF = sqlContext.read().json(studentInfosRDD);



        studentInfoDF.registerTempTable("students_info");


        String sql = "select name,age from students_info where name in (";
        for (int i = 0; i < goodStudentNames.size();i++){
            sql += "'" + goodStudentNames.get(i).toString() + "'";
            if (i < goodStudentNames.size()-1 ){
                sql += ", ";
            }
        }
        sql += ")";


        Dataset goodStudentInfoDF = sqlContext.sql(sql);

        //row 一般默认取的Long，因此先String，再Integer

        JavaPairRDD<String,Tuple2<Integer,Integer>> goodStudentRDD =
                goodStudentNameDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String,Integer>(row.getString(0),
                                        Integer.valueOf(String.valueOf(row.getLong(1)))
                        );
                    }
                }).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String,Integer>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1)))
                        );
                    }
                }));

        JavaRDD<Row> goodStudentRowRDD = goodStudentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
            }
        });


//        创建元数据

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset goodStudentDF = sqlContext.createDataFrame(goodStudentRowRDD,structType);
        goodStudentDF.show();

        goodStudentDF.write().format("json").save("hdfs://xy:8021/goodStudent");




    }
}
