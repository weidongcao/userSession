package com.don.session.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by Cao Wei Dong on 2017-03-18.
 */
public class TestSparkSQL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Test Spark SQL")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(Integer.valueOf(1), "CaoWeidong", 1990L));
        rows.add(RowFactory.create(Integer.valueOf(2), "CaoJiling", 1986L));
        rows.add(RowFactory.create(Integer.valueOf(3), "ZhuXiaomeng", null));

//        JavaRDD<String> original = sc.textFile("file:///D:\\Workspace\\Code\\userSession\\src\\main\\java\\com\\don\\session\\test\\testData");

        /*JavaRDD<Row> rowRDD = original.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String line) throws Exception {
                        String[] fields = line.split("\t");
                        return RowFactory.create(
                                Integer.valueOf(fields[0]),
                                fields[1],
                                Long.valueOf(fields[2])
                        );
                    }
                }
        );*/

        JavaRDD<Row> rowRDD = sc.parallelize(rows);

        StructType tableSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("year", DataTypes.LongType, true)
        ));

        DataFrame df = sqlContext.createDataFrame(rowRDD, tableSchema);

        df.registerTempTable("info");

        DataFrame infoDF = sqlContext.sql("select * from info");

        infoDF.javaRDD().foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println("year = " + row.getLong(2));
                    }
                }
        );

    }
}
