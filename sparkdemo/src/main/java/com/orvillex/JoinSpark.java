package com.orvillex;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public final class JoinSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> empy = session.read().json("sparkdemo/data/employees.json");
        Dataset<Row> category = session.read().json("sparkdemo/data/categories.json");

        // 内连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag"))).show();
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "inner").show();

        // 外连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "outer").show();

        // 左外连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "left_outer").show();

        // 右外连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "right_outer").show();

        // 左半连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "left_semi").show();

        // 左反连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "left_anti").show();

        // 交叉连接
        // empy.join(category, empy.col("category").equalTo(category.col("cag")), "left_anti").show();

        session.stop();
    }
}
