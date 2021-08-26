/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orvillex;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.Upper;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [partitions]
 */
public final class JavaSparkPi {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local"); //.setMaster("spark://ip:port");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
        StructType structType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("salary", DataTypes.LongType, true),
            DataTypes.createStructField("count", DataTypes.LongType, true),
            DataTypes.createStructField("category", DataTypes.StringType, true)
        ));
        Dataset<Row> df = session.read().schema(structType).json("sparkdemo/data/employees.json");

        // Column col = functions.col("name");
        // Column expr = functions.expr("concat(name, '_ed')");

        // df.select(functions.concat(col,functions.lit("_ed"))).show();
        // df.select(expr).show();

        // 支持SQL查询
        // df.createOrReplaceTempView("dfTable");

        // df.select("name").show(2); // 等同于 select name from dfTable limit 2

        // df.select(functions.expr("name as sname")).show(2); // 等同于 select name as sname from dfTable limit 2

        // df.selectExpr("name as sname").show(2); // 同上

        // df.selectExpr("avg(salary)", "count(distinct(name))").show(); // 等同select avg(salary),count(distinct name) from dfTable

        // df.select(functions.expr("name").equalTo("Andy")).show(); // 等同判断name是否为Andy

        // df.select(functions.expr("*"), functions.lit("true").as("existed")).show();

        // 判断salary列，并将结果做为greater列（true/false）
        // df.withColumn("greater", functions.expr("salary > 3500")).show();

        // 将name列重命名为subname列，同时过滤掉小于3500的数据
        // df.withColumnRenamed("name", "subname").filter(x -> x.getLong(1) > 3500).show();

        // 删除name列，将其他列进行显示
        // df.drop("name").show();

        // 将salary列强制转换为string类型，同时重命名为salarystr
        // df.withColumn("salarystr", functions.col("salary").cast(DataTypes.StringType)).show();

        // df.where("salary > 3500").show();
        // df.where(functions.col("name").equalTo("Andy")).show();
        // df.where("name = 'Andy'").show();
        // df.select("salary").distinct().show();

        // 0.2表示随机抽取样本数据中的20%的样本
        // df.sample(false, 0.2).show();

        // 将数据按照 25% 与 75% 比例拆分
        // Dataset<Row>[] dts = df.randomSplit(new double[] { 0.25, 0.75 });
        // dts[0].show();

        // dts[0].union(dts[1]).show();

        // df.sort("salary").show();
        // df.orderBy(functions.col("salary").desc()).show();
        // df.orderBy(functions.desc("salary")).show();

        // df.orderBy(functions.desc_nulls_first("salary")).show();

        // df.limit(4).show();

        // df.rdd().getNumPartitions();
        // df.repartition(5);
        // df.repartition(functions.col("name"));

        // 等同于 select pow(salary, 2) + 4 as powSalary from dfTable
        // Column col = functions.pow(functions.col("salary"), 2).plus(4).as("powSalary");
        // df.select(col).show();
        // df.selectExpr("pow(salary, 2) + 4 as pwSalary").show(); // 效果同上

        // round为向上取整，bround为向下取整
        // df.select(functions.round(functions.lit(2.5)), functions.bround(functions.lit(2.5))).show();

        // monotonicallyIncreasingId将可以按照数据顺序从0开始自增
        // df.select(functions.monotonicallyIncreasingId()).show();
        // df.selectExpr("monotonically_increasing_id()", "*").show();

        // 通过coalesce从所选择的列中选择第一个不为null的列
        // df.select(functions.coalesce(functions.col("name"), functions.col("salary"))).show();
    
        // df.na().drop("all");
        // df.na().drop("all", new String[] {"name", "salary"});

        // df.na().fill("null str");
        // df.na().fill(3, new String[] { "salary" });

        // df.selectExpr("(name, salary) as complex").select("complex.name").show();
        // df.select(functions.struct(functions.col("name"), functions.col("salary")).alias("complex")).select("complex.name").show();

        // df.select(functions.split(functions.col("name"), " ").alias("array_col")).selectExpr("array_col[0]").show();
        // df.selectExpr("split(name, ' ') as array_col").selectExpr("array_col[0]").show();

        // df.selectExpr("size(split(name, ' ')) as array_size").show();
        // df.selectExpr("array_contains(split(name, ' '), 'B')").show();

        // df.select(functions.map(functions.col("name"), functions.col("salary")).alias("map_col")).selectExpr("map_col['Berta D']").show();
        // df.selectExpr("map(name, salary) as map_col").selectExpr("map_col['Berta D']").show();

        // Dataset<Row> jsonDF = session.range(1).selectExpr("\'{\"myJSONKey\": {\"myJSONValue\": [1, 2, 3]}}\' as jsonString");
        // jsonDF.select(functions.get_json_object(functions.col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),
        //     functions.json_tuple(functions.col("jsonString"), "myJSONKey")).show();

        // df.selectExpr("(name, salary) as map_col").select(functions.to_json(functions.col("map_col"))).show();

        // UDF1<Long, Long> fnc = x -> x * x;

        // UserDefinedFunction udf = functions.udf(fnc, DataTypes.LongType);
        // df.select(udf.apply(functions.col("salary"))).show();

        // session.udf().register("power2", udf);
        // df.selectExpr("power2(salary)").show();
        

        // df.show(); 显示所有数据
        // df.printSchema(); 显示数据模型

        session.stop();
    }


}
