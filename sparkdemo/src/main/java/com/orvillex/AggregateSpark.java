package com.orvillex;

import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import breeze.linalg.argmax;
import breeze.macros.expand.args;

import java.util.HashMap;

public final class AggregateSpark {
    
    public static void main(String[] args) {
        CommandLineParser parser = new BasicParser();
        Options commandOptions = new Options();
        commandOptions.addOption("f", "file", true, "input file");
        commandOptions.addOption("o", "outfile", true, "output file");
        try {
            CommandLine cmdline = parser.parse(commandOptions, args);
            String file = cmdline.getOptionValue("f");
            String out = cmdline.getOptionValue("o");
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local"); //.setMaster("spark://ip:port");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
        StructType structType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("salary", DataTypes.LongType, true),
            DataTypes.createStructField("count", DataTypes.LongType, true),
            DataTypes.createStructField("category", DataTypes.StringType, true)
        ));
        Dataset<Row> df = session.read().schema(structType).json("sparkdemo/data/employees.json");

        //df.select(functions.count("salary")).show();
        //df.selectExpr("count(salary)").show();

        // df.select(functions.countDistinct("salary")).show();
        // df.selectExpr("count(distinct *)").show();

        // df.select(functions.approxCountDistinct("salary", 0.1)).show();
        // df.selectExpr("approx_count_distinct(salary, 0.1)").show();

        // df.select(functions.first("salary"), functions.last("salary")).show();
        // df.selectExpr("first(salary)", "last(salary)").show();

        // df.select(functions.min("salary"), functions.max("salary")).show();
        // df.selectExpr("min(salary)", "max(salary)").show();

        // df.select(functions.sum("salary")).show();
        // df.selectExpr("sum(salary)").show();
        // df.select(functions.sumDistinct("salary")).show();
        // df.selectExpr("sum(distinct salary)").show();

        // df.select(functions.avg("salary")).show();
        // df.selectExpr("avg(salary)").show();
        // df.select(functions.mean("salary")).show();
        // df.selectExpr("mean(salary)").show();

        // df.select(functions.variance("salary")).show(); // ????????????  
        // df.select(functions.var_samp("salary")).show(); // ??????  
        // df.selectExpr("variance(salary)").show(); // ??????  
        // df.selectExpr("var_samp(salary)").show(); // ??????  
        // df.select(functions.stddev("salary")).show(); // ???????????????  
        // df.select(functions.stddev_samp("salary")).show(); // ??????  
        // df.selectExpr("stddev(salary)").show(); // ??????  
        // df.selectExpr("stddev_samp(salary)").show(); // ??????  

        // df.select(functions.var_pop("salary")).show(); // ????????????  
        // df.selectExpr("var_pop(salary)").show(); // ??????  
        // df.select(functions.stddev_pop("salary")).show(); // ???????????????  
        // df.selectExpr("stddev_pop(salary)").show(); // ??????  

        // df.select(functions.skewness("salary")).show(); // ????????????  
        // df.selectExpr("skewness(salary)").show(); // ??????  
        // df.select(functions.kurtosis("salary")).show(); // ????????????  
        // df.selectExpr("kurtosis(salary)").show(); // ??????  

        // df.select(functions.corr("salary", "count")).show(); // ?????????  
        // df.selectExpr("corr(salary, count)").show(); // ??????  
        // df.select(functions.covar_pop("salary", "count")).show(); // ???????????????  
        // df.selectExpr("covar_pop(salary, count)").show(); // ??????  
        // df.select(functions.covar_samp("salary", "count")).show(); // ???????????????  
        // df.selectExpr("covar_samp(salary, count)").show();  // ??????  

        // df.select(functions.collect_list("salary"), functions.collect_set("salary")).show();
        // df.selectExpr("collect_list(salary)", "collect_set(salary)").show();

        // df.groupBy("category").agg(functions.count("salary").alias("pre"), functions.expr("count(salary)")).orderBy(functions.col("pre").desc()).show();
        // df.groupBy("category").sum("salary").show();

        // df.groupBy("category").agg(new HashMap<String, String>() {{
        //     put("salary", "sum");
        //     put("count", "count");
        // }}).show();

        /**
         * ????????????
         */
        // WindowSpec windowSpec = Window.partitionBy("category")
        //     .orderBy(functions.col("salary"))
        //     .rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        
        // Column salarySum = functions.sum("count").over(windowSpec);
        // Column countRank = functions.avg("salary").over(windowSpec);

        // df.select(functions.col("*"), salarySum, countRank).show();

        // df.rollup("name", "category")
        //     .agg(new HashMap<String, String>() {{
        //         put("salary", "sum");
        //         put("count", "avg");
        //     }})
        //     .selectExpr("name", "category", "`sum(salary)` as total", "`avg(count)` as avg").show();

        // df.cube("name", "category")
        //     .agg(functions.expr("grouping_id()"), functions.sum("salary"), functions.avg("count"))
        //     .orderBy(functions.expr("grouping_id()").desc())
        //     .selectExpr("name", "category", "`sum(salary)` as total", "`avg(count)` as avg", "`grouping_id()`").show();

        // df.groupBy("category").pivot("name").sum().show();

        // session.udf().register("booland", new BoolAnd());
        // df.selectExpr("booland(true)").show();

        

        session.stop();
    }

    public static class BoolAnd extends UserDefinedAggregateFunction {

        /**
         * ??????UDAF????????????
         */
        @Override
        public StructType bufferSchema() {
            return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("result", DataTypes.BooleanType, true)
            ));
        }

        /**
         * ?????????????????????
         */
        @Override
        public DataType dataType() {
            return DataTypes.BooleanType;
        }

        /**
         * ??????UDAF????????????????????????????????????????????????
         */
        @Override
        public boolean deterministic() {
            return true;
        }

        /**
         * ??????????????????????????????
         */
        @Override
        public Object evaluate(Row arg0) {
            return arg0.getBoolean(0);
        }

        /**
         * ????????????????????????????????????
         */
        @Override
        public void initialize(MutableAggregationBuffer arg0) {
            arg0.update(0, true);
        }

        /**
         * ???????????????????????????
         */
        @Override
        public StructType inputSchema() {
            return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("value", DataTypes.BooleanType, true)
            ));
        }

        @Override
        public void merge(MutableAggregationBuffer arg0, Row arg1) {
            arg0.update(0, (boolean)arg1.getAs(0));
        }

        @Override
        public void update(MutableAggregationBuffer arg0, Row arg1) {
            arg0.update(0, (boolean)arg0.getAs(0) && (boolean)arg1.getAs(0));
        }
        
    }
}
