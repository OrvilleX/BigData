package com.orvillex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import scala.Char;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.reflect.ClassManifestFactory;
import scala.reflect.internal.Trees.Return;

public final class RDDWithSpark {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
        String[] myCollection = "Spark the definitive guide : big data processing made simple".split(" ");
        
        JavaRDD<String> words = JavaRDD.fromRDD(session.sparkContext().parallelize(JavaConverters.asScalaIteratorConverter(Arrays.asList(myCollection).iterator()).asScala().toSeq(), 2, 
            ClassManifestFactory.classType(String.class)), ClassManifestFactory.classType(String.class));
        
        // 转换操作

        // distinct
        // long len = words.distinct().count();

        // filter
        // List<String> coll = words.filter((x) -> x.startsWith("S")).collect();

        // map
        // List<Integer> lens = words.map((x) -> x.length()).collect();

        // flatMap
        // List<char[]> chars = words.flatMap((x) -> Arrays.asList(x.toCharArray()).iterator()).collect();

        // sort
        // List<String> coll = words.sortBy((x) -> x.length(), true, 2).collect();

        // random split
        // JavaRDD<String>[] rdds = words.randomSplit(new double[]{ 0.5, 0.5 });

        // 动作操作

        // reduce
        // String result = words.reduce((x, y) -> x + y);

        // count
        // words.countApprox(400, 0.95);
        // words.countApproxDistinct(0.05);
        // words.countByValue();
        // words.countByValueApprox(400, 0.95);

        // String first = words.first();
        // List<String> take = words.take(3);

        // 键值操作

        JavaPairRDD<Character, String> kv = words.keyBy(x -> x.toLowerCase().charAt(0));

        // 映射
        // List<Tuple2<Character, String>> map = kv.mapValues(x -> x.toUpperCase()).collect();
        // List<Tuple2<Character, char[]>> result = kv.flatMapValues((word) -> Arrays.asList(word.toCharArray())).collect();

        // 提取Key或value
        // List<Character> keys = kv.keys().collect();
        // List<String> Values = kv.values().collect();

        // 查询Value
        // List<String> look = kv.lookup('s');

        // 聚合操作  

        // counyByKey
        // Map<Character, Long> count = kv.countByKey();

        // reduceByKey
        // Map<Character, String> result = kv.reduceByKey((x, y) -> x + y).collectAsMap();

        // aggregate
        // Long length = kv.aggregate(0l, (before, value) -> before + value._2().length() , (x1, x2) -> x1 + x2);

        // treeAggregate
        // Long length = kv.treeAggregate(0l, (before, value) -> before + value._2().length() , (x1, x2) -> x1 + x2, 3);

        // aggregateByKey
        // Map<Character, Long> result = kv.aggregateByKey(0l, (before, value) -> before + value.length() , (x1, x2) -> x1 + x2).collectAsMap();
        
        // combineByKey
        // Map<Character, char[]> map = kv.combineByKey(x -> x.toCharArray(), (col, v) -> ArrayUtils.addAll(col, v.toCharArray()), (x1, x2) -> ArrayUtils.addAll(x1, x2)).collectAsMap();

        // Join
        // JavaPairRDD<Character, Double> keyedDou = kv.mapValues(x -> new Random().nextDouble());
        // Map<Character, Tuple2<String, Double>> join = kv.join(keyedDou).collectAsMap();

        session.close();
    }
}
