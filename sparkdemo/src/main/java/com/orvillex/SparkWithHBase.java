package com.orvillex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Spark中操作HBase
 * 可参考： https://github.com/IvanFernandez/hbase-spark-playground/blob/ea42c089b339ab6cb74b4869af907c0e1d95ebb9/src/main/java/spark/examples/SparkToHBaseWriter.java
 */
public final class SparkWithHBase {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().master("local").appName("hello-wrold").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("row1"), true);
        scan.withStopRow(Bytes.toBytes("row11"), true);
        scan.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));

        String scanToString = TableMapReduceUtil.convertScanToString(scan);
        
        conf.set(TableInputFormat.SCAN, scanToString);
        conf.set(TableInputFormat.INPUT_TABLE, "mytable");

        // 远程连接
        // conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03");
        // conf.set("hbase.zookeeper.property.clientPort","2181");
        
        JavaPairRDD<ImmutableBytesWritable, Result> HBaseRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
                    ImmutableBytesWritable.class, Result.class);
        
        // 再将以上结果转成Row类型RDD
        JavaRDD<Row> HBaseRow = HBaseRdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {

            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws IOException {
                Result result = tuple2._2;
                String rowKey = Bytes.toString(result.getRow());
                String birthday = Bytes.toString(result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("qual1")));
                return RowFactory.create(rowKey, birthday);
            }
        });
        
        // 顺序必须与构建RowRDD的顺序一致
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("birthday", DataTypes.StringType, true));
        
        // 构建schema
        StructType schema = DataTypes.createStructType(structFields);
        
        // 生成DataFrame
        Dataset<Row> HBaseDF = spark.createDataFrame(HBaseRow, schema);
        HBaseDF.show();
        
        // 获取birthday多版本数据
        JavaRDD<Row> multiVersionHBaseRow = HBaseRdd
        .mapPartitions(new FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Row> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> t) throws Exception {

                List<Row> rows = new ArrayList<Row>();
                while (t.hasNext()) {
                    Result result = t.next()._2();
                    String rowKey = Bytes.toString(result.getRow());
                    
                    // 获取当前rowKey的family_bytes列族对应的所有Cell
                    List<Cell> cells = result.getColumnCells(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
                    for (Cell cell : cells) {
                        String birthday = Bytes.toString(CellUtil.cloneValue(cell));
                        rows.add(RowFactory.create(rowKey, birthday, cell.getTimestamp()));
                    }
                }
                return rows.iterator();
            }
        });
        List<StructField> multiVersionStructFields = Arrays.asList(
                DataTypes.createStructField("rowKey", DataTypes.StringType, true),
                DataTypes.createStructField("birthday", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp", DataTypes.LongType, true));

        // 构建schema
        StructType multiVersionSchema = DataTypes.createStructType(multiVersionStructFields);
        // 生成DataFrame
        Dataset<Row> multiVersionHBaseDF = spark.createDataFrame(multiVersionHBaseRow, multiVersionSchema);
        multiVersionHBaseDF.show();

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Result.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        JavaPairRDD<ImmutableBytesWritable, Result> writeRow = HBaseRdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, ImmutableBytesWritable, Result>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Result> call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws IOException {
                return tuple2;
            }
        });
        writeRow.saveAsNewAPIHadoopDataset(job.getConfiguration());
        spark.stop();
    }
}
