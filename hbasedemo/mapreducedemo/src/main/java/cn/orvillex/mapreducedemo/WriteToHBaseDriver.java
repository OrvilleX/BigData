package cn.orvillex.mapreducedemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteToHBaseDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WriteToHBaseDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Max temperature");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        TableMapReduceUtil.initTableReducerJob("mytable", IntSumReducer.class, job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text,LongWritable,LongWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            byte[] row = Bytes.toBytes("row10");
            byte[] family = Bytes.toBytes("mycf");
            byte[] column = Bytes.toBytes("text");
            byte[] val = Bytes.toBytes("val1");

            Put put = new Put(row);
            put.addColumn(family, column, val);

            context.write(new LongWritable(1), new LongWritable(1));
        }
    }

    static class IntSumReducer extends TableReducer<LongWritable, LongWritable, Text> {
 
        private Text resultKey = new Text();
     
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            Put put = new Put(key.toString().getBytes());
            put.addColumn("mycf".getBytes(), "count".getBytes(), Bytes.toBytes(sum));
            context.write(resultKey, put);
        }
     
    }
}
