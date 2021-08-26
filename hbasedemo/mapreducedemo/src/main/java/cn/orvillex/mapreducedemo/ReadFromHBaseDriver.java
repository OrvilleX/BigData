package cn.orvillex.mapreducedemo;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadFromHBaseDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WriteToHBaseDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Max temperature");
        job.setJarByClass(getClass());
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setMapperClass(ReadFromHBaseMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("hits"));
        TableMapReduceUtil.initTableMapperJob("mytable", scan, ReadFromHBaseMapper.class, Text.class, Text.class, job);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ReadFromHBaseMapper extends TableMapper<Text, Text> {
        private Text resultKey = new Text();
        private Text resultVal = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
                throws IOException, InterruptedException {
                    StringBuilder sb = new StringBuilder();
            // 通过cell获取表中的列值
            for (Cell cell : value.listCells()) {
                String val = Bytes.toString(Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueOffset() + cell.getValueLength()));
                sb.append(val).append("|");
            }
            // 写入HDFS
            resultKey.set(Bytes.toString(value.getRow()));
            resultVal.set(sb.deleteCharAt(sb.length()-1).toString());
            context.write(resultKey, resultVal);
        }
    }
    
}
