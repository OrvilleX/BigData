package bigdata.vip56.cn.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.vip56.cn.NcdcRecordParser;

public class AvroGenericMaxTemperature extends Configured implements Tool {

    private static final Schema SCHEMA = new Schema.Parser().parse(
    "{" +
        "\"type\": \"record\"," +
        "\"name\": \"WeatherRecord\"," +
        "\"doc\": \"A weather reading.\"," +
        "\"fields\": [" +
            "{\"name\": \"year\", \"type\": \"string\"}," +
            "{\"name\": \"temperature\", \"type\": \"int\"}," +
            "{\"name\": \"stationId\", \"type\": \"string\"}" +
    "]}");

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>> {
        private NcdcRecordParser parser = new NcdcRecordParser();
        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                record.put("year", parser.getYear());
                record.put("temperature", parser.getAirTemperature());
                record.put("stationId", 1);
                context.write(new AvroKey<>(parser.getYear()), new AvroValue<GenericRecord>(record));
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

        @Override
        protected void reduce(AvroKey<String> arg0, Iterable<AvroValue<GenericRecord>> arg1, Context context) throws IOException, InterruptedException {
            GenericRecord max = null;
            for (AvroValue<GenericRecord> value : arg1) {
                GenericRecord record = value.datum();
                if (max == null || (Integer)record.get("temperature") > (Integer)max.get("temperature")) {
                    max = new GenericData.Record(SCHEMA);
                    max.put("year", record.get("year"));
                    max.put("temperature", record.get("temperature"));
                    max.put("stationId", record.get("stationId"));
                    context.write(new AvroKey<GenericRecord>(max), NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(getClass());
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, SCHEMA);
        AvroJob.setOutputKeySchema(job, SCHEMA);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        return 0;
    }

    public static void main( String[] args ) throws Exception
    {
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
        System.exit(exitCode);
    }
}
