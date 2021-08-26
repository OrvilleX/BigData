package cn.orvillex.mapreducedemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class ReadFromHBaseDriverTest {
    
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("g://output.txt");

        FileSystem fs = FileSystem.getLocal(conf);

        ReadFromHBaseDriver driver = new ReadFromHBaseDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] {
            input.toString()
        });

        assertThat(exitCode, is(0));
    }
}
