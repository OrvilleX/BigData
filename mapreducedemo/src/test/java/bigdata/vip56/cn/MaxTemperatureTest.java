package bigdata.vip56.cn;

import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static org.hamcrest.core.Is.is;
import org.junit.Test;

public class MaxTemperatureTest {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("g://data.txt");
        Path output = new Path("g://result.txt");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] {
            input.toString(),
            output.toString()
        });

        assertThat(exitCode, is(0));
    }
}