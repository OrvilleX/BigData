package cn.orvillex.normaldemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseCounterTest {
    private Table table;

    public HBaseCounterTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("mytable");
        
        if (!admin.tableExists(tableName)) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("mycf");
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(columnFamilyDescriptor).build();
            admin.createTable(tableDescriptor);
        }

        
        table = connection.getTable(tableName);
    }

    /**
     * 单计数器
     * @throws IOException
     */
    @Test
    public void incrementTest() throws IOException {
        long cnt1 = table.incrementColumnValue(Bytes.toBytes("row5"), Bytes.toBytes("mycf"), Bytes.toBytes("hits"), 1);
        long cnt2 = table.incrementColumnValue(Bytes.toBytes("row5"), Bytes.toBytes("mycf"), Bytes.toBytes("hits"), 1);

        long current = table.incrementColumnValue(Bytes.toBytes("row5"), Bytes.toBytes("mycf"), Bytes.toBytes("hits"), 0);

        long cnt3 = table.incrementColumnValue(Bytes.toBytes("row5"), Bytes.toBytes("mycf"), Bytes.toBytes("hits"), -1);

        System.out.printf("the cnt2: %d, current: %d, cnt3: %d", cnt2, current, cnt3);
    }

    /**
     * 
     * @throws IOException
     */
    @Test
    public void incrementArrayTest() throws IOException {
        Increment increment = new Increment(Bytes.toBytes("row6"));

        increment.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("hits"), 1);
        increment.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("clicks"), 1);
        increment.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("hits"), 10);
        
        Result result = table.increment(increment);
        for (Cell cell : result.rawCells()) {
            String str = Bytes.toString(cell.getValueArray());
            System.out.println(str);
        }
    }
}
