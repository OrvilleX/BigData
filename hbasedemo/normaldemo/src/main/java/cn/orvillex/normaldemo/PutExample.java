package cn.orvillex.normaldemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class PutExample {
    
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("mytable");
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("mycf");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(columnFamilyDescriptor).build();

        if (!admin.tableExists(tableName)) {
            admin.createTable(tableDescriptor);
        }

        Table table = connection.getTable(tableName);

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), Bytes.toBytes("test"));
        table.put(put);
    }

}
