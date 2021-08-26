package cn.orvillex.observerdemo;

import java.io.IOException;
import java.util.Map;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        enableCoprocessor(admin);
    }

    private static void enableCoprocessor(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("mytable");
        admin.disableTable(tableName);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("mycf");
        CoprocessorDescriptor coprocessorDescriptor = CoprocessorDescriptorBuilder.newBuilder("Coprocessor.RegionObserverExample")
            .setJarPath("hdfs://<namenode>:<port>/user/<hadoop-user>/coprocessor.jar")
            .setPriority(100)
            .build();
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(columnFamilyDescriptor)
            .setCoprocessor(coprocessorDescriptor)
            .build();
        admin.modifyTable(tableDescriptor);
        admin.enableTable(tableName);
    }

    private static void disableCoprocessor(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("mytable");
        admin.disableTable(tableName);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("mycf");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(columnFamilyDescriptor)
            .build();
        admin.modifyTable(tableDescriptor);
        admin.enableTable(tableName);
    }

    private static void endpointDemo() throws ServiceException, Throwable {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("users");
        Table table = connection.getTable(tableName);
        
        final Sum.SumRequest request = Sum.SumRequest.newBuilder().setFamily("salaryDet").setColumn("gross").build();

        Map<byte[], Long> results = table.coprocessorService(
            Sum.SumService.class,
            null,  /* start key */
            null,  /* end   key */
            new Batch.Call<Sum.SumService, Long>() {
                @Override
                public Long call(Sum.SumService aggregate) throws IOException {
                    BlockingRpcCallback<Sum.SumResponse> rpcCallback = new BlockingRpcCallback<>();
                    aggregate.getSum(null, request, rpcCallback);
                    Sum.SumResponse response = rpcCallback.get();
                    return response.hasSum() ? response.getSum() : 0L;
                }
            }
        );

        for (Long sum : results.values()) {
            System.out.println("Sum = " + sum);
        }
    }
}
