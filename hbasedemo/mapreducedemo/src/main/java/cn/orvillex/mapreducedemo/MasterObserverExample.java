package cn.orvillex.mapreducedemo;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;

public class MasterObserverExample implements MasterObserver, Coprocessor {
    
    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc,
            RegionInfo[] regions) throws IOException {
        String tableName = regions[0].getTable().getNameAsString();

        System.out.println("create table:" + tableName);
    }
}
