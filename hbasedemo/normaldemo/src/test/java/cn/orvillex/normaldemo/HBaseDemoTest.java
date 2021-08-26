package cn.orvillex.normaldemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseDemoTest {
    private Table table;

    public HBaseDemoTest() throws IOException {
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

    @Test
    public void putArrayTest() throws IOException {
        List<Put> puts = new ArrayList<Put>();
        
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"), Bytes.toBytes("val"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
        puts.add(put3);

        table.put(puts);

        /**
         * 对于这类批量操作，服务端采用遍历的方式执行，也就意味着其中一个出现，其他已执行成功
         * 的操作并不会受此影响，而是会正常运行。同时这个遍历不一定按照我们添加到列表中的顺序
         * 执行
         */
    }

    /**
     * 原子性操作，即保证修改数据为需要修改的版本
     * @throws IOException
     */
    @Test
    public void compareAndSetTest() throws IOException {

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"), null);
        boolean res1 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("mycf")).qualifier(Bytes.toBytes("qual1")).
            ifEquals(Bytes.toBytes("val")).thenPut(put1);
        System.out.println("Put applied: " + res1);

        boolean res2 = table.checkAndMutate(Bytes.toBytes("row1"), Bytes.toBytes("mycf")).qualifier(Bytes.toBytes("qual1")).
            ifEquals(Bytes.toBytes("val")).thenPut(put1);
        System.out.println("Put applied: " + res2);

        boolean res3 = table.checkAndMutate(Bytes.toBytes("row2"), Bytes.toBytes("mycf")).qualifier(Bytes.toBytes("qual1")).
            ifEquals(Bytes.toBytes("val1")).thenPut(put1);
        System.out.println("Put applied: " + res3);

        /**
         * 如果需要进行删除则通过thenDelete进行
         */

        /**
         * 有一种特别的检查通过checkAndPut调用来完成，即只有在另外一个值
         * 不存在的情况下，才执行这个修改。要执行这个操作只需要将参数value
         * 设置为null即可，只要指定列不存在，就可以成功执行修改操作。
         */
    }

    /**
     * 获取单一数据
     * @throws IOException
     */
    @Test
    public void getTest() throws IOException {
        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));

        if(table.exists(get)) {
            Result result = table.get(get);
            byte[] val = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
            String strVal = Bytes.toString(val);
            System.out.println("Value: " + strVal);
        }

        /**
         * 对于get方法除了可以根据具体的列簇跟列获取具体的列数据
         * 还可以通过setTimeRange与setTimeStamp设定具体的时间戳
         * 与时间范围，最后还可以通过setMaxVersions设置需要获取的
         * 数据版本，如果设置超过1则通过Result的getColumn即可获得
         * 多个版本的数据。
         * 
         * 对于Result的结果，需要开发者根据列中存储数据的实际格式通过
         * Bytes.toXXX进行转换，具体如下
         * Bytes.toString
         * Bytes.toBoolean
         * Bytes.toLong
         * Bytes.toFloat
         * Bytes.toInt
         */
    }

    /**
     * 批量获取
     * @throws IOException
     */
    @Test
    public void getArrayTest() throws IOException {
        List<Get> getList = new ArrayList<Get>();

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        getList.add(get1);

        Get get2 = new Get(Bytes.toBytes("row2"));
        get2.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        getList.add(get2);

        Result[] results = table.get(getList);

        for (Result result : results) {
            byte[] val = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
            System.out.println("The val: " + Bytes.toString(val));
        }
    }

    @Test
    public void deleteTest() throws IOException {
        Delete del = new Delete(Bytes.toBytes("row1"));
        del.setTimestamp(1);
        del.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        del.addColumns(Bytes.toBytes("mycf"), Bytes.toBytes("qual2"));

        table.delete(del);

        /**
         * 其中addColumn用于删除对应列的最新记录，而addColumns则是
         * 删除对应列表的所有版本记录
         */
    }

    @Test
    public void batchTest() {
        List<Row> batch = new ArrayList<Row>();

        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        batch.add(get);

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"), Bytes.toBytes("123"));
        batch.add(put);

        Delete del = new Delete(Bytes.toBytes("row1"));
        del.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        batch.add(del);

        Object[] results = new Object[batch.size()];
        try {
            table.batch(batch, results);    
        } catch (Exception e) {
        }

        

        /**
         * 其返回值需要根据对应请求行为，而产生对应的累，具体如下
         * 
         * null: 操作与远程服务器的通信失败
         * EmptyResult: Put和Delete操作成功后的返回结果
         * Result: Get操作成功的返回结果
         */
    }

    /**
     * 扫描技术，其类似于传统数据库中的游标
     * @throws IOException
     */
    @Test
    public void scanTest() throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("row1"), true);
        scan.withStopRow(Bytes.toBytes("row11"), true);

        /**
         * 由于HBase中的数据是按列簇进行存储的，如果扫描不读取
         * 某个列簇，那么整个列簇文件就都不会被读取，这就是列式
         * 存储的优势
         */
        scan.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            System.out.println(result);
        }

        /**
         * 对于ResultScanner读取的数据，并不会通过一次RPC请求
         * 就将所有匹配的数据行进行返回，而实以行为单位进行返回。
         * 
         * 如果需要返回一次RPC返回多个数据则需要通过setCaching或
         * 配置项hbasse.client.scanner.caching进行配置，但是该值
         * 如果设置过大，将导致服务端性能下降。
         * 
         * 如果存在单行数据量过大，这些行有可能超过客户端进程的内存
         * 容量，可以通过批量的方式获取，即通过setBatch决定每次next
         * 可以读取的列的数量。
         */
    }

    /**
     * 行过滤器
     * @throws IOException
     */
    @Test
    public void rowFilterTest() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));

        Filter filter1 = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row22")));
        scan.setFilter(filter1);
        ResultScanner scanner1 = table.getScanner(scan);
        for (Result result : scanner1) {
            System.out.println(result);
        }
        scanner1.close();

        Filter filter2 = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(".*2"));
        scan.setFilter(filter2);
        ResultScanner scanner2 = table.getScanner(scan);
        for (Result result : scanner2) {
            System.out.println(result);
        }
        scanner2.close();
    }

    /**
     * 列簇过滤器
     * @throws IOException
     */
    @Test
    public void familyFilterTest() throws IOException {
        Scan scan = new Scan();

        Filter filter1 = new FamilyFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("mycf2")));
        scan.setFilter(filter1);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            System.out.println(result);
        }

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.setFilter(filter1);
        Result result = table.get(get1);
        System.out.println(result);
    }

    /**
     * 列名过滤器
     * @throws IOException
     */
    @Test
    public void qualifierFilterTest() throws IOException {
        Scan scan = new Scan();

        Filter filter = new QualifierFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("qual2")));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            System.out.println(result);
        }
        resultScanner.close();

        Get get = new Get(Bytes.toBytes("row2"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);
    }

    /**
     * 值过滤器
     * @throws IOException
     */
    @Test
    public void valueFilterTest() throws IOException {
        Filter filter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("1"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell c : result.rawCells()) {
                System.out.println("q: " + Bytes.toString(c.getQualifierArray()) + " v: " + Bytes.toString(c.getValueArray()));
            }
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row1"));
        get.setFilter(filter);
        Result result = table.get(get);
        for (Cell c : result.rawCells()) {
            System.out.println("q: " + Bytes.toString(c.getQualifierArray()) + " v: " + Bytes.toString(c.getValueArray()));
        }
    }

    /**
     * 单列值过滤器
     * @throws IOException
     */
    @Test
    public void singleColumnValueFilterTest() throws IOException {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"), CompareOperator.EQUAL, new SubstringComparator("1"));
        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row1"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);
    }

    /**
     * 前缀过滤器
     * @throws IOException
     */
    @Test
    public void prefixFilterTest() throws IOException {
        Filter filter = new PrefixFilter(Bytes.toBytes("row"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("row1"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);
    }

    @Test
    public void pageFilterTest() throws IOException {
        Filter filter = new PageFilter(15);

        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.withStartRow(Bytes.toBytes("row1"));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    @Test
    public void inclusiveStopFilterTest() throws IOException {
        Filter filter = new InclusiveStopFilter(Bytes.toBytes("row3"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.withStartRow(Bytes.toBytes("row1"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    /**
     * 时间戳过滤器
     * @throws IOException
     */
    @Test
    public void timestampFilterTest() throws IOException {
        List<Long> ts = new ArrayList<>();
        ts.add(5l);
        ts.add(10l);
        ts.add(15l);
        Filter filter = new TimestampsFilter(ts);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    /**
     * 列分页过滤器
     */
    @Test
    public void columnPaginationFilterTest() throws IOException {
        Filter filter = new ColumnPaginationFilter(1, 1);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    @Test
    public void skipFilterTest() throws IOException {
        Filter filter = new ValueFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("val")));
        Filter filter2 = new SkipFilter(filter);

        Scan scan = new Scan();
        scan.setFilter(filter2);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    @Test
    public void filterListTest() throws IOException {
        List<Filter> filters = new ArrayList<>();

        Filter filter1 = new ValueFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("val")));
        filters.add(filter1);

        Filter filter2 = new ColumnPaginationFilter(1, 1);
        filters.add(filter2);

        FilterList filterList = new FilterList(filters);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }
}
