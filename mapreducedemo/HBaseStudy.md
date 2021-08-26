# HBase权威指南核心知识梳理  

## 一、开发环境准备  

在完成Docker安装后通过如下命令启动对应容器服务  

```bash
docker run -d -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301  -p 16030:16030 -p 16020:16020 --name hbase harisekhon/hbase:2.1
```  

完成部署后访问本地`127.0.0.1:16010`服务，选择具体的机器，我们可以发现其通过主机名进行寻找，所以我们
需要将对应主机名对应到HOST中。  

## 二、基础使用  

首先我们需要连接到HBase实例中，如上我们在本地搭建一个基于本地的开发环境，而默认的
配置就是基于此，所以我们可以直接进行创建并连接。如果读者希望连接到具体的实例中可以
在Resources中放置`hbase-site.xml`配置文件，将会默认读取。  

```java
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

Table table = connection.getTable(tableName);
```  

如上述代码我们通过`HBaseConfiguration`创建了默认的配置项，并通过`ConnectionFactory`
根据对应配置创建实际的连接，并通过连接创建了控制对象`Admin`用于创建我们所需的表等基础信
息，随后我们通过表名获取对应的`Table`对象。代码中我们判断了对应的表是否存在，如果不存在
则通过`TableDescriptorBuilder`创建对应表的列簇等信息。  

以上以上操作后我们所需的表就准备好了，接下来我们就可以进行其他数据操作了。当然这里需要读
者根据后面的知识提前在table中通过shell创建对应的数据。  

### 1.新增/更新数据  

以下我们将带领读者新增一条数据，通过本示例就可以大致了解对于HBase SDK的使用风格。  

```java
Put put = new Put(Bytes.toBytes("row1"));
put.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("name"), Bytes.toBytes("test"));
table.put(put);
```  

掌握的以上单条数据的新增，对应了我们就可以快速的了解如何进行批量数据新增/更新操作。  

```java
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
```  

对于这类批量操作，服务端采用遍历的方式执行，也就意味着其中一个出现异常，其他已执行
成功的操作并不会受此影响，而是会正常运行。同时这个遍历不一定按照我们添加到列表中的
顺序执行。

### 2.原子操作  

对于多客户端同时操作同一个数据的情况下，往往会导致不可预见的行为。为了保障我们修改
的数据是我们所需要修改的数据，我们就需要在修改前进行判断，如果符合预期的要求则对数
据进行后续的操作。  

`对应的checkAndPut、checkAndDelete已经不建议使用，故下述代码将使用官方推荐的方式`  

```java
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
 * /
```  

有一种特别的检查通过checkAndPut调用来完成，即只有在另外一个值不存在的情况下，
才执行这个修改。要执行这个操作只需要将参数value设置为null即可，只要指定列不
存在，就可以成功执行修改操作。  

### 3.获取数据  

上述我们介绍了各类新增/更新数据的方式，下面我们将继续介绍如何获取我们
希望获取的列。  

```java
Get get = new Get(Bytes.toBytes("row1"));
get.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));

if(table.exists(get)) {
    Result result = table.get(get);
    byte[] val = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
    String strVal = Bytes.toString(val);
    System.out.println("Value: " + strVal);
}
```  

对于get方法除了可以根据具体的列簇跟列获取具体的列数据还可以通过`setTimeRange`
与`setTimeStamp`设定具体的时间戳与时间范围，最后还可以通过`setMaxVersions`设
置需要获取的数据版本，如果设置超过1则通过`Result.getColumn`即可获得多个版本的
数据。

对于Result的结果，需要开发者根据列中存储数据的实际格式通过Bytes.toXXX进行转
换，具体如下  

* Bytes.toString
* Bytes.toBoolean
* Bytes.toLong
* Bytes.toFloat
* Bytes.toInt

与Put操作类似，我们还可以批量的获取多个数据。  

```java
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
```

### 4.删除数据  

删除我们删除行、列簇或者具体的列数据。  

```java
Delete del = new Delete(Bytes.toBytes("row1"));
del.setTimestamp(1);
del.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
del.addColumns(Bytes.toBytes("mycf"), Bytes.toBytes("qual2"));

table.delete(del);
```  

其中addColumn用于删除对应列的最新记录，而addColumns则是删除对应列表的所有版本记录。  

### 5. 批处理  

上述的各类批量处理，其底层均是采用`batch`进行操作，所以我们可以直接使用
该方法同时将更新、获取与删除操作放入一个列表同时下发，当然对应的类型也需
要使用其基类`Row`。  

```java
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
```  

其返回值需要根据对应请求行为，而产生对应的累，具体如下：  

 * null: 操作与远程服务器的通信失败  
 * EmptyResult: Put和Delete操作成功后的返回结果  
 * Result: Get操作成功的返回结果  

### 6.扫描  

其技术类似传统数据库中的游标，故这类资源对于服务器的消耗较大，一般不
建议读者大量使用，即使使用了也要及时关闭，以降低对服务器的压力。  

```java
Scan scan = new Scan();
scan.withStartRow(Bytes.toBytes("row1"), true);
scan.withStopRow(Bytes.toBytes("row11"), true);

scan.addColumn(Bytes.toBytes("mycf"), Bytes.toBytes("qual1"));
ResultScanner scanner = table.getScanner(scan);

for (Result result : scanner) {
    System.out.println(result);
}
```  

由于HBase中的数据是按列簇进行存储的，如果扫描不读取某个列簇，那么
整个列簇文件就都不会被读取，这就是列式存储的优势。  

对于ResultScanner读取的数据，并不会通过一次RPC请求就将所有匹配的
数据行进行返回，而实以行为单位进行返回。  如果需要返回一次RPC返回
多个数据则需要通过setCaching或配置项hbasse.client.scanner.cach
ing进行配置，但是该值如果设置过大，将导致服务端性能下降。  

如果存在单行数据量过大，这些行有可能超过客户端进程的内存容量，
可以通过批量的方式获取，即通过setBatch决定每次next可以读取的列的数量。
