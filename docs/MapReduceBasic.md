# MapReduce教程

## 一、项目基础  

### 1. 如何开发应用

首先需要将[hadoop 3.1.1](https://archive.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz)解压到对应文件夹下，然后下载[winutils](https://github.com/cdarlint/winutils)将对应版本的文件复制到bin目录下，完成之后我们还需要调整环境变量，主要设置如下：

```bash
HADOOP_HOME=hadoop路径
PATH=%HADOOP%\bin
```

同时为了还需要将hadoop.dll复制到`C:\Windows\System32`下保证其访问有效性，接着只要将项目生成对应的
jar文件后通过如下方式即可运行：  

```bash
export HADOOP_CLASSPATH=mapreducedemo.jar
hadoop MaxTempreatureDriver input/sample.txt output
```  

当然为了更便于测试本项目使用了`hadoop-minicluster`依赖更佳便于整体的测试，其中单元测试中就可以完全在
本地模拟一个环境进行执行测试。  

## 二、学习知识  

由`fs.default.name`设置Hadoop的默认文件系统，而由`dfs.replication`设置块副本数量，其默认为3，测试
开发环境需要设置为1副本。  

### 1. hadoop fs指令集  

* copyFromLocal： 将本地文件复制到HDFS  
* copyToLocal：将HDFS文件复制到本地  
* mkdir：创建文件夹  
* ls：查询文件  


### 2. FileSystem  

如果需要读取HDFS中的文件，我们可以通过其提供的`FileSystem`类，并通过其中提供的各类`get`方法获取
到实例。读取文件则需要通过`open`方法打开指定的路径后通过`FSDataInputStream`对象进行操作。该方法
继承了`DataInputStream`对象，实现了`Seekable`与`PositionedReadable`接口以实现随机访问功能。具
体读取字节等通过`IOUtils`工具类。下面通过代码示例：  

```java
FileSystem fs = FileSystem.get(uri, conf);
FSDataInputStream in = null;
try {
    in = fs.open(new Path(uri));
    IOUtils.copyBytes(in, System.out, 4096, false);
    in.seek(0);
    IOUtils.copyBytes(in, System.out, 4096, false);
} finally {
    IOUtils.closeStream(in);
}
```

如果我们需要写入文件则可以通过`create`或`append`创建或者追加一个文件，通过其返回的`FSDataOutputStream`
兑现实现数据的写入，因为HDFS的特性，我们还可以通过`Progressable`接收异步的进度。下面我们通过一个简单的
例子来说如何使用。  

```java
FileSystem fs = FileSystem.get(uri, conf);
InputStream in = new BufferedInputStream(new FileInputStream(srcPath));
OutputStream out = fs.create(new Path(uri), new Progressable() {
    public void progress() {
        System.out.print(".");
    }
});
IOUtils.copyBytes(in, out, 4096, true);
```  

除了以上几个常用的写入写出以外还支持以下对HDFS的操作方法：  

* mkdirs: 创建文件夹  
* exists: 判断文件是否存在  
* getFileStatus: 查询文件元数据，通过返回FileStatus对象  
* listStatus: 列出目录下所有文件的元数据  
* globalStatus: 支持通配符查询文件元数据  
* delete: 删除数据  
* 

### 3. OutputCommitter  

如果希望在一个任务完成后自行处理相关的后续清理或其他工作可以自行实现`OutputCommitter`接口，并
通过`JobConf`的`setCommitter`配置进去。而默认采用`FileOutputCommitter`实现。  

### 4. Partitioner  

用于指定分区键，默认的实现为`HashPartitioner`类，其具体的方法签名如下：  

```java
public abstract class Partitioner<KEY, VALUE> {
    public abstract int getPartition(KEY key, VALUE value, int numPartitions);
}
```  

### 5. 多个输入  

考虑到实际的业务场景可能需要一个mapreduce处理两个文件，那么我们就需要使用如下的方式进行添加。  

```java
MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, MaxTemperatureMapper.class);
MultipleInputs.addInputPath(job, metofficeInputPath, TextInputFormat.class, MetofficMaxTemperatureMapper.class);
```  

### 6. 多个输出  

为了能够自定义输出的文件目录接口，可以通过`MultipleOutputs`实现，其需要在任务的`reducer`中
使用，并通过其将记录输出即可。  

```java
static class MultipleOutputReducer extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        for (Text value : values) {
            multipleOutputs.write(NullWritable.get(), value, key.toString());
        }
    }

    @Override
    protected void cleanup(Context context) {
        multipleOutputs.close();
    }
}
```  

### 7. 计数  

除了诸多自带的计数器，用户也可以通过mapreduce任务中的context进行任务统计。  

```java
context.getCounter(Temperature.MISSING).increment(1);
context.getCounter("TemperatureQuality", getQuality()).increment(1);
```  


