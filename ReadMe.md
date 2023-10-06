# 大数据技术学习  

本教程将根据当下的主流技术与版本进行相关教程的编写，考虑到当前往上主要以老旧版本的教程居多，对于较新版的教程明显比较
匮乏。考虑到新版诸多API存在破坏性更新，致使新人在使用新版本的SDK上进行开发学习时往往会被劝退。为此本系列将围绕新版进
行编写并辅以实际项目代码以及博客内容。  

相关技术版本选型如下：  
* Hadoop：3.1.1  
* HBase：2.4.0  
* Spark：2.4.0  
* JDK：1.8

围绕本源码项目的对应的教程文档可以通过[我的知乎专栏](https://www.zhihu.com/column/c_1693727391665512448)

---

## 基础教程  

对于HDFS相信大家都耳熟能详了，为了快速的巩固对于其中`mapreduce`的理解，用户可以结合[本教程](/docs/MapReduceBasic.md)
进行学习，其中相关源码均在`mapreducedemo`文件夹下。  

## HBase教程  

本教程需要集合实际的项目目录进行学习，本项目的主要目录组成如下（hbasedemo）。  

* normaldemo：包含各类常用操作以及Phoenix使用方式  
* mapreducedemo：包含MapReduce操作HBase的支持  
* obsererdemo：包含协处理器相关使用方式  

## Spark教程  

本教程当前主要以Java为基础进行Demo编写，主要由以下几个文件组成，有关Spark的其他更
详细的可查阅[本教程](/sparkdemo/ReadMe.md)。    

* AggregateSpark：包含聚合操作
* JavaSparkPi：基础入门  
* JoinSpark：连接操作相关  
* RDDWithSpark：RDD操作相关  
* SparkWithHBase：与HBase操作相关  
