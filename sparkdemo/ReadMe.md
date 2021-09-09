# Spark学习教程  

## 通用知识  

### 1. 通过命令行获取参数  

往往我们在实际开发过程中需要根据外部实际情况调整具体的运行参数，如读取数据的来源、
格式、输出的地址、格式的等。考虑到其内部其他框架已依赖了`commons-cli`类库，所以
可以直接利用其提供的`CommandLineParser`来帮助我们提供服务。下面我们将列举具体
的使用方式。  

```java
CommandLineParser parser = new BasicParser();
Options commandOptions = new Options();
commandOptions.addOption("f", "file", true, "input file");
commandOptions.addOption("o", "outfile", true, "output file");

try {
    CommandLine cmdline = parser.parse(commandOptions, args);
    String file = cmdline.getOptionValue("f");
    String out = cmdline.getOptionValue("o");
} catch (ParseException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
}
```  

如果需要在提交任务的时候提交对应的参数，则可以通过下述的方式进行提交即可。  

```bash
spark-submit --master xxx demo.jar "-f" "inputfile" "-o" "outfile"
```  


