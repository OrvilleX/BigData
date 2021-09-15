# Spark MLib

在Spark下进行机器学习，必然无法离开其提供的MLlib框架，所以接下来我们将以本框架为基础进行实际的讲解。首先我们需要了解其中最基本的结构类型，即转换器、估计器、评估器何流水线。

## 一、基础使用  

接下来我们将以

### 1. 特征工程  

这部分相关知识可以参考本人编写的[人工智能专题]()的开源教程，其中对该部分进行详细的说明，下面我们将就框架提供的`RFormula`进行具体的实战操作（这里熟悉R语言的可能对此比较熟悉，本身就是借鉴了R语言，但是仅实现了其中的一个子集），对于我们需要进行特征化的数据首先我们需要定义对应的线性模型公式，具体如下。  

```java
Dataset<Row> df = session.read().json("sparkdemo/data/simple-ml");
RFormula supervised = new RFormula().setFormula("lab ~ . + color: value1 + color: value2");
```

当然仅仅通过上述的方式还不能实现对数据的特征化，我们还需要通过数据对其进行训练，从而得到我们所需的转换器，为此我们需要使用其中的`fit`方法进行转换。  

```java
RFormulaModel model = supervised.fit(df);
```  

完成转换器的训练后我们就可以利用其进行实际的转换操作，从而生成特征`features`与标签`label`列，当然读者也可以通过`supervised.setLabelCol`设置标签列名，`supervised.setFeaturesCol`设置特征列名。对于监督学习都需要将数据分为样本数据与测试数据，为此我们需要通过以下方式将数据拆分。  

```java
Dataset<Row>[] data = preparedDF.randomSplit(new double[]{0.7, 0.3});
```  

### 2. 模型训练  

在Spark MLib中为估计器，这里我们将采用逻辑回归的算法做为演示，提供一个分类算法模型的训练，首先我们实例化我们需要的模型类，通过其提供的方式对将训练数据传入其中进行模型的训练。  

```java
LogisticRegression lr = new LogisticRegression();
LogisticRegressionModel lrModel = lr.fit(data[0]);
lrModel.transform(data[1]).select("label1", "prediction").show();
```  

如果在对数据进行特征工程的时候将标签以及特征列的名称进行了修改，那么我们也需要通过`lr.setLabelCol`以及`lr.setFeaturesCol`进行同步修改调整。同时框架也提供了`explainParams`方法打印模型中可供调整的参数。  

### 3. 流水线  

对于机器学习，后期工作基本就是对各种参数的调优，为此Spark提供了友好的流水线，并基于其本平台分布式计算集群的能力助力我们缩短对不同参数模型的训练与评估，从而提供最佳的参数模型供我们使用，下面我们将一步一步介绍如何使用其提供的该特性。  

首先我们定义工作流中涉及到的阶段步骤，具体如下所示。  

```java
Dataset<Row> df = session.read().json("sparkdemo/data/simple-ml.json");
Dataset<Row>[] data = df.randomSplit(new double[] {0.7, 0.3});

RFormula rForm = new RFormula();
LogisticRegression lr = new LogisticRegression();
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { rForm, lr });
```  

上述完成工作流水线各阶段的任务后，接下来我们就需要指定各阶段的参数列表，从而便于Spark形成不同的组合进行模型训练。  

```java
Seq<String> formulaParam = JavaConverters.asScalaIteratorConverter(Arrays.asList("lab ~ . + color:value1", "lab ~ . + color:value1 + color:value2").iterator()).asScala().toSeq();

ParamMap[] params = new ParamGridBuilder()
    .addGrid(rForm.formula(), formulaParam)
    .addGrid(lr.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
    .addGrid(lr.regParam(), new double[]{0.1, 2.0})
    .build();
```  

有了以上其实我们就可以单纯的进行模型训练了，但是这样训练除的模型并无法评估出最好的一个模型。我们需要指定一个评估器用来评估实际效果是否符合最佳。这里我们主要采用了`BinaryClassificationEvaluator`类。  

```java
BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")
    .setRawPredictionCol("prediction")
    .setLabelCol("label");
```  

最后我们需要能够自动调整超参数，并自动分配数据集的方式将上述的各部分组成从而形成最终有效的模型。  

```java
TrainValidationSplit tvs = new TrainValidationSplit()
    .setTrainRatio(0.75)
    .setEstimatorParamMaps(params)
    .setEstimator(pipeline)
    .setEvaluator(evaluator);
```  

而具体的使用与之前逻辑回归的方式如出一辙。  

```java
TrainValidationSplitModel model = tvs.fit(data[0]);
System.out.println(evaluator.evaluate(model.transform(data[1])));
```  

如果读者需要将该模型进行持久化可以采用` model.write().overwrite().save("sparkdemo/data/model");`该方式进行实际的持久化，当然读取时需要采用与写入一致的类，否则将无法正确读取。  

