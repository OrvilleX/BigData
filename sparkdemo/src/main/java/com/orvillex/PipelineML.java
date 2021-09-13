package com.orvillex;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public final class PipelineML {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkML").setMaster("local");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> df = session.read().json("sparkdemo/data/simple-ml.json");
        Dataset<Row>[] data = df.randomSplit(new double[] {0.7, 0.3});

        RFormula rForm = new RFormula();
        LogisticRegression lr = new LogisticRegression();
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { rForm, lr });

        Seq<String> formulaParam = JavaConverters.asScalaIteratorConverter(Arrays.asList("lab ~ . + color:value1", "lab ~ . + color:value1 + color:value2").iterator()).asScala().toSeq();

        ParamMap[] params = new ParamGridBuilder()
            .addGrid(rForm.formula(), formulaParam)
            .addGrid(lr.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
            .addGrid(lr.regParam(), new double[]{0.1, 2.0})
            .build();

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
            .setMetricName("areaUnderROC")
            .setRawPredictionCol("prediction")
            .setLabelCol("label");
        
        TrainValidationSplit tvs = new TrainValidationSplit()
            .setTrainRatio(0.75)
            .setEstimatorParamMaps(params)
            .setEstimator(pipeline)
            .setEvaluator(evaluator);

        TrainValidationSplitModel model = tvs.fit(data[0]);

        System.out.println(evaluator.evaluate(model.transform(data[1])));

        model.write().overwrite().save("sparkdemo/data/model");
    }
}
