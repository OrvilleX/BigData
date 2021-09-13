package com.orvillex;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.RFormulaModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark MLlib相关学习
 */
public final class MachineLearnWithSpark {
    
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkML").setMaster("local");
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> df = session.read().json("sparkdemo/data/simple-ml.json");
        RFormula supervised = new RFormula().setFormula("lab ~ . + color: value1 + color: value2");
        supervised.setLabelCol("label1");
        supervised.setFeaturesCol("features2");

        RFormulaModel model = supervised.fit(df);
        Dataset<Row> preparedDF = model.transform(df);

        Dataset<Row>[] data = preparedDF.randomSplit(new double[]{0.7, 0.3});

        LogisticRegression lr = new LogisticRegression();
        lr.setLabelCol("label1");
        lr.setFeaturesCol("features2");
        
        lr.explainParams();

        LogisticRegressionModel lrModel = lr.fit(data[0]);

        lrModel.transform(data[1]).select("label1", "prediction").show();
    }
}
