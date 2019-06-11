package mllib.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LogisticRegressionDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by LMR on 2019/6/9
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LogisticRegression")
    val sc = new SparkContext(conf)

    //val data: RDD[LabeledPoint] = LinearDataGenerator.generateLinearRDD(sc, 200,20,0.5)
    val data: RDD[LabeledPoint] = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 200, 20, 0.3)
    Logger.getRootLogger.setLevel(Level.ERROR)

    //划分训练集和测试集
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed =  11L)

    val train: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    //建立模型并训练
    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train)

    //测试
    val predictionAndLabel: RDD[(Double, Double)] = test.map {
      case LabeledPoint(label, features) =>
        val prediction: Double = model.predict(features)
        (prediction, label)
    }
    val print_prediction: Array[(Double, Double)] = predictionAndLabel.take(20)
    for (elem <- print_prediction) {println(elem)}

    //计算误差
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val accuracy: Double = metrics.accuracy
    println("Accuracy = ",accuracy)


  }
}
