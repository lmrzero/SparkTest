package mllib.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by LMR on 2019/6/9
  */
object LinearRegression {

  def main(args: Array[String]): Unit = {
    val lrConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LR")
    val sc = new SparkContext(lrConf)

    Logger.getRootLogger.setLevel(Level.ERROR)

    val data: RDD[LabeledPoint] = LinearDataGenerator.generateLinearRDD(sc,200,10,0.5)

    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model: LinearRegressionModel = LinearRegressionWithSGD.train(data, numIterations, stepSize, miniBatchFraction)
    println("权重和偏移量")
    println(model.weights)
    println(model.intercept)

    val prediction: RDD[Double] = model.predict(data.map(_.features))
    val predictionAndLabel: RDD[(Double, Double)] = prediction.zip(data.map(_.label))

    val print_predict: Array[(Double, Double)] = predictionAndLabel.take(50)
    for (elem <- print_predict) {
      println(elem)
    }

    val loss: Double = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)


    val rmse: Double = math.sqrt(loss / data.count())
    println(s"Test RMSE = $rmse")

    val ModelPath = "E://model"
    model.save(sc,ModelPath)
    val sameModel: LinearRegressionModel = LinearRegressionModel.load(sc, ModelPath)
  }

}
