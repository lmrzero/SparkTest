package mllib.regression

import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}

/**
  * created by LMR on 2019/6/10
  */
object Isotonic_Regression {
  def main(args: Array[String]): Unit = {

    val source: BufferedSource = Source.fromFile("E:\\IDEAWorkPlace\\SparkTest\\src\\main\\scala\\mllib\\sample_isotonic_regression_data.txt")
    val lines: Iterator[String] = source.getLines()
    val array: Array[String] = lines.toArray


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("isotonic")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.parallelize(array)
    val parsedData: RDD[(Double, Double, Double)] = data.map { line =>
      val parts: Array[Double] = line.split(",").map(_.toDouble)
      (parts(0), parts(1), 1.0)
    }

    //划分训练机和测试集
    val splits: Array[RDD[(Double, Double, Double)]] = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val train: RDD[(Double, Double, Double)] = splits(0)
    val test: RDD[(Double, Double, Double)] = splits(1)

    //建立保序模型，并训练

    val model: IsotonicRegressionModel = new IsotonicRegression().setIsotonic(true).run(train)
    val x: Array[Double] = model.boundaries
    val y: Array[Double] = model.predictions
    println("boundaries" + "\t" + "predictions")
    for (i <- 0 to x.length - 1)
      {
        println(x(i) + "\t" + y(i))
      }

    //误差计算
    val predictAndLabel: RDD[(Double, Double)] = test.map { point =>
      val predict: Double = model.predict(point._2)
      (predict, point._1)
    }
    val print_predict: Array[(Double, Double)] = predictAndLabel.take(20)
    println("prediction"+"\t"+"label")
    for (elem <- print_predict) {
      println(elem._1+"\t"+elem._2)
    }

    val meanSquaredError: Double = predictAndLabel.map {
      case (p, l) =>
        math.pow((p - l), 2)
    }.mean()
    println("Mean Squared Error = " + meanSquaredError)

  }

}
