package mllib.classsifiers

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by LMR on 2019/6/10
  */
object SVMClassifier {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("svm")
    val sc = new SparkContext(conf)

    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "E:\\IDEAWorkPlace\\SparkTest\\src\\main\\scala\\mllib\\data\\sample_libsvm_data.txt")

    //划分数据
    //val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6,0.4), seed = 11L)
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6,0.4), seed = 11L)
    val train: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    val numIterations = 100
    val model: SVMModel = SVMWithSGD.train(train, numIterations)

    //测试
    val predictionAndLabel: RDD[(Double, Double)] = test.map { point =>
      val prediction: Double = model.predict(point.features)
      (prediction, point.label)
    }

    val print_prediction: Array[(Double, Double)] = predictionAndLabel.take(20)
    for (elem <- print_prediction) {println(elem._1 + "\t" + elem._2)}

    //准确率
    val accuracy: Double = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(accuracy)

  }

}
