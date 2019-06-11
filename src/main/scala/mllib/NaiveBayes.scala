package mllib

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{MLUtils, SVMDataGenerator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


/**
  * created by LMR on 2019/6/10
  */
object Naive_Bayes {//要求特征值非负

  Logger.getRootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("naiveBayes")
    val sc = new SparkContext(conf)

    //产生数据
   //SVMDataGenerator.main(Array("local[*]", "E://output"))
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "E://output")

    //划分训练集喝测试集
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6,0.4), seed = 11L)
    val train: RDD[LabeledPoint] = splits(0)
    val test: RDD[LabeledPoint] = splits(1)

    //建立贝叶斯模型
    val model: NaiveBayesModel = NaiveBayes.train(train, lambda = 1.0, modelType = "multinomial")

    //测试集进行测试
    val predictionAndLabel: RDD[(Double, Double)] = test.map(p => (model.predict(p.features), p.label))

    val print_prediction: Array[(Double, Double)] = predictionAndLabel.take(20)
    for (elem <- print_prediction) {println(elem._1 + "\t" + elem._2)}

    //准确率
    val accuracy: Double = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(accuracy)

  }

}
