package mllib

import breeze.optimize.proximal.LogisticGenerator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator, LogisticRegressionDataGenerator, MLUtils, SVMDataGenerator}
import org.apache.spark.rdd.RDD

/**
  * created by LMR on 2019/6/6
  */
object MLlibDataFormat {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SpparkMLlib").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //MLUtils用于辅助加载、保存、处理MLlib相关算法所需要的数据
    //MLUtils.loadLibSVMFile()
    // 参数分别为：SparkContext，数据数量，聚类数，数据维度，初始中心分布的缩放因子，分区数
    val kmeansData: RDD[Array[Double]] = KMeansDataGenerator.generateKMeansRDD(sc, 40, 5, 3, 1.0, 2)
    val data: Array[Array[Double]] = kmeansData.take(5)
    for (elem <- data) {
     elem.foreach(f => print(f," "))
      println()
    }

    println("-----------------------线性RDD----------------")
    //用于生成线性回归的训练样本
    //参数分别为：SparkContext, 数据量， 特征维度， Epsilon因子， 分区数
    //Epsilon因子，，随机生成的数都乘以该因子，用于数据缩放
    val linearData: RDD[LabeledPoint] = LinearDataGenerator.generateLinearRDD(sc, 40, 3, 1.0, 2, 0.0)
    val datal: Array[LabeledPoint] = linearData.take(5)
    for (elem <- datal){
      println(elem)
    }

    println("-----------------------------------Lgistic数据--------------------")
    //其余参数和上面相同，最后一个表示数据中标签为1的概率
    val logicData: RDD[LabeledPoint] = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 40, 3, 1.0, 2, 0.5)
      val datalr: Array[LabeledPoint] = logicData.take(5)
    datalr.foreach(f => println(f))

    println("----------------------------------SVM样本数据-------------------------")
   // SVMDataGenerator中已经封装

  }

}
