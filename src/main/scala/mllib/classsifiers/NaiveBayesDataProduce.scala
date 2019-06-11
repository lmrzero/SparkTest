package mllib.classsifiers

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * created by LMR on 2019/6/10
  */
object NaiveBayesDataProduce {

  def main(args: Array[String]): Unit = {

    val random = new Random()
    val numsInstances = 1000
    val numsfeature = 3
    val numsclass = 3
    var data: Array[LabeledPoint] = Array.fill[LabeledPoint](numsInstances)(null)
    for (i <- 0 to numsInstances - 1)
    {
      val array: Array[Double] = Array.fill[Double](numsfeature)(1)
      for (j <- 0 to numsfeature - 1){
        array(j) =  random.nextInt(5)
      }
      val vector = new DenseVector(array)
      val label: Int = random.nextInt(3)
      data(i) = LabeledPoint(label, vector)
    }
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("naiveBayes")
    val sc = new SparkContext(conf)

    val dataRDD: RDD[LabeledPoint] = sc.parallelize(data)
    MLUtils.saveAsLibSVMFile(dataRDD,"E://output")
  }
}
