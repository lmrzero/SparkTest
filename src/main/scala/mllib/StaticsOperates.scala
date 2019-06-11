package mllib


import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.test.ChiSqTestResult
/**
  * created by LMR on 2019/6/6
  */
object StaticsOperates {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SpparkMLlib").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val array = Array("1 2 3 4", "5 6 7 8", "10 11 12 13", "14 15 16 17")
  //  val data: RDD[String] = sc.textFile("data.txt")
    val data: RDD[String] = sc.parallelize(array)
    val mapdata: RDD[Array[Double]] = data.map(_.split(" ")).map(f => f.map(f => f.toDouble))
    val data1: RDD[linalg.Vector] = mapdata.map(f => Vectors.dense(f))
    val result: Array[linalg.Vector] = data1.collect()
    println("data->",data1)
    val stat1: MultivariateStatisticalSummary = Statistics.colStats(data1)
    println("max->",stat1.max)
    println("min->",stat1.min)
    println("mean->",stat1.mean)
    println("variance->",stat1.variance)
    println("norm1->",stat1.normL1)
    println("norml2",stat1.normL2)

    println("-------------------------------------x相关系数------------------")
    val pearsonCorr: Matrix = Statistics.corr(data1, "pearson")
    val spearmanCorr: Matrix = Statistics.corr(data1, "spearman")

    println("Pearson系数：\n",pearsonCorr)
    println("SpearMan系数：\n",spearmanCorr)

    val x1: RDD[Double] = sc.parallelize(Array(1, 2, 3, 4))//必须为Double类型，否则，编译不通过
    val x2: RDD[Double] = sc.parallelize(Array(5, 6, 7, 8))
    val corrs: Double = Statistics.corr(x1, x2, "pearson")
    println("两个向量之间的Pearson相关系数：",corrs)

    println("---------------------------假设检验----------------------------")
    val v1: linalg.Vector = Vectors.dense(43.0, 9.0)
    val v2: linalg.Vector = Vectors.dense(44.0, 4.0)
    val c1: ChiSqTestResult = Statistics.chiSqTest(v1, v2)
    println(c1)

  }

}
