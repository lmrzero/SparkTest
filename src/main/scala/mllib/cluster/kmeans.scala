package mllib.cluster

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}

/**
  * created by LMR on 2019/6/10
  */
object kmeans {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("svm")
    val sc = new SparkContext(conf)

    //从windows本地读取数据，转化为RDD[Vector]
    val source: BufferedSource = Source.fromFile("E:\\IDEAWorkPlace\\SparkTest\\src\\main\\scala\\mllib\\data\\kmeans_data.txt")
    val lines: Array[String] = source.getLines().toArray
    val vectors: Array[linalg.Vector] = lines.map { line =>
      val splits: Array[Double] = line.split(" ").map(_.toDouble)
      Vectors.dense(splits)

    }

    val data: RDD[linalg.Vector] = sc.parallelize(vectors)

    val rdd1: RDD[String] = sc.parallelize(lines)
    rdd1.map(_.split(" ")).map(f => f)
    //创建模型并训练
    val initMode = "k-means||"//可以选择random和k-means++（使用k-means||）
    val numsClusters = 2
    val numIteerations = 20
    val model: KMeansModel = new KMeans()
      .setInitializationMode(KMeans.K_MEANS_PARALLEL)
      .setK(numsClusters)
      .setMaxIterations(numIteerations)
      .run(data)

    //计算误差
    val WSSSE: Double = model.computeCost(data)
    println(WSSSE)
  }

  /*

  create table temp1 as
   select fatherid from productsort where fatherid != null

  create table temp2 as
    select fatherid  from productsort where id in
  {
     select * from temp1
  } and fatherid != null

  create table temp3 as
      select wPid, Name from Ware where wPid in {select * from temp2}

  select Name, count(*) count from temp3 group by wPid



  *
  */


}
