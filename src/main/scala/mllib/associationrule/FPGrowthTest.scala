package mllib.associationrule

import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}

/**
  * created by LMR on 2019/6/11
  */
object FPGrowthTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("svm")
    val sc = new SparkContext(conf)

    //从windows本地读取数据，转化为RDD[Vector]
    val source: BufferedSource = Source.fromFile("E:\\IDEAWorkPlace\\SparkTest\\src\\main\\scala\\mllib\\data\\sample_fpgrowth.txt")
    val lines: Array[String] = source.getLines().toArray

    val data: RDD[String] = sc.parallelize(lines)
    val examples: RDD[Array[String]] = data.map(_.split(" ")).cache()


    val miniSupport = 0.2
    val numPartition = 10
    val model: FPGrowthModel[String] = new FPGrowth()
      .setMinSupport(miniSupport)
      .setNumPartitions(numPartition)
      .run(examples)


    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    val array: Array[FPGrowth.FreqItemset[String]] = model.freqItemsets.collect()
    model.freqItemsets.collect().foreach(itemset =>
    println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq))
  }

}
