package cn.edu360.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by LMR on 2019/6/2
  */
object RainTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rain")
    val sc = new SparkContext(conf)

    val dataRdd: RDD[String] = sc.textFile("E://f31909.dat")
   /* val mapRdd: RDD[(String, Double)] = dataRdd.map(line => {
      //val fields: Array[String] = line.split(" ")
      val year = line.substring(8,12)
      val rainSize = line.substring(47,52).toDouble
      (year, rainSize)
    })
(1959,461761.4999999999)
(1972,423801.49999999994)
(1974,420151.4999999998)
(2000,419750.00000000006)
(1968,419092.9999999998)
(1984,418764.5)
(1971,385257.5)
(1943,384089.5)
(1956,377519.50000000006)

    val reduceRdd: RDD[(String, Double)] = mapRdd.reduceByKey(_+_)
    val sortRdd: RDD[(String, Double)] = reduceRdd.sortBy(_._2,false)
    val result: Array[(String, Double)] = sortRdd.take(10)

    for (elem <- result) {
      println(elem)
    }*/


    val fields: RDD[Array[String]] = dataRdd.map(line => {
      line.trim.replace("   ", " ").replace("  ", " ").split(" ")
    }).filter(_.length == 15).filter(_ (13) != "9").filter(_ (11) != "999.9")
    val mapRDD: RDD[(String, Double)] = fields.map(fields => (fields(1), fields(11).toDouble))
    val reduceRDD: RDD[(String, Double)] = mapRDD.reduceByKey(_+_)
    val mulReduceRdd: RDD[(String, Double)] = reduceRDD.map(x => (x._1, x._2*365))
    val sortRDD: RDD[(String, Double)] = mulReduceRdd.sortBy(_._2,false)
    val re: Array[(String, Double)] = sortRDD.collect()
   /* val pre_ann: RDD[(String, Double)] = fields.map(fields => (fields(1),fields(11).toDouble)).groupByKey().map(x => (x._1, x._2.reduce(_+_)))

    val re: Array[(String, Double)] = pre_ann.map(x => (x._2*365, x._1)).sortByKey(false).map(x => (x._2, x._1)).collect()
*/
    for (elem <- re) {
      println(elem)
    }
  }

}
