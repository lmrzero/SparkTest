package cn.edu360.day6.day6Learn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemp1 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")
    var sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    var lines = sc.textFile(args(0))

    val boyRDD: RDD[Boy] = lines.map(line => {
      val files = line.split(",")
      val id = files(0).toLong
      val name = files(1)
      val age = files(2).toInt
      val fv = files(3).toLong
      Boy(id, name, age, fv)
    })

    import sqlContext.implicits._
    val bdf : DataFrame = boyRDD.toDF

    bdf.registerTempTable("t_boy")

    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy order by fv desc, age asc")

    result.show()
    sc.stop()
  }
}

case class Boy(id : Long, name: String, age : Int, fv : Double)
