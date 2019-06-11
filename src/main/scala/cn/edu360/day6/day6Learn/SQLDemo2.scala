package cn.edu360.day6.day6Learn

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, types}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo2 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLDemo2").setMaster("local")
     val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile(args(0))

    val rowRDD = lines.map(line => {
      val fileds = line.split(",")
      val id = fileds(0).toLong
      val name = fileds(1)
      val age = fileds(2).toInt
      val fv = fileds(3).toDouble

      Row(id, name, age, fv)
    })

    val sch = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))


    val bdf = sqlContext.createDataFrame(rowRDD, sch)

    bdf.registerTempTable("t_boy")

    val result = sqlContext.sql("select * from t_boy order by fv desc, age asc")

    result.show()

    sc.stop()

  }
}
