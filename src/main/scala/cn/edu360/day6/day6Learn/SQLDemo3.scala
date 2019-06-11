package cn.edu360.day6.day6Learn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object SQLDemo3 {

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

        val df1: DataFrame = bdf.select("name","age","fv")

        import sqlContext.implicits._

        val df2: Dataset[Row] = df1.orderBy($"fv" desc, $"age" asc)

        df2.show()
        sc.stop()

        sc.stop()
  }
}
