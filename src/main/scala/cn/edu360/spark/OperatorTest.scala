package cn.edu360.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by LMR on 2019/5/31
  */
object OperatorTest {

  val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")
  //创建spark执行的入口
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    takeAndtop()

  }

  def takeAndtop(): Unit ={

    val dataRDD: RDD[(Int, Int)] = sc.makeRDD(Array((1,25),(2,24),(3,23),(4,22)))
    val takeResult: Array[(Int, Int)] = dataRDD.take(2)
    val topResult: Array[(Int, Int)] = dataRDD.top(2)

    println("---------------------------take--------------------")
    for (elem <- takeResult) {
      println(elem)
    }

    println("---------------------------top----------------------")
    for (elem <- topResult) {
      println(elem)
    }
  }

  def aggregateAndFold(): Unit ={

      val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    //输出每个数的分区
    val tuples: Array[(String, List[Int])] = rdd.mapPartitionsWithIndex((Index, iter) => {
      var part_map = scala.collection.mutable.Map[String, List[Int]]()
      while (iter.hasNext) {
        var part_name = "part_" + Index
        var elem = iter.next();
        if (part_map.contains(part_name)) {
          var elems = part_map(part_name)
          elems ::= elem
          part_map(part_name) = elems
        } else {
          part_map(part_name) = List[Int] {
            elem
          }
        }
      }
      part_map.iterator
    }).collect()
    for (elem <- tuples) {
      println(elem._1,"-->",elem._2)
    }

    val i: Int = rdd.aggregate(1)(
      { (x: Int, y: Int) => x + y },
      { (a: Int, b: Int) => a * b }
    )
    println(i)


    val j: Int = rdd.fold(1)(
      { (x: Int, y: Int) => x + y }
    )
    println(j)
  }

  def combineByKeyTest(): Unit ={

    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")

    var rdd = sc.makeRDD(Array(("A",2),("A",1),("A",3),("B",1),("B",2),("C",1)))

    val tuples: Array[(String, List[(String, Int)])] = rdd.mapPartitionsWithIndex((Index, iter) => {
      var part_map = scala.collection.mutable.Map[String, List[(String, Int)]]()

      while (iter.hasNext) {
        var part_name = "part_" + Index
        var elem = iter.next();
        if (part_map.contains(part_name)) {
          var elems = part_map(part_name)
          elems ::= elem
          part_map(part_name) = elems
        } else {
          part_map(part_name) = List[(String, Int)] {
            elem
          }
        }
      }
      part_map.iterator
    }).collect()
    for (elem <- tuples) {
      println(elem._1,"-->",elem._2)
    }

    /**
      * (part_0,-->,List((A,2)))
      * (part_1,-->,List((A,3), (A,1)))
      * (part_2,-->,List((B,1)))
      * (part_3,-->,List((C,1), (B,2)))
      *
      * */
    val collect: Array[(String, String)] = rdd.combineByKey(
      (v: Int) => v + "_",
      (c: String, v: Int) => c + "@" + v,//同一分区内
      (c1: String, c2: String) => c1 + "$" + c2
    ).collect

    for (elem <- collect) {
      println(elem._1,"--",elem._2)
    }

  }

}
