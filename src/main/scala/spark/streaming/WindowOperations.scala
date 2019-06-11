package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * created by LMR on 2019/6/4
  */
object WindowOperations {

  private val conf: SparkConf = new SparkConf().setAppName("WindowOperaions").setMaster("local[*]")
  private val ssc = new StreamingContext(conf,Seconds(1))
  private val data: ReceiverInputDStream[String] = ssc.socketTextStream("master",44444)
  def main(args: Array[String]): Unit = {
    reduceByWindowsTest();

  }

  private val unit: DStream[String] = data.flatMap(_.split(" "))

  def reduceByKeyAndWindowTest(): Unit ={
    val map: DStream[(String, Int)] = unit.map(x => (x, 1))
    val value: DStream[(String, Int)] = map.reduceByKeyAndWindow((a : Int, b :Int) => (a + b), Seconds(30), Seconds(10))
    value.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def windowTest(): Unit ={
    val value: DStream[String] = data.window(Seconds(5))
    value.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def reduceByWindowsTest(): Unit ={

  //  unit.reduceByWindow((a : String, b : String) => (a + b), Seconds(30),Seconds(10))
    val unit1: DStream[(String, Long)] = unit.countByValue()//每一秒
    unit1.print()
    ssc.start()
    ssc.awaitTermination()
  }



}
