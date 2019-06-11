package com.lmr.loganalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable
import scala.collection.mutable.{HashMap,Seq}

/**
  * created by LMR on 2019/6/5
  */
object PercentileAna {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ClickStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(1))

    val zkQuorum = "master:2181,slave1:2181"
    val groupId = "g1"
    val topic = Map[String, Int]("cmcc" -> 1)

    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    //过滤是因为数据中可能存在不满足情况的，防止出现异常
    //filter后的Array为(URL, 浏览器类型, 加载时间)
    val filter: DStream[Array[String]] = data.map(line => line._2.split(" ")).filter(_.size == 3)
    //将数据预处理为包含以URL和浏览器为Key，延迟时间为Value的元组
     val map: DStream[(Seq[String], Int)] = filter.map(line => (Seq(line(0), line(1)), line(2).toInt))

    val mapParation: DStream[(Seq[String], HashMap[Int, Int])] = map.mapPartitions(iter => {
      //Map Side聚集汇总结果
      //HashMap is a Map (key -> Map(value, count))
      val resultmap = new HashMap[Seq[String], HashMap[Int, Int]]
      var temp: (Seq[String], Int) = null;
      while (iter.hasNext) {
        temp = iter.next();
        val valueMap: mutable.HashMap[Int, Int] = resultmap.getOrElse(temp._1, new HashMap[Int, Int]())
        val count: Int = valueMap.getOrElse(temp._2, 0)
        valueMap.put(temp._2, count + 1)
        resultmap.put(temp._1, valueMap)
      }
      resultmap.iterator
    })
    val result: DStream[(mutable.Seq[String], mutable.HashMap[Double, Int])] = mapParation.reduceByKeyAndWindow((x: mutable.HashMap[Int, Int], y: mutable.HashMap[Int, Int]) => {
      //Reduce端聚集汇总结果，合并两个hashmap
      y.foreach(r => {
        x.put(r._1, x.getOrElse(r._1, 0) + r._2)
      })
      x
    }, Seconds(30), Seconds(10)).mapPartitions(iter => {

      //计算百分位，结果格式为：Map(key, Map(percentage, value))
      val resultMap = new HashMap[mutable.Seq[String], mutable.HashMap[Double, Int]]()
      while (iter.hasNext) {
        val tmp = iter.next()
        val map = tmp._2
        val sumCount = map.map(r => r._2).reduce(_ + _)//计算总的延迟访问请求
        val p25 = sumCount * 0.25//第25%的位置
        val p50 = sumCount * 0.50
        val p75 = sumCount * 0.75
        val sortDataSeq: scala.Seq[(Int, Int)] = map.toSeq.sortBy(r => r._1)//对数据按照延迟时间进行排序
        val iterS: Iterator[(Int, Int)] = sortDataSeq.iterator
        var curTmpSum = 0.0
        var prevTmpSum = 0.0
        val valueMap = new mutable.HashMap[Double, Int]()//存储结果数据
        while (iterS.hasNext) {
          val tmpData: (Int, Int) = iterS.next()
          prevTmpSum = curTmpSum
          curTmpSum += tmpData._2
          if (prevTmpSum <= p25 && curTmpSum >= p25) {//找到第25%的访问记录，存储其延迟时间
            valueMap.put(0.25, tmpData._1)
          } else if (prevTmpSum <= p50 && curTmpSum >= p50) {
            valueMap.put(0.5, tmpData._1)
          } else if (prevTmpSum < p75 && curTmpSum >= p75) {
            valueMap.put(0.75, tmpData._1)
          }
        }
        resultMap.put(tmp._1, valueMap)
      }
      resultMap.iterator
    })
    result.print()



    ssc.start()
    ssc.awaitTermination()
  }

}
