package com.lmr.loganalysis

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
/**
  * created by LMR on 2019/6/5
  */
object ClickStreamAna {

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
    val filter: DStream[Array[String]] = data.map(line => line._2.split(" ")).filter(_.size == 3)

     val map: DStream[(String, String, String)] = filter.map(line => (line(0), line(1), line(2)))
    //统计每个批次指定时间段数据PV
    val pagecounts: DStream[(String, Long)] = map.map(view => view._1).countByValue()//统计页面出现次数
    pagecounts.print()
    //统计过去15秒的访客数量，每次间隔2秒访问一次
    val window = Seconds(15)
    val interval = Seconds(2)
    val visitorcounts: DStream[(String, Int)] = map.window(window, interval).map(view => (view._2, 1)).groupByKey().map(v => (v._1, v._2.size))
    visitorcounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
