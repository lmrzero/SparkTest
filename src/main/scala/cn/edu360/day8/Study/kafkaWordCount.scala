package cn.edu360.day8.Study

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaWordCount {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCounts").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val zkQuorum = "master:2181,slave1:2181"
    val groupid = "g12"
    val topic = Map[String, Int]("lin" -> 1)

    //创建DSrtream，需要KafkaDStream

    val data : ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupid, topic)

    //对数据进行处理

    val lines : DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    //这里有区别
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
