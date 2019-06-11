package cn.edu360.day8.Study

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulKafkaWordCounts {

  val updatefun = (iter : Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
  }
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("fulKafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //如果需要使用历史数据（累加），那么就需要将中间结果保存起来

    ssc.checkpoint("./ck")

    val zkQuorum = "master:2181,slave1:2181"
    val groupid = "g12"
    val topic = Map[String, Int]("cmcc" -> 1)

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupid, topic)

    val lines : DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
//wordAndOne.updateStateByKey(updatefun, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updatefun, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
