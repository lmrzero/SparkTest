package cn.edu360.day8.Study

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {


  def main(args: Array[String]): Unit = {
    //离线任务是创建SparkContext，要实现在线任务必须使用StreamingContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Milliseconds(5000))
  //y有了StreamingContext就可以船舰Spark Streaming抽象了DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("92.168.177.11",8888)


    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne : DStream[(String, Int)] = words.map((_,1))

    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    reduced.print()

    ssc.start()

    ssc.awaitTermination()


  }
}
