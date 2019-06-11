package cn.edu360.day8.Study

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    //指定组名
    val groupid = "g1"
    //创建SparkConf
    val conf = new SparkConf().setAppName("SparkConfWordCount").setMaster("local[2]")
    //创建SparkStreaming，并设置间隔时间

    val ssc = new StreamingContext(conf, Duration(5))

    val topic = "lin"
    //指定kafka的broker地址（SparkStream的Task直接连接到Kafka分区上，用更加底层的方法，效率更高）
    val brokerList = "master:9092,slave1:9092"

    val zkQuorum = "master:2181,slave1:2181"
    //创建stream时使用的topic名字集合，SparkStreaming可同时消费多个topic
    val topics : Set[String] = Set(topic)

    val topicDirs = new ZKGroupTopicDirs(groupid, topic) //只用一个同批次？？？？
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupid,
      //从头开始读数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    //zookeeper的host和ip，创建一个客户端，用于更新偏移量
    //
    val zkClient = new ZkClient(zkQuorum)

    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/
    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream : InputDStream[(String, String)] = null

    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if(children > 0)
    {
        for(i <- 0 until children){

          val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")

          val tp = TopicAndPartition(topic, i)

          fromOffsets += (tp -> partitionOffset.toLong)
        }

     val messageHandler =  (mmd : MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }else{

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    var offsetRanges = Array[OffsetRange]()

    //从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    //该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream
    val transform: DStream[(String, String)] = kafkaStream.transform { rdd =>
      //得到该 rdd 对应 kafka 的消息的 offset
      //该RDD是一个KafkaRDD，可以获得偏移量的范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val messages: DStream[String] = transform.map(_._2)

    //依次迭代DStream中的RDD
    messages.foreachRDD { rdd =>
      //对RDD进行操作，触发Action
      rdd.foreachPartition(partition =>
        partition.foreach(x => {
          println(x)
        })
      )

      for (o <- offsetRanges) {
        //  /g001/offsets/wordcount/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        //将该 partition 的 offset 保存到 zookeeper
        //  /g001/offsets/wordcount/0/20000
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
