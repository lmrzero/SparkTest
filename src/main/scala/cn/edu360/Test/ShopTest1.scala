package cn.edu360.day8

import cn.edu360.Test.{Caculate, MyUtils}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Created by zx on 2017/7/31.
  */
object ShopTest1 {

  def main(args: Array[String]): Unit = {

    //指定组名
    val group = "g001"
    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]")
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Seconds(5))

    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(rules)

    val topic = "shop"
    //指定kafka的broker地址（SparkStream的T
    // ask直接连接到Kafka分区上，用更加底层的方法，效率更高）
    val brokerList = "master:9092,slave1:9092"

    val zkQuorum = "master:2181,slave1:2181"

    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)


    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //如果保存过 offset
    if (children > 0) {
      for (i <- 0 until children) {

        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0
        val tp = TopicAndPartition(topic, i)
        // wordcount/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD(rdd => {

        if(!rdd.isEmpty())
          {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val lines: RDD[String] = rdd.map(_._2)
            val filelds: RDD[Array[String]] = lines.map(_.split(" "))

            Caculate.caculatuIteams(filelds, broadcastRef)

            for (o <- offsetRanges) {
              //  /g001/offsets/wordcount/0
              val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
              //将该 partition 的 offset 保存到 zookeeper
              //  /g001/offsets/wordcount/0/20000
              ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
            }
          }
    })

    ssc.start()
    ssc.awaitTermination()

  }






}
