package cn.edu360.Test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object Caculate {

  def caculatuIteams(fields : RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]) ={

    val provinceAndValue: RDD[(String, Double)] = fields.map(f => {
      val ip = MyUtils.ip2Long(f(1))
      val value = f(4).toDouble
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找
      var province = "未知"
      val index = MyUtils.binarySearch(rulesInExecutor, ip)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, value)
    })
    val reduced: RDD[(String, Double)] = provinceAndValue.reduceByKey(_+_)

    reduced.foreach(f => {
      val jedis: Jedis = RedisConnect.getConnection()

      jedis.incrByFloat(f._1,f._2)
    })
  }

}
