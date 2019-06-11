package cn.edu360.Test

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

object IPUtils {

  def brocast(ssc :StreamingContext,path : String): Broadcast[Array[(Long, Long, String)]] = {
    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(path)

    //将Drive端的数据广播到Executor中
    val sc: SparkContext = ssc.sparkContext
    //调用sc上的广播方法
    //广播变量的引用（还在Driver端）
     sc.broadcast(rules)
  }

}
