package cn.edu360.Test

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisConnect {

  def getConnection(): Jedis={

    val conf :JedisPoolConfig = new JedisPoolConfig()
    conf.setMaxIdle(10)
    conf.setMaxWaitMillis(20000)

    val pool = new JedisPool(conf, "master", 6379)

    val jedis: Jedis = pool.getResource()

    jedis
  }

  def main(args: Array[String]): Unit = {
    val jedis :Jedis = RedisConnect.getConnection()
      val keys: util.Set[String] = jedis.keys("*")

    import scala.collection.JavaConversions._
    for(key <- keys){
      val value = jedis.get(key)

      println(key + "\t" + value)
    }

    jedis.set("key","1","2")
    println(jedis.get("key"))
    jedis.close()
  }
}
