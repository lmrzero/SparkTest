package actor.test

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory


class ActorServer extends Actor{
  override def receive: Receive = {

    case "start" => println("服务端准备就绪.......")
    case ClientMessage(msg) =>{

      msg match {
        case "你叫啥"  => sender() ! ServerMessage(s"服务端返回消息：林钱洪")
        case "性别" => sender() ! ServerMessage("服务端返回消息：男")
        case "有女朋友吗"  => sender()! ServerMessage("服务端返回消息：有")
        case _ => sender() ! ServerMessage("对不起，无法回答！")
      }
    }
  }
}

object ActorServer extends App{

  val host = "127.0.0.1"
  val port = 8088

  val config = ConfigFactory.parseString(
    s"""
       |akka.actor.provider="akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname=$host
       |akka.remote.netty.tcp.port=$port
        """.stripMargin)

  private val actorSystem = ActorSystem("Server",config)
  private val linActorRef: ActorRef = actorSystem.actorOf(Props[ActorServer],"lin")
  linActorRef ! "start"//发送到自己的receive
}
