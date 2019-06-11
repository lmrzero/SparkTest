package actor.test

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

class ActorClient extends Actor{

  var serverActorRef : ActorSelection = _
  override def preStart(): Unit = {
    serverActorRef = context.actorSelection("akka.tcp://Server@127.0.0.1:8088/user/lin")
  }

  override def receive: Receive = {

    case "start" => println("客户端启动了....")
    case msg : String =>{
      serverActorRef ! ClientMessage(msg) // 把客户端输入的内容发送给 服务端（actorRef）--》服务端的mailbox中 -> 服务端的receive
    }
    case ServerMessage(msg) => println(s"收到服务器端回复:${msg}")
  }
}

object ActorClient{

  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = 8090

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
        """.stripMargin)
    val clientSystem = ActorSystem("client", config)

    // 创建dispatch | mailbox
    val actorRef = clientSystem.actorOf(Props[ActorClient], "NMW-002")

    actorRef ! "start" // 自己给自己发送了一条消息 到自己的mailbox => receive

    while (true) {
      val question = StdIn.readLine() // 同步阻塞的， shit
      actorRef ! question // mailbox -> receive
    }
  }


  }
