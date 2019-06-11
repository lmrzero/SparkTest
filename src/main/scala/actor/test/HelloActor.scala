package actor.test

import akka.actor.{Actor, ActorRef, ActorSystem, Props}


class HelloActor extends Actor{
  override def receive: Receive = {

    case "你好帅" => println("说的对")
    case "丑" => println("滚犊子")
    case "stop" =>{
      println()
    }
  }
}
object HelloActor {

    private val helloActorSystem = ActorSystem("Hello")

    private val helloActorRef: ActorRef = helloActorSystem.actorOf(Props[HelloActor],"helloActor")

    def main(args: Array[String]): Unit = {

     // AkkaUtils
      helloActorRef ! "你好帅"
     }

}
