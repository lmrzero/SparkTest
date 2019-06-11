package spark.streaming

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}

import scala.io.Source


/**
  * created by LMR on 2019/6/4
  */
object SockerServer {

  def main(args: Array[String]): Unit = {

    if (args.length != 3)
    {
      println("the nums of params must be 3: <filename> <port> <time(ms)>")
      System.exit(1)
    }
    val filename = args(0)
    val lines: Array[String] = Source.fromFile(filename).getLines().toArray//从文件中读取数据并转化成为数组
    val filerow = lines.length
    val listener = new ServerSocket(args(1).toInt)

    while (true)
    {
        val socket: Socket = listener.accept()
        new Thread(){
          override def run(): Unit = {
            println("Got client connected from: " + socket.getInetAddress)
            val writer = new PrintWriter(socket.getOutputStream, true)
            while (true){
              Thread.sleep(args(2).toLong)
              val content: String = lines(index(filerow))
              writer.write(content + " \n")
              writer.flush()
            }
            socket.close()
          }
        }.start()
    }
  }


  def index(length : Int): Int ={

    import java.util.Random
    val r = new Random();
    r.nextInt(length)

  }

}
