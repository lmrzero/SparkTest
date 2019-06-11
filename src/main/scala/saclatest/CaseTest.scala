package saclatest

/**
  * 样例类,使用case 关键字修饰的类, 其重要的特征就是支持模式匹配
  * *
  * * 样例类默认是实现了序列化接口的
  */

case class Message(msg: String){


}

case object CheckHeart{

  var name : String = "*****"//不能封装数据？？？？？？？？？？？
}


object CaseTest {
  def main(args: Array[String]): Unit = {
    println(CheckHeart.name)
  }
}
