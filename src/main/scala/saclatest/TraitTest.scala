package saclatest

trait T1{
  val classname: String = "Lin"

  def say(msg :String)//定义一个没有实现的方法

  def doSome(move : String) ={
    println(move)
  }
}

class Tl extends T1{
  override def say(msg: String): Unit = {
    println(msg)
  }
}

class DT{

  def eat(food : String)={
    println(food)
  }
  def doSome(msg : String)={//会被报错
    println("DT")
  }
}
object TraitTest{
  def main(args: Array[String]): Unit = {

    val t = new Tl()
    t.doSome("play")

    val dt: DT with T1 = new DT with T1 {
      override def doSome(move: String): Unit = {//如果不重写该方法会报错
        println(move)
      }

      override def say(msg: String): Unit = {
        println("hahhaahhahh")
      }
    }
    dt.say("")
    dt.doSome("****")
  }
}
