package saclatest

//object 对象不能够new 不能够带参数
object ScalaSingleton {


  def saySomething(msg : String) = {

    print(msg)
  }
}

object test{
  def main(args: Array[String]): Unit = {

    ScalaSingleton.saySomething("王八犊子")
    println(ScalaSingleton)

   // new ScalaSingleton()
  }
}
