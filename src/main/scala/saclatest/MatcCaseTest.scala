package saclatest

object MatcCaseTest {


  def listmath(list : Any)={

    list match {

      case x: Int => println("Int")
      case y: Double => println("Double")
      case _ => println("Other")
    }
  }
  def main(args: Array[String]): Unit = {

    val l1 : Int = 5
    val l2 : Double = 6.0
    val l : Float = 4


    //Product1  ????????????
    listmath(l1)
    listmath(l2)
    listmath(l)
  }
}
