package saclatest

abstract class AbstractTest {

  println("this is a constructor---------")

  def sleep()//没有实现的方法

  def eat(food : String) ={

    println(food)
  }
}


class Dog extends  AbstractTest{
  override def sleep(): Unit = {

    println("sleep------------------")
  }
}

object AbstTest{

  def main(args: Array[String]): Unit = {

    val dog = new Dog
    dog.sleep()

    val dog1: Dog with T1 = new Dog with T1 {
      override def say(msg: String): Unit = {
        println(msg)
      }
    }
    dog1.doSome("----***")
  }
}