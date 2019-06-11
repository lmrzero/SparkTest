package saclatest

object ApplyTest {

  def apply() = {
    println("--------------")
  }
  def apply(name: String)={
    println(name)
  }
  def main(args: Array[String]): Unit = {

    val apply = ApplyTest("sss")//带（）时会带哦用apply方法
  }
}
