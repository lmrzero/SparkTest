/**
  * created by LMR on 2019/6/2
  */
object test {

  def main(args: Array[String]): Unit = {
    val str = " 31909  1923   1   1 9 999.9 9 999.9 9 999.9 9 999.9 9 9 9999";

    val strings: Array[String] = str.split(" ")
   // println(strings.size)
    var i = 0
    for (elem <- strings) {
      println(elem," ",i)
      i += 1
    }
  }

}
