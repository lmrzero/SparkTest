package saclatest

/**
  * created by LMR on 2019/5/15
  */
object LeftOrRightTest {

  def main(args: Array[String]): Unit = {
    for (s <- Left("flower").left) println(s.length)
  }

}
