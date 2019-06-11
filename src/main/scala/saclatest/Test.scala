package saclatest

object Test1 {

  def main(args: Array[String]): Unit = {
    val name = "zhangsan"

    val stu = new Student
    stu.name = name

    println(stu.name,stu.age)
  }
}
