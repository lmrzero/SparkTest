package saclatest

class Student {

  var name : String = _
  //val age : Int = _//val修饰的不能够使用占位符，因为其不能被修改
  val age : Int = 9
}


object Test2 {

  def main(args: Array[String]): Unit = {
    val name = "zhangsan"

    val stu = new Student
    stu.name = name

    println(stu.name,stu.age)
  }
}
