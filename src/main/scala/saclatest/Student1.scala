package saclatest

class Student1 (val name : String, var age : Int){//类旁边定义的是主构造器

  var gender : String = _

  def this(name : String, age : Int, gender : String){//辅助构造器
    //前面不能够存在语句
    this(name,age)//辅助构造器里面必须调用主构造器
    this.gender = gender
  }
}

object Test3 {

  def main(args: Array[String]): Unit = {
    val student = new Student1("lin",22)
    println(s"name: ${student.name}-------age:${student.age}")//主构造器里面的参数必须有修饰，没有修饰不能够被访问
  }
}
