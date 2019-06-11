package saclatest

class Student3 private (val name :String, var age: Int){
  // new Student3("lin",27)主构造器用private修饰之后不能够使用主构造器来创建实例
  var gender : String = _

  def this(name: String, age :Int, gender : String){
    this(name, age)
    this.gender = gender
  }
  // private[this]关键字标识该属性只能在类的内部访问, 伴生类不能访问
  private[this] val province : String = "BJ"
  /*
  * private[包名] class 放在类声明最前面, 是修饰类的访问权限, 也就是说类在某些
  包下不可见或不能访问
  *
  * private[sheep] class 代表student4 在sheep 包下及其子包下可以见, 同级包
  中不能访问
  *
  * */
}

/*object Test4{
  def main(args: Array[String]): Unit = {

    val student = new Student3("lin",28,"man")
    println(s"${student.gender}")
  }
}*/
/**
  *
  * 在Scala 中, 当单例对象与某个类共享同一个名称时，他被称作是这个类的伴生对象。必须
  * 在同一个源文件里定义类和它的伴生对象。类被称为是这个单例对象的伴生类。类和它的伴
  * 生对象可以互相访问其私有成员。*/
object Student3{//类的伴生对象
  def main(args: Array[String]): Unit = {

    val student = new Student3("lin",29)//伴生对象可以访问私有主构造器
  }
}
