package implictTest

import ClothesEnum.ClothesEnum

/**
  * 泛型： 就是类型约束
  *
  * List<T>
  */
abstract class Message[C](content: C)

class StrMessage(content: String) extends Message(content)

class IntMessage[Int](content: Int) extends Message[Int](content)

// 定义一个泛型类衣服
class Clothes[A, B, C](val clothType: A, val color: B, val size: C)

// 枚举类
object ClothesEnum extends Enumeration {
    type ClothesEnum = Value
    val 上衣, 内衣, 裤子 = Value

}

object ScalaFanXing {

    def main(args: Array[String]): Unit = {

        val clth1 = new Clothes[ClothesEnum, String, Int](ClothesEnum.上衣, "black", 150)
        println(clth1.clothType)


        val clth2 = new Clothes[ClothesEnum, String, String](ClothesEnum.上衣, "black", "M")
        println(clth2.size)


        val neiYi = new Clothes[ClothesEnum, String, String](ClothesEnum.内衣, "yellow", "X")
        println(neiYi.color)

    }

}
