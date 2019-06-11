package implictTest

import java.io.{BufferedReader, File, FileReader}

import MyImpicits._

import scala.io.Source


class RichFile(file: File)  {
    /**
      * 返回文件的记录行数
      * @return
      */
    def count(): Int = {
        val fileReader = new FileReader(file)
        val bufferedReader = new BufferedReader(fileReader)
        var sum = 0
        try {
            var line = bufferedReader.readLine()
            while (line != null) {
                sum += 1
                line = bufferedReader.readLine()
            }
        } catch {
            case _: Exception => sum
        } finally {
            fileReader.close()
            bufferedReader.close()
        }
        sum
    }

}

object ScalaImplicit {



    /**
      * 隐式转换
      *
      *     隐式参数
      *
      *
      *     隐式的类型转换
      *
      *
      *     隐式类
      *
      */

    implicit val content = 1

    def say(implicit content: String = "明天10.1拉") = println(content)

    def add(a: Int)(implicit b: Int) = a + b

    /**
      * 方法的参数如果有多个隐式参数的话，只需要使用一个implicit关键字即可
      * 隐式参数列表必须放在方法的参数列表后面
      */
    def addPlus(a: Int)(implicit b: Int, c: Int) = a + b + c

    /**
      * 定义一个隐式的方法
      */
    implicit def double2Int(double: Double) = {
        println("---double2Int---")
        double.toInt
    }
    implicit val fdouble2Int = (double: Double) => {
        println("---fdouble2Int---")
        double.toInt
    }


    // 隐式类 - 只能在静态对象中使用
    implicit class FileRead(file: File) {

        def read = Source.fromFile(file).mkString

    }




    def main(args: Array[String]): Unit = {

        say("下午好")
        /**
          * say方法的参数是隐式参数，如果你没有传递参数的话，
          * 编译器在编译的时候会自动的从当前的上下文中找一个隐式值（符合参数的类型的隐式值）
          */

         // 编译器在查找隐式值的时候，不能有歧义
        // implicit val msg = "你好帅"
        implicit val msg1 = "你好帅。。。。"
        say
        println(add(5))

        println("===== "+addPlus(5))


        println("-------------隐式类型转换---------")
        // age是一个Int类型，但是赋值的时候却是一个浮点型，此刻编译器会在当前上下文中找一个隐式转换，找一个能把浮点型变成Int的隐式转换
        val age: Int = 20.5
        println(age)

        val file = new File("D:\\log\\2017-09-21\\access.log")

        println("Count = "+file.count())
        println(s"FileContent = ${file.read}")
    }

}
