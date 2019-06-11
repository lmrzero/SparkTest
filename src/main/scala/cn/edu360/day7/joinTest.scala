package cn.edu360.day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object joinTest {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import spark.implicits._
    val lines : Dataset[String]= spark.createDataset(List("1,laozhao,china","2,laoyang,usa","3,laowang,jp"))

    //对数据进行整理
    val tpDs : Dataset[(Long, String, String)]= lines.map(line => {

      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nation = fields(2)
      (id, name, nation)
    })
    val df1 = tpDs.toDF("id","name","nation")

    val nations : Dataset[String]= spark.createDataset(List("china,中国", "usa,美国"))

    val ndataset: Dataset[(String, String)] = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })

    val df2 = ndataset.toDF("ename","cname")

   /* df1.createTempView("v_users")
    df2.createTempView("v_nations")
    val r: DataFrame = spark.sql("SELECT name, cname FROM v_users JOIN v_nations ON nation = ename")

    r.show()*/

    val r = df1.join(df2, $"nation" === $"ename", "left_outer")
    r.show()
    spark.stop()
  }
}
