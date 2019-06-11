package com.lmr.loganalysis

/**
  * created by LMR on 2019/6/5
  */
class PView(val site: String, val vistor : String, val pageurl :String) extends Serializable {

  def parseData(content : String): PView ={

    val fields: Array[String] = content.split(" ")
    new PView(fields(2), fields(1), fields(0))
  }


}
