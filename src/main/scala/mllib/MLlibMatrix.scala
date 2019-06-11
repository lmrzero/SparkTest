package mllib

import breeze.linalg.{DenseMatrix, DenseVector, Transpose, accumulate, diag}
import org.apache.log4j.{Level, Logger}

/**
  * created by LMR on 2019/6/6
  */
object MLlibMatrix {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
  //breeze
    val m1: DenseMatrix[Double] = DenseMatrix.zeros[Double](2,3)
    println(m1)
    println()
    val zerosVec: DenseVector[Double] = DenseVector.zeros[Double](3)
    println(zerosVec)
    println()
    val onesVec: DenseVector[Double] = DenseVector.ones[Double](3)
    println(onesVec)
    println()
    //按照数值填充向量
    val fillVec: DenseVector[Double] = DenseVector.fill[Double](3){5.0}
    println("fill:5.0->",fillVec)
    println()
    val rangeVec: DenseVector[Int] = DenseVector.range(1, 10, 2)
    println(rangeVec)
     println()
    //eye表示单位矩阵
    val eyeMatrix: DenseMatrix[Double] = DenseMatrix.eye[Double](3)
    println(eyeMatrix)
    println()

    //对角矩阵
    val diaMatrix: DenseMatrix[Double] = diag(DenseVector(1.0, 2.0,3.0))
    println(diaMatrix)
    println()

    val matrix: DenseMatrix[Double] = DenseMatrix((1.0,2.0),(3.0,4.0))
    println(matrix)
    println()

    //向量转置
    val trans: Transpose[DenseVector[(Int, Int, Int, Int)]] = DenseVector((1,2,3,4)).t
    println(trans)
    println()

    val result: DenseVector[Int] = accumulate(DenseVector(1,2,3,4))
    println(result)

  }

}
