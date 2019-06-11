package mllib

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
/**
  * created by LMR on 2019/6/9
  */
object DistributeMatrix {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Matrix")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Array[Double]] = sc.parallelize(Array(Array(1.0, 2.0,3.0,4.0), Array(3.0, 4.0, 5.0, 6.0)))

    val vector: RDD[linalg.Vector] = rdd1.map(f => Vectors.dense(f))
    val rowMatrix = new RowMatrix(vector)//行矩阵
    //相似度 The threshold parameter is a trade-off knob between estimate quality and computational cost.
    //采用抽样得方法
    val simic1: CoordinateMatrix = rowMatrix.columnSimilarities(0.5)
    //不抽样
    rowMatrix.columnSimilarities()//

    //每列汇总
    val summary: MultivariateStatisticalSummary = rowMatrix.computeColumnSummaryStatistics()
    println(summary.normL1,"\t",summary.normL2,"\t",summary.mean)

    //每列之间得协方差
    val covmatrix: Matrix = rowMatrix.computeCovariance()
    println(covmatrix)

    //计算格拉姆矩阵AT*A
    val gramMatrix: Matrix = rowMatrix.computeGramianMatrix()
    println(gramMatrix)

    //主成分分析选取前k个
    val prinMatrix: Matrix = rowMatrix.computePrincipalComponents(3)
    println(prinMatrix)

    //奇异值分解
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rowMatrix.computeSVD(4,true)
    val u: RowMatrix = svd.U
    val s: linalg.Vector = svd.s
    val v: Matrix = svd.V



    //矩阵乘法
    val mulRes: RowMatrix = rowMatrix.multiply(new DenseMatrix(4,2,Array(1,2,1,2,1,2,1,2)))
    //println(mulRes)
    val vectors: Array[linalg.Vector] = mulRes.rows.collect()
    for (elem <- vectors) {
      println(elem)
    }

    //行索引矩阵
    val i: Accumulator[Int] = sc.accumulator(0)
    val indexRows: RDD[IndexedRow] = vector.map(f => {//始终i为1，因为无法访问到外面得值，可以用累加变量
      i.add(1)//Exector无法读取累加变量
      new IndexedRow(i.value, f)}
      )
    val indexedRowMatrix = new IndexedRowMatrix(indexRows,2,4)
    val rows: RDD[IndexedRow] = indexedRowMatrix.rows
    val array: Array[IndexedRow] = rows.collect()
    for (elem <- array) {
      println(elem)
    }
    //计算格拉姆矩阵、SVD分解和乘法与RowMatrix相同

    //转化为BlockMatrix矩阵
    val blockmatrix: BlockMatrix = indexedRowMatrix.toBlockMatrix(2,2)
    //转化为坐标矩阵
    val corrdmatrix: CoordinateMatrix = indexedRowMatrix.toCoordinateMatrix()
    //转化为行矩阵
    val rowMatrix1: RowMatrix = indexedRowMatrix.toRowMatrix()

    //坐标矩阵

    //分块矩阵
  }

}
