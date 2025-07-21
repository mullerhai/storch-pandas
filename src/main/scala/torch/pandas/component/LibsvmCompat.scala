package torch.pandas.component

import scala.collection.mutable.ListBuffer
import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import breeze.linalg.SparseVector
import torch.pandas.DataFrame

case class LibSVMData(label: Double, features: Map[Int, Double])

object LibsvmCompat {

  def main(args: Array[String]): Unit = {
    val filePath = "resources\\aclImdb\\train\\labeledBow.feat"
    val (labels, sparseVectors) = libSVMToSparseVectors(filePath)
    println(s"Labels: $labels")
    println(s"First sparse vector: ${sparseVectors.head}")
  }

  def mains(args: Array[String]): Unit = {
    val filePath = "src\\main\\resources\\aclImdb\\train\\labeledBow.feat"
    val data = readLibSVMFile(filePath)
    data.foreach { case LibSVMData(label, features) =>
      println(s"Label: $label, Features: $features")
    }
  }

  def libSVMToSparseVectors(
      filePath: String,
  ): (List[Double], List[SparseVector[Double]]) = {
    val lines = Source.fromFile(filePath).getLines().toList
    try {
      // 存储所有样本的标签
      val labels = ListBuffer[Double]()
      // 存储所有样本的稀疏向量
      val sparseVectors = ListBuffer[SparseVector[Double]]()

      lines.foreach { line =>
        val parts = line.split("\\s+", 2)
        if (parts.length == 2)
          try {
            // 提取标签
            val label = parts(0).toDouble
            labels += label

            // 提取特征索引 - 值对
            val featurePairs = parts(1).split("\\s+")
            val indices = ListBuffer[Int]()
            val values = ListBuffer[Double]()
            var maxIndex = 0

            featurePairs.foreach { pair =>
              val indexValue = pair.split(":", 2)
              if (indexValue.length == 2)
                try {
                  val index = indexValue(0).toInt
                  val value = indexValue(1).toDouble
                  indices += index // - 1 // LibSVM 索引从 1 开始，Breeze 从 0 开始
                  values += value
                  maxIndex = math.max(maxIndex, index)
                } catch {
                  case _: NumberFormatException => // 忽略解析失败的特征
                }
            }

            // 创建稀疏向量
            val sparseVector = SparseVector.zeros[Double](maxIndex + 1)
            indices.zip(values).foreach { case (idx, value) =>
//              println(s"index $idx, value $value")
              sparseVector(idx) = value
            }
            sparseVectors += sparseVector
          } catch {
            case _: NumberFormatException => // 忽略解析失败的行
          }
      }

      (labels.toList, sparseVectors.toList)
    } finally Source.fromFile(filePath).close()
  }

  def readLibSVMFile(filePath: String): Seq[LibSVMData] = Source
    .fromFile(filePath).getLines().flatMap { line =>
      line.split("\\s+").toList match {
        case labelStr :: featurePairs =>
          try {
            val label = labelStr.toDouble
            val features = featurePairs.flatMap(pair =>
              pair.split(':') match {
                case Array(indexStr, valueStr) =>
                  try {
                    val index = indexStr.toInt
                    val value = valueStr.toDouble
                    Some(index -> value)
                  } catch { case _: NumberFormatException => None }
                case _ => None
              },
            ).toMap
            Some(LibSVMData(label, features))
          } catch { case _: NumberFormatException => None }
        case _ => None
      }
    }.toSeq
//  def readLibsvm(file: String): DataFrame[AnyRef] = Serialization.readLibsvm(file)
//
//  def writeLibsvm(df: DataFrame[AnyRef], file: String): Unit = Serialization.writeLibsvm(df, file)
}
