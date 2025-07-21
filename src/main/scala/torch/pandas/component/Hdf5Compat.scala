package torch
package pandas.component

import java.nio.file.Paths
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._

import org.apache.commons.lang3.ArrayUtils

import io.jhdf.HdfFile
import io.jhdf.api.Dataset
object Hdf5Compat {

  def main(args: Array[String]): Unit =
    try {
      val hdfFile = new HdfFile(
        Paths.get("D:\\data\\git\\testNumpy\\src\\main\\resources\\example.h5"),
      )
      try {
        val fileIter = hdfFile.iterator().asScala
        while (fileIter.hasNext) {
          val node = fileIter.next()
          System.out.println(node.getName)
          println(node.toString)
          println(node.getType.toString)
          println(node.isGroup)
          println(node.isAttributeCreationOrderTracked)
          println(node.isLink)
          println(node.getAttributes)
        }
        val dataset = hdfFile.getDatasetByPath("data_6")
        // data will be a java array of the dimensions of the HDF5 dataset
        val data = dataset.getData
        println(data)
        println(ArrayUtils.toString(data)) // NOSONAR - sout in example
      } finally if (hdfFile != null) hdfFile.close()
    }

  def writeHdf5(
      outFile: String,
      datasetName: String,
      data: Array[AnyRef],
  ): Unit =
    try {
      val hdfFile = HdfFile.write(Paths.get(outFile))
      try hdfFile.putDataset(datasetName, data)
//        hdfFile.putDataset("doubles", Array[Double](1.0, 2.0, 3.0, 4.0))
//        val multiDimGroup = hdfFile.putGroup("multiDim")
//        multiDimGroup.putDataset("2d-ints", Array[Array[Int]](Array(1, 2), Array(3, 4)))
//        multiDimGroup.putDataset("3d-ints", Array[Array[Array[Int]]](Array(Array(1, 2), Array(3, 4)), Array(Array(5, 6), Array(7, 8))))
      finally if (hdfFile != null) hdfFile.close()
    }

  def readHdf5(hdf5Path: String, datasetName: String): Array[AnyRef] = {
    val hdfFile = new HdfFile(Paths.get(hdf5Path))
    try {
      val dataset = hdfFile.getDatasetByPath(datasetName)
      // data will be a java array of the dimensions of the HDF5 dataset
      println(s"dataset.getDataType ${dataset.getDataType} data dim ${dataset
          .getDimensions.mkString(",")} javaType ${dataset
          .getJavaType} size ${dataset.getSize}")
      val data = dataset.getData
      var realData: Array[AnyRef] = null
      realData = data match {
        //                case arr: Array[Byte] => arr.map(_.toByte)
        //                case arr: Array[Short] => arr.asInstanceOf[Array[Short]] // arr.map(_.toShort)
        //                case arr: Array[Int] => arr.asInstanceOf[Array[Int]] //arr.map(_.toInt)
        //                case arr: Array[Long] => arr.asInstanceOf[Array[Long]] //arr.map(_.toLong)
        //                case arr: Array[Float] => arr.asInstanceOf[Array[Float]] // arr
        //                case arr: Array[Double] => arr.asInstanceOf[Array[Double]]//.map(_.toDouble)
        case arr: Array[Array[Byte]] => arr.map(_.map(_.toByte))
        case arr: Array[Array[Short]] => arr.map(_.map(_.toShort))
        case arr: Array[Array[Int]] => arr.map(_.map(_.toInt))
        case arr: Array[Array[Long]] => arr.map(_.map(_.toLong))
        case arr: Array[Array[Float]] => arr.map(_.map(_.toFloat))
        case arr: Array[Array[Double]] => arr.map(_.map(_.toDouble))
        case arr: Array[AnyRef] => arr
        case _ => throw new RuntimeException(
            s"dataset $datasetName data type not support",
          )
      }
      println(ArrayUtils.toString(data))
      realData
    } finally if (hdfFile != null) hdfFile.close()
  }
//  def readHdf5Old(hdf5Path: String,datasetName:String): Array[AnyRef] = {
//    val hdfFile = new HdfFile(Paths.get(hdf5Path))
//    try {
//      val dataset = hdfFile.getDatasetByPath(datasetName)
//      // data will be a java array of the dimensions of the HDF5 dataset
//      println(s"dataset.getDataType ${dataset.getDataType} data dim ${dataset.getDimensions.mkString(",")} javaType ${dataset.getJavaType} size ${dataset.getSize}")
//      val data = dataset.getData
//      data match{
////        case arr: Array[Byte] => arr.map(_.toByte)
////        case arr: Array[Short] => arr.map(_.toShort)
////        case arr: Array[Int] => arr.map(_.toInt)
////        case arr: Array[Long] => arr.map(_.toLong)
////        case arr: Array[Float] => arr
////        case arr: Array[Double] => arr.map(_.toDouble)
//        case arr: Array[Array[Byte]] => arr.map(_.map(_.toByte))
//        case arr: Array[Array[Short]] => arr.map(_.map(_.toShort))
//        case arr: Array[Array[Int]] => arr.map(_.map(_.toInt))
//        case arr: Array[Array[Long]] => arr.map(_.map(_.toLong))
//        case arr: Array[Array[Float]] => arr.map(_.map(_.toFloat))
//        case arr: Array[Array[Double]] => arr.map(_.map(_.toDouble))
//        case arr: Array[AnyRef] => arr
//        case _ => throw new RuntimeException(s"dataset $datasetName data type not support")
//
//      }
//      data
//    } finally if (hdfFile != null) hdfFile.close()
//  }
}
