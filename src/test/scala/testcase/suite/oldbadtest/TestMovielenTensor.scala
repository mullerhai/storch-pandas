package testcase.suite.oldbadtest

import torch.csv.CSVFormat
import torch.numpy.enums.DType.Float32
import torch.pandas.DataFrame
import torch.numpy.matrix.NDArray
import torch.pandas.component.RatingCSVCompat

object TestMovielenTensor {

  def main(args: Array[String]): Unit = {
    given customCSVFormat: CSVFormat = RatingCSVCompat.customCSVFormat
    val etledPath = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\ml-1m\\ratings.dat"
    val traindf = DataFrame.readRatingCSV(etledPath, -1, Some(Seq("userId", "movieId", "rating", "timestamp"))) //(using RatingCSVCompat.customCSVFormat)//.drop("id")//,"click")
    traindf.show()
    val numCols = traindf.nonnumeric.getColumns
    val selectDf = traindf.cast(classOf[Float]).numeric
    //    selectDf.show()
    println(s"begin generate numpy ndarray")
    val ndArray:NDArray[Float] = selectDf.values[Float]().asInstanceOf[NDArray[Float]] //.asDType(Float32)
    //    ndArray.printArray()
    println(ndArray.getShape.mkString(","))
    val mainStartTime = System.nanoTime()
    println(s"end generate numpy ndarray,try to generate pytorch Tensor ${mainStartTime}")
  }
}
