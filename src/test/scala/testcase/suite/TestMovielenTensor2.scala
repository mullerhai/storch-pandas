package testcase.suite

//import torch.Device.CPU
//import torch.Tensor
import torch.csv.CSVFormat
import torch.numpy.enums.DType.Float32
import torch.numpy.matrix.NDArray
import torch.pandas.DataFrame
import torch.pandas.component.RatingCSVCompat

object TestMovielenTensor2 {

//  def main(args: Array[String]): Unit = {
//    val header = Seq("userId", "movieId", "rating", "timestamp")
//    val path = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\ratings.dat"
//    val path2 = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\movies.dat"
//    val path3 = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\users.dat"
//    val df = RatingCSVCompat.readCSV(
//      path,
//      Some(header), limit = 500
//    )(using customCSVFormat)
//    df.heads(100).show()
//  }

  def main(args: Array[String]): Unit = {
    given customCSVFormat: CSVFormat = RatingCSVCompat.customCSVFormat
    val header = Seq("userId", "movieId", "rating", "timestamp")
    val ratingPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\ratings.dat"
    val moviePath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\movies.dat"
    val userPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\users.dat"
    val ratingDf = DataFrame.readRatingCSV(ratingPath, 1000, Some(Seq("userId", "movieId", "rating", "timestamp"))) //(using RatingCSVCompat.customCSVFormat)//.drop("id")//,"click")
   //MovieID::Title::Genres
    val movieDf = DataFrame.readRatingCSV(moviePath, -1, Some(Seq("movieId", "title", "genres")))
   //UserID::Gender::Age::Occupation::Zip-code
    val userDf = DataFrame.readRatingCSV(userPath, -1, Some(Seq("userId", "gender","age", "occupation", "zip-code")))
//    ratingDf.show()
//    movieDf.show()
//    userDf.show()
    val userIdSeq =  ratingDf.columnSelect(Seq("userId")).toArray.flatten.distinct //.unique("userId").show()
    val movieIdSeq =  ratingDf.columnSelect(Seq("movieId")).toArray.flatten.distinct

    val userIdMap = userIdSeq.zipWithIndex.map((userId, index) => ( index.toFloat,userId.toString.toInt)).toMap
    val movieIdMap = movieIdSeq.zipWithIndex.map((movieId, index) => ( index.toFloat,movieId.toString.toInt)).toMap
    println(s" user ${userIdSeq.mkString(",")}, movieIdSeq: ${movieIdSeq.length} ${movieIdSeq.mkString(",")} userIdSeq: ${userIdSeq.length}  movie size ${movieIdSeq.length}")

    val castRatingDF  = ratingDf.cast(classOf[Long]).numeric
    castRatingDF.show()
//    System.arraycopy()
    println(s"castRatingDF ${castRatingDF.getShape}, columns ${castRatingDF.getColumns.mkString(",")}")
//    val arr  = castRatingDF.values[Long](isLazy = true)
    val userIdNDArray = castRatingDF.columnSelect(Seq("userId")).values[Long](isLazy = false)
    val movieIdNDArray = castRatingDF.columnSelect(Seq("movieId")).values[Long](isLazy = false)
    val ratingNDArraySeq = castRatingDF.columnSelect(Seq("rating")).values[Long](isLazy = false).trainTestDatasetSplit(0.2,-1,false)
//    val userIdTensor = Tensor[Long](userIdNDArray, false, CPU) //Tensor.fromNDArray(userIdNDArray)
//    val movieIdTensor = Tensor[Long](movieIdNDArray, false, CPU) //Tensor.fromNDArray(movieIdNDArray)
//    val ratingTensor = Tensor[Long](ratingNDArraySeq._1, false, CPU)
//    val userIdTensor = Tensor.createFromNDArray[Long](userIdNDArray,false, CPU) //Tensor.fromNDArray(userIdNDArray)
//    val movieIdTensor = Tensor.createFromNDArray[Long](movieIdNDArray,false, CPU)//Tensor.fromNDArray(movieIdNDArray)
//    val ratingTensor = Tensor.createFromNDArray[Long](ratingNDArray,false, CPU) // Tensor.fromNDArray(ratingNDArray)
//    movieIdTensor.native.print()
//    userIdTensor.native.print()
//    ratingTensor.native.print()
//    ratingTensor(10).native.print()
    //    val numCols = traindf.nonnumeric.getColumns
//    val selectDf = traindf.cast(classOf[Float]).numeric
//    //    selectDf.show()
//    println(s"begin generate numpy ndarray")
//    val ndArray:NDArray[Float] = selectDf.values[Float]().asInstanceOf[NDArray[Float]] //.asDType(Float32)
//    //    ndArray.printArray()
//    println(ndArray.getShape.mkString(","))
//    val mainStartTime = System.nanoTime()
//    println(s"end generate numpy ndarray,try to generate pytorch Tensor ${mainStartTime}")
  }
}
