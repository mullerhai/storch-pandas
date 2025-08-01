package testcase.suite.convertion

//import torch.Device.CPU
//import torch.Tensor
import torch.numpy.enums.DType.Float32
import torch.pandas.DataFrame
import torch.numpy.matrix.NDArray

object testAvazuTensor {

  def main(args: Array[String]): Unit ={
    val etledPath = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\combined_df.csv"
    val traindf = DataFrame.readCsv(etledPath, 3200000, needConvert = false)//,"click")
    val numCols = traindf.nonnumeric.getColumns
    val triggerBatch = 1000000
    traindf.splitChunkIter(triggerBatch).foreach(df => {
      println(s"try to cast to float for traindf ... numCols ${numCols.mkString(",")}")
      val selectDf = df.drop("id").cast(classOf[Float]).numeric
      //    selectDf.show()
      println(s"begin generate numpy ndarray ")
      val ndArray: NDArray[Float] = selectDf.values[Float](isLazy = true, triggerBatch = triggerBatch).asInstanceOf[NDArray[Float]] //.asDType(Float32)

//      ndArray.printArray()
      println(ndArray.getShape.mkString(","))
    })

    val mainStartTime = System.nanoTime()
    println(s"end generate numpy ndarray,try to generate pytorch Tensor ${mainStartTime}")

//    val tensor = Tensor.createFromNDArray[Float](ndArray,false, CPU)
//    val endTime = System.nanoTime() // 记录结束时间
//    val duration = (endTime - mainStartTime) / 1e9 // 将纳秒转换为秒
//
//    println(s"Numpy NDArray  convert Pytorch Tensor done,shape ${tensor.shape.mkString(",")} duration ${duration} s ")
//    println(tensor.native.print())
//    tensor.detach().data()
  }
}
