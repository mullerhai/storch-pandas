package testcase.suite.convertion

import torch.pandas.DataFrame

object AvazuDataTest {

  def main(args: Array[String]): Unit  ={
    val etledPath = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\combined_df.csv"
    val traindf = DataFrame.readCSV(etledPath, 500)
    val numCols = traindf.nonnumeric.getColumns
    val selectDf = traindf.columnSelect(numCols).cast(classOf[Double]).numeric
    //    traindf.numeric.cast(classOf[Double]).values[Double]().printArray()
    
    val desc = selectDf.values[Double]()
//    println(desc.printArray())
    println(selectDf.getShape)
    println(selectDf.getColumns.mkString(","))
    selectDf.show()
    traindf.show()
  }
  def mains(args: Array[String]): Unit = {
    val csvPath2 ="D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\combined_df.csv"
    val csvPath = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\data\\avazu\\test.csv"
    val df = DataFrame.readCsv(csvPath2, 500)
    df.show()
    val selectCategoryCol = Array("site_id", "site_domain", "site_category", "app_id", "app_domain",
       "app_category", "device_id", "device_ip", "device_model")
    println(df.getShape)
    df.columnSelect(selectCategoryCol.toSeq).show()
    println(df.getColumns.mkString(","))
    println(df.nonnumeric.getColumns.mkString(","))


  }
}
