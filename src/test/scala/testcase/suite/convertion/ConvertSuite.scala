package testcase.suite.convertion

import org.scalatest.funsuite.AnyFunSuite
import torch.pandas.DataFrame
import torch.pandas.DataFrame.{JoinType, train_test_split}

class ConvertSuite extends AnyFunSuite{
  val trainPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\avazu\\train.csv"
  val testPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\avazu\\test.csv"
  val etledPath = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\combined_df.csv"
  //    val dff = readCSV

  test("split dataframe for train test dataset") {
    val df = DataFrame.readCSV(testPath, 500)
    df.show()
    println(df.getShape)
    val dfSeq = df.train_test_split("C21",0.2)
    println(df.getColumns.mkString(","))
    println(dfSeq(0).getColumns.mkString(","))
    println(dfSeq(1).getColumns.mkString(","))
    println(dfSeq(2).getColumns.mkString(","))
    println(dfSeq(3).getColumns.mkString(","))
  }

  test("drop columns dataframe for train test dataset") {
    val df = DataFrame.readCSV(testPath, 500)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
    println(s" df shape ${df.getShape} column size ${df.getColumns.size} -> df.getColumns = ${df.getColumns.mkString(",")}")
    val dfDrop = df.drop("C21")
    println(s"drop df shape ${dfDrop.getShape} column size ${dfDrop.getColumns.size} -> dfDrop.getColumns = ${dfDrop.getColumns.mkString(",")}")
  }

  test("select columns dataframe for train test dataset") {
    val df = DataFrame.readCSV(testPath, 500)
    //    val df = DataFrame.readCSV(testPath, 500)
    val selectColz = Array("C15", "C14", "C17", "C16", "C19", "C18", "click", "device_ip", "site_id", "app_category")
    val selectdf = df.columnSelect(selectColz) //.values[Double].printArray() //.show()
//    df.show()
    println(s" df shape ${df.getShape} column size ${df.getColumns.size} -> df.getColumns = ${df.getColumns.mkString(",")}")
    println(s"select columns  selectdf shape ${selectdf.getShape} column size ${selectdf.getColumns.size} -> selectdf.getColumns = ${selectdf.getColumns.mkString(",")}")

  }

  test("select index range dataframe for train test dataset") {
    val df = DataFrame.readCSV(testPath, 500)
    //    val df = DataFrame.readCSV(testPath, 500)
    val selectColz = Array("C15", "C14", "C17", "C16", "C19", "C18", "click", "device_ip", "site_id", "app_category")
    val selectdf = df.indexSelect(12,45) //.values[Double].printArray() //.show()
    //    df.show()
    println(s" df shape ${df.getShape} column size ${df.getColumns.size} -> df.getColumns = ${df.getColumns.mkString(",")}")
    println(s"select columns  selectdf shape ${selectdf.getShape} column size ${selectdf.getColumns.size} -> selectdf.getColumns = ${selectdf.getColumns.mkString(",")}")

  }

  test("select index Seq dataframe for train test dataset") {
    val df = DataFrame.readCSV(testPath, 500)
    //    val df = DataFrame.readCSV(testPath, 500)
    val selectColz = Array("C15", "C14", "C17", "C16", "C19", "C18", "click", "device_ip", "site_id", "app_category")
    val selectdf = df.indexSelect(Seq(12,23,36, 45)) //.values[Double].printArray() //.show()
    //    df.show()
    println(s" df shape ${df.getShape} column size ${df.getColumns.size} -> df.getColumns = ${df.getColumns.mkString(",")}")
    println(s"select columns  selectdf shape ${selectdf.getShape} column size ${selectdf.getColumns.size} -> selectdf.getColumns = ${selectdf.getColumns.mkString(",")}")

  }

  test("dataframe concat  for train test dataset") {
    val traindf = DataFrame.readCSV(trainPath, 500)
    val testdf = DataFrame.readCSV(testPath, 500) //trainPath
    //    val df = DataFrame.readCSV(testPath, 500)
//    val selectColz = Array("C15", "C14", "C17", "C16", "C19", "C18", "click", "device_ip", "site_id", "app_category")
    val concatdf = traindf.concat(testdf) //df.indexSelect(Seq(12, 23, 36, 45)) //.values[Double].printArray() //.show()
    //    df.show()
    println(s"train df shape ${traindf.getShape} column size ${traindf.getColumns.size} -> df.getColumns = ${traindf.getColumns.mkString(",")}")
    println(s"concatdf columns  concatdf shape ${concatdf.getShape} column size ${concatdf.getColumns.size} -> selectdf.getColumns = ${concatdf.getColumns.mkString(",")}")
  }

  test("dataframe convert to numpy  for train test dataset") {
    val traindf = DataFrame.readCSV(trainPath, 500)
    val testdf = DataFrame.readCSV(testPath, 500) //trainPath
    //    val df = DataFrame.readCSV(testPath, 500)
    val selectColz = Array("C15", "C14", "C17", "C16", "C19", "C18", "click", "device_ip", "site_id", "app_category")
    val ndArray = traindf.columnSelect(selectColz).cast(classOf[Double]).values[Double]() //.show()
    ndArray.printArray()
    println(s"train df shape ${traindf.getShape} column size ${traindf.getColumns.size} -> df.getColumns = ${traindf.getColumns.mkString(",")}")
    println(s"concatdf columns  concatdf shape ${ndArray.getShape} ") //column size ${concatdf.getColumns.size} -> selectdf.getColumns = ${concatdf.getColumns.mkString(",")}")
  }

  test("compute merge") {
    val traindf = DataFrame.readCSV(trainPath, 500)
    val testdf = DataFrame.readCSV(testPath, 500)
    val df =traindf.merge(testdf)
    println(df.getShape)
    println(traindf.getShape)
  }
  test("compute join") {
    val traindf = DataFrame.readCSV(trainPath, 500)
    val testdf = DataFrame.readCSV(testPath, 500)
    val desc = traindf.join(testdf, JoinType.INNER)
    println(desc.getShape)
  }

  test("transform pandas to numpy ndarray 1") {
    val traindf = DataFrame.readCsv(testPath, 500)

    val desc = traindf.values[Double]()
    println(desc.getShape)
    println(desc.printArray())
  }


  test("transform pandas to numpy ndarray 2") {
    val traindf = DataFrame.readCsv(etledPath, 500)

    val desc = traindf.values[Double]()

    println(desc.printArray())
    println(desc.getShape)
    println(traindf.getColumns.mkString(","))
    println(traindf.nonnumeric.getColumns.mkString(","))
    println(traindf.nonnumeric.getColumns.size)
    println(traindf.numeric.getColumns.mkString(","))
  }

  test("transform pandas cast to double to numpy ndarray 3") {
    val traindf = DataFrame.readCsv(etledPath, 500)
    val numCols = traindf.nonnumeric.getColumns
    val selectDf = traindf.columnSelect(numCols).cast(classOf[Double]).numeric
//    traindf.numeric.cast(classOf[Double]).values[Double]().printArray()
    val desc = selectDf.values[Double]()

    println(desc.printArray())
    println(selectDf.getShape)
    println(selectDf.getColumns.mkString(","))
//    println(traindf.nonnumeric.getColumns.mkString(","))
//    println(traindf.nonnumeric.getColumns.size)
//    println(traindf.numeric.getColumns.mkString(","))
  }

  test("transform pandas cast to double to numpy ndarray 4") {
    val traindf = DataFrame.readCsv(etledPath, 500)
    val numCols = traindf.nonnumeric.getColumns
    val selectDf = traindf.cast(classOf[Double]).numeric
    //    traindf.numeric.cast(classOf[Double]).values[Double]().printArray()
    val desc = selectDf.values[Double]()

    println(desc.printArray())
    println(selectDf.getShape)
    println(selectDf.getColumns.mkString(","))
  }

  test("split  pandas column  select then  concat these column to df") {
    val traindf = DataFrame.readCsv(etledPath, 500)
    val numCols = traindf.numeric.getColumns
    val nonumCols = traindf.nonnumeric.getColumns
    val numDf = traindf.columnSelect(numCols)
    val nonumDf = traindf.columnSelect(nonumCols) //.cast(classOf[Double])
    val mergeDf = numDf.merge(nonumDf)

//    val selectDf = traindf.cast(classOf[Double]).numeric
    //    traindf.numeric.cast(classOf[Double]).values[Double]().printArray()
    val desc = mergeDf.values[Double]()

    println(desc.printArray())
    println(mergeDf.getShape)
    println(mergeDf.getColumns.mkString(","))
  }

}
