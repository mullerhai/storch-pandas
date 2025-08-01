package testcase.suite.io

import org.scalatest.funsuite.AnyFunSuite
import torch.pandas.DataFrame
import torch.pandas.component.Person
import torch.polars.Polars
import torch.polars.example.utils.CommonUtils

import java.sql.{Connection, DriverManager}

class ReadSuite extends AnyFunSuite {


  val npFile = "D:\\data\\git\\testNumpy\\src\\main\\resources\\npy_dir\\random_float32_array.npy"
  //    val npFile ="D:\\data\\git\\testNumpy\\src\\main\\resources\\npy_dir\\random_bool_array.npy"
  val csvPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\avazu\\test.csv"

  val csvPath2 = "D:\\code\\data\\llm\\手撕LLM速成班-试听课-小冬瓜AIGC-20231211\\data\\avazu\\test.csv"
  val picklePath = "D:\\data\\git\\storch-pandas-old-use\\src\\test\\resources\\newOnlyNumpyArray.pkl"
//  val picklePath = ""
  val xlsfile = "src/main/resources/sample_new2.xls"
  val xlsxfile = "src/main/resources/sample_new.xlsx"

  val parquetPath = "src/test/resources/data.parquet"
  val ipcPath = "src/test/resources/data.ipc"
  val iPCfullPath = "D:\\data\\git\\storch-pandas-old-use\\src\\test\\resources\\data.ipc"
  val jsonLinePath = "src/test/resources/data.json"
  val jsonPath = "src/test/resources/random_data.json"
  val jsonAmazonPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\amzon\\reviews_Electronics_5.json"

  val hdf5path ="D:\\data\\git\\testNumpy\\src\\main\\resources\\example.h5" //"src/test/resources/norm.hdf5"
  val sqlExec = "select name, age ,tall, weight from person limit 500"
  
  test("read sql file generate dataframe") {
    val conn = "jdbc:sqlite:src/test/resources/test.db"
    val connect :Connection = DriverManager.getConnection(conn)
    val df = DataFrame.readSql(connect,sqlExec)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }
  //readHdf5(hdf5Path: String, datasetName: String, needConvert: Boolean = false)
  test("read hdf5 file generate dataframe") {
    val dsname = "data_6"
    val df = DataFrame.readHdf5(hdf5path,datasetName = dsname)
    println(df.head.asInstanceOf[Array[Array[Float]]].head.mkString(","))
//    df.show()
//    println(df.getShape)
//    println(df.getColumns.mkString(","))
  }
  test("read parquet file by polars generate dataframe") {
    val df = DataFrame.readParquet(parquetPath)
    println(df.getShape)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read ipc file by polars generate dataframe") {
    val df = DataFrame.readIPC(iPCfullPath)//ipcPath)
    println(df.getShape)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }
  test("read json file generate dataframe"){
    val df = DataFrame.readJson(jsonPath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read amazon json line file by normal source generate dataframe") {
    val df = DataFrame.readJsonLine(jsonAmazonPath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read json line file by normal source generate dataframe") {
    val df = DataFrame.readJsonLine(jsonLinePath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read criteo by csv "){
    def main(args: Array[String]): Unit = {
      val path = "D:\\data\\git\\testNumpy\\src\\main\\resources\\criteo_small\\train.txt"
      val header = Some((0 to 39).map(_.toString).toSeq)
      val df = DataFrame.readCsv(file = path, separator = "\\t", naString = "", hasHeader = false, limit = -1, needConvert = false, headers = header)
      df.show()
    }
  }

  test("read criteo by CSV .."){
    val path = "D:\\data\\git\\testNumpy\\src\\main\\resources\\criteo_small\\train.txt"
    val header = Some((0 to 39).map(_.toString).toSeq)
    val df = DataFrame.readCSV(file = path, spliterator = "\\t", header = header, limit = -1, needConvert = false)

    df.show()

  }


  test("read json line file by polars generate dataframe") {
    val df = DataFrame.readJsonLinePolars(jsonLinePath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }
  test("read npy file generate dataframe") {
    val df2 = DataFrame.fromNumpyNpyFile[Float](npFile)
    println(df2.getShape)
    df2.show()
  }

  test("read csv file generate dataframe") {
    val df = DataFrame.readCSV(csvPath, 500, needConvert =  true)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
    println(df.nonnumeric.getColumns.mkString(","))

  }

  test("read csv file generate dataframe by normal") {
    val df = DataFrame.readCsv(csvPath, 500)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
    println(df.nonnumeric.getColumns.mkString(","))

  }

  test("read csv file generate dataframe by normal 2") {
    val df = DataFrame.readCsv(csvPath2, 500)
    df.show()
//    println(df.getShape)
//    println(df.getColumns.mkString(","))
//    println(df.nonnumeric.getColumns.mkString(","))
    println(df.numeric.getColumns.mkString(","))

  }

  test("read xls file generate dataframe") {
    val df = DataFrame.readXls(xlsfile)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read xlsx file generate dataframe") {
    val df = DataFrame.readXlsx(xlsxfile)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read pickle file generate dataframe") {
    val df = DataFrame.readPickle[Person](picklePath)
//    df.numpyNdarray.toNDArray[Float]().printArray()
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read pickle file generate dataframe numpy") {
    val df = DataFrame.readPickleForNumpy(picklePath)
    df.numpyNdarray.toNDArray[Float]().printArray()
    //    df.show()
    //    println(df.getShape)
    //    println(df.getColumns.mkString(","))
  }

  test("read parquet file generate dataframe") {

    val df = DataFrame.readParquet(parquetPath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read parquet file generate dataframe by polars") {
    println(s"read parquet file generate dataframe by polars")
    val path = CommonUtils.getResource(parquetPath)
    val df = Polars.scan.parquet(path).collect()
    df.show()
//    val df = DataFrame.readParquet(picklePath)
//    df.show()
//    println(df.getShape)
//    println(df.getColumns.mkString(","))
  }

}