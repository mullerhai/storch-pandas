package testcase.suite.io

import org.scalatest.funsuite.AnyFunSuite
import torch.pandas.DataFrame

import java.sql.{Connection, DriverManager}

class ReadSuite extends AnyFunSuite {


  val npFile = "D:\\data\\git\\testNumpy\\src\\main\\resources\\npy_dir\\random_float32_array.npy"
  //    val npFile ="D:\\data\\git\\testNumpy\\src\\main\\resources\\npy_dir\\random_bool_array.npy"
  val csvPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\avazu\\test.csv"

  val picklePath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\npy_dir\\random_float32_array.npy"
  val xlsfile = "src/main/resources/sample_new2.xls"
  val xlsxfile = "src/main/resources/sample_new.xlsx"

  val parquetPath = "src/test/resources/data.parquet"
  val ipcPath = "src/test/resources/data.ipc"
  val iPCfullPath = "D:\\data\\git\\storch-pandas-old-use\\src\\test\\resources\\data.ipc"
  val jsonLinePath = "src/test/resources/data.json"
  val jsonPath = "src/test/resources/random_data.json"
  val jsonAmazonPath = "D:\\data\\git\\testNumpy\\src\\main\\resources\\amzon\\reviews_Electronics_5.json"

  val hdf5path ="src/test/resources/norm.hdf5"
  val sqlExec = "select name, age ,tall, weight from person limit 500"
  
  test("read sql file generate dataframe") {
    val conn = "jdbc:sqlite:src/test/resources/test.db"
    val connect :Connection = DriverManager.getConnection(conn)
    val df = DataFrame.readSql(connect,sqlExec)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }
//  test("read hdf5 file generate dataframe") {
//    val df = DataFrame.readHDF5(hdf5path)
//    df.show()
//    println(df.getShape)
//    println(df.getColumns.mkString(","))
//  }
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
    val df = DataFrame.readCSV(csvPath, 500)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))

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
    val df = DataFrame.readPickle[Float](picklePath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

  test("read parquet file generate dataframe") {
    val df = DataFrame.readParquet(picklePath)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }

}