package testcase.suite.convertion

import org.scalatest.funsuite.AnyFunSuite
import torch.pandas.DataFrame
import torch.pandas.DataFrame.Axis.COLUMNS
class ComputeSuite extends AnyFunSuite {

  def before():DataFrame[Int | Float | String] = {
    val cols = Seq("category", "name","age", "value", "version")
    val rows = Seq("row1", "row2", "row3", "row4", "row5", "row6")
    val col4Data = Seq("test", "release", "alpha", "beta", "gama", "peter")
    val col5Data = Seq("one", "two", "three", null, "five", "six")
    //  val col6Data = Seq(Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN)// Float.NaN, 48, 52, 67)
    val col1Data = Seq(14, 25, 40, 48, 52, 67)
    val col2Data = Seq(3, 5, Float.NaN, 0, null, 10)
    val col3Data = Seq(10, 25, 32, 45, 53, 60)
    val data = List(col1Data, col2Data,col3Data, col4Data, col5Data)
    // this(index: KeySet[Any], columns: mutable.Set[Any], data: List[Seq[V]])
    var df = new DataFrame(rows, cols, data)
    df.show()
    println(df.getShape)
//    println(df.getColumns.mkString(","))
    df
  }
  test("compute describe") {
    val df = before()
    val desc = df.describe
    println(desc)
  }

  test("compute sum") {
    val df = before()
    val desc = df.sum
    println(desc)
  }

  test("compute mean") {
    val df = before()
    val desc = df.mean
    println(desc)
  }
  test("compute var") {
    val df = before()
    val desc = df.`var`
    println(desc)
  }

  test("compute stddev") {
    val df = before()
    val desc = df.stddev
    println(desc)
  }

  test("compute cov") {
    val df = before()
    val desc = df.cov
    println(desc)
  }

  test("compute cummax") {
    val df = before()
    val desc = df.cummax
    println(desc)
  }

  test("compute cummin") {
    val df = before()
    val desc = df.cummin
    println(desc)
  }

  test("compute cumsum") {
    val df = before()
    val desc = df.cumsum
    println(desc)
  }

  test("compute transpose") {
    val df = before()
    val desc = df.transpose
    println(desc)
  }

  test("compute prod") {
    val df = before()
    val desc = df.prod
    println(desc)
  }

  test("compute max") {
    val df = before()
    val desc = df.max
    println(desc)
  }
  test("compute min") {
    val df = before()
    val desc = df.min
    println(desc)
  }
  test("compute collapse") {
    val df = before()
    val desc = df.collapse
    println(desc)
  }
  test("compute explode") {
    val df = before()
    val desc = df.explode
    println(desc)
  }
  test("compute cumprod") {
    val df = before()
    val desc = df.cumprod
    println(desc)
  }
  test("compute coalesce") {
    val df = before()
    val desc = df.coalesce()
    println(desc)
  }
  test("compute median") {
    val df = before()
    val desc = df.median
    println(desc)
  }
  test("compute percentile") {
    val df = before()
    val desc = df.percentile(0.5)
    println(desc)
  }
  test("compute kurt") {
    val df = before()
    val desc = df.kurt
    println(desc)
  }
  test("compute flatten") {
    val df = before()
    val desc = df.flatten
    println(desc)
  }

  test("compute count") {
    val df = before()
    val desc = df.count
    println(desc)
  }
  test("compute numeric") {
    val df = before()
    val desc = df.numeric
    println(desc)
  }

  test("compute fillna") {
    val df: DataFrame[Int | Float | String] = before()
    val desc = df.fillna(12.asInstanceOf[Int | Float | String])
    println(desc)
  }

  test("compute dropna row ") {
    val df: DataFrame[Int | Float | String] = before()
    val desc = df.dropna //(12.asInstanceOf[Int | Float | String])
    println(desc)
  }
  test("compute dropna column") {
    val df: DataFrame[Int | Float | String] = before()
    val desc = df.dropna(COLUMNS) //(12.asInstanceOf[Int | Float | String])
    println(desc)
  }

  test("compute rename column") {
    val df: DataFrame[Int | Float | String] = before()
    val desc = df.rename("category", "category2")//(12.asInstanceOf[Int | Float | String])
    println(desc)
  }

  test("compute filter column can not filter NaN") {
    val df: DataFrame[Int | Float | String] = before()
    df.foreach(println(_))
    println("\r\n")
//    val result2 = list.filter(el => el.exists {
//      case num: Float if num.isNaN => true
//      case _ => false
//    })
//    val desc = df.filter(el => el.contains(Float.NaN)||el.contains(null)||el.contains("NaN")||el.contains(Double.NaN)||el.contains(None))//(12.asInstanceOf[Int | Float | String])
    val desc = df.filter(el => el.contains(Float.NaN) || el.contains(null) || el.contains("NaN") || el.contains(Double.NaN) || el.exists {
      case num: Float if num.isNaN => true
      case _ => false
    }) //(12.asInstanceOf[Int | Float | String])

    println(desc)
  }






}
