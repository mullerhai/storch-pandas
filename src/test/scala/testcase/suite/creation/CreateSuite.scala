package testcase.suite.creation

import org.scalatest.funsuite.AnyFunSuite
import torch.pandas.DataFrame

import scala.collection.{mutable, Set as KeySet}

class CreateSuite extends AnyFunSuite {


  test("test create DataFrame from Seq with row and column names") {
    val cols = Seq("category", "name", "value","version")
    val rows= Seq("row1", "row2", "row3", "row4", "row5", "row6")
    val col4Data = Seq("test", "release", "alpha", "beta", "gama", "peter")
    val col5Data = Seq("one", "two", "three", "four", "five", "six")
    //  val col6Data = Seq(Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN,Float.NaN)// Float.NaN, 48, 52, 67)
    val col1Data = Seq(14, 25,Float.NaN, 48, 52, 67)
    val col2Data = Seq(3, 5, Float.NaN, 0, 9, 10)
    val col3Data = Seq(10, 25, 32, 45, 53, 60)
    val data = List(col1Data, col2Data, col4Data, col5Data)
    // this(index: KeySet[Any], columns: mutable.Set[Any], data: List[Seq[V]])
    var df = new DataFrame(rows, cols, data)
    df.show()
    println(df.getShape)
    println(df.getColumns.mkString(","))
  }
}
