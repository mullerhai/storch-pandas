import torch.DataFrame
import torch.DataFrame.PlotType.{AREA, BAR, LINE, SCATTER}

import scala.collection.immutable.Seq
import scala.collection.{mutable, Set as KeySet}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@main
def main(): Unit =
  // TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
  // to see how IntelliJ IDEA suggests fixing it.
//    (1 to 5).map(println)

    val cols: Seq[String] = Seq("category", "name", "value","version","age","score")
    val rows: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5", "row6")
    val col1Data = Seq("test", "release", "alpha", "beta", "gama", "peter")
    val col2Data = Seq("one", "two", "three", "four", "five", "six")
    val col3Data = Seq(14, 25, Float.NaN, 48, 52, 67)
    val col4Data = Seq(3, 5, Float.NaN, 0, 9, 10)
    val col5Data = Seq(10, 25, 32, 45, 53, 60)
//    val col1Data = Seq(14, 25, 36, 48, 52, 67)
//    val col2Data = Seq(3, 5, 7, 8, 9, 10)
//  val col3Data = Seq("5one", "7two", "0three", "6one", "3two", "2three") //
    val col6Data = Seq(10, 20, 30, 40, 50, 60)
    val data = List(col1Data, col2Data, col3Data,col4Data, col5Data, col6Data)
    // this(index: KeySet[Any], columns: mutable.Set[Any], data: List[Seq[V]])
    val df = new DataFrame[Any](rows, cols.asInstanceOf[Seq[Any]], data) // ("row1", "row2", "row3")//, , ("test", "test", "test", "beta", "beta", "beta"), ("one", "two", "three", "one", "two", "three"), (10, 20, 30, 40, 50, 60)))
    val index = df.index
    val colz = df.columns
    val values = df.data
    val gp = df.groups
    val df2 = df.drop(Seq(2),true)
//    df2.show()
//    println(df.rows)
    println(df.row("row4"))
    println(df.flatten)
    println(df.index)
    println(df.columns)
    println(df.data)
    println(df.groups)
//    println(df.`var`)

//    df.show()
    df.plot(SCATTER)
//    println(index)
//    println(colz)
    println(df.columns.names.mkString(","))
//    df.plot()

//    println(df.columns.get("value"))
//    println(df.col_with_view(2))
//    println(df.col("value"))

//    val xlsPath =
//      "D:\\data\\git\\storch-pandas-old-use\\src\\main\\resources\\sample_new2.xls"
//    val xlsdf = DataFrame.readXls(xlsPath)
//    println(xlsdf)

//    DataFrame. ( "D:\\data\\git\\storch-pandas-old-use\\src\\main\\resources\\sample_new2.xls")

    //    val csvPath =
//      "D:\\data\\git\\storch-pandas\\src\\main\\resources\\industry_sic.csv"
//    val csvdf = DataFrame.readCsv(csvPath, ",", " ", true)
//    println(csvdf)


//    println(df)
//    println(df.heads(6))
//  val csvPath = ""
//  DataFrame.readCsv("")
//    println(df.getColumns.mkString(","))


//    for (i <- 1 to 5) do
//      // TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
//      // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
//      println(s"i = $i")
