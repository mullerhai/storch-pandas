import torch.DataFrame
import torch.DataFrame.Axis.{COLUMNS, ROWS}
import torch.DataFrame.JoinType
import torch.DataFrame.PlotType.{AREA, BAR, GRID, GRID_WITH_TREND, LINE, LINE_AND_POINTS, SCATTER, SCATTER_WITH_TREND}

import scala.collection.immutable.Seq
import scala.collection.{mutable, Set as KeySet}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
def testdf(): Unit = {

    //  val df = DataFrame(1, 2, 3, 4, 5)
    //  println(df.row("row4"))
    //  println(df.columns().asScala.mkString(", "))
    //  val df2 =df.flatten() //.percentChange()//.merge(df, JoinType.LEFT) //.concat(df) //join(df) //.dropna(Axis.COLUMNS).fillna(1).groupBy("name") //.max()//.unique() //.explode() //.numeric()//nonnumeric() //.cov() //.transpose() //.kurt() //.stddev() //.cummax() //.cumsum() //.describe() //.collapse()//.prod()
    //  println(df2)
    //  df2.fillna(0.1)
    //  df.add("ca")//,col5Data.asJava)
    //  df.set("ca", col5Data.asJava)
    //  df.append("newCol", Seq(1, 2, 3, 4, 5, 6).asJava)
    //  df.show()
    //  df = df.drop("category")
}

@main
def main(): Unit =
  // TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
  // to see how IntelliJ IDEA suggests fixing it.
//    (1 to 5).map(println)

    val cols: Seq[String] = Seq("category", "name", "value","version","age","score")
    val rows: Seq[String] = Seq("row1", "row2", "row3", "row4", "row5", "row6")
    val col1Data = Seq("test", "release", "alpha", "beta", "gama", "peter")
    val col2Data = Seq("one", "two", "three", "four", "five", "six")
    val col3Data = Seq(14, 25, Float.NaN, 0, 52, 67)
    val col4Data = Seq(3, 5, Float.NaN, 0, 9, 10)
    val col5Data = Seq(10, 25, 32, 45, 53, 60)
//    val col1Data = Seq(14, 25, 36, 48, 52, 67)
//    val col2Data = Seq(3, 5, 7, 8, 9, 10)
//  val col3Data = Seq("5one", "7two", "0three", "6one", "3two", "2three") //
    val col6Data = Seq(10, 20, 30, 40, 50, 60)
    val data = List(col3Data, col3Data, col3Data,col4Data, col4Data, col4Data)
    // this(index: KeySet[Any], columns: mutable.Set[Any], data: List[Seq[V]])
    val df = new DataFrame[Any](rows, cols.asInstanceOf[Seq[Any]], data) // ("row1", "row2", "row3")//, , ("test", "test", "test", "beta", "beta", "beta"), ("one", "two", "three", "one", "two", "three"), (10, 20, 30, 40, 50, 60)))
    val index = df.index
    val colz = df.columns
    val values = df.data
    val gp = df.groups
    val df2 = df.drop(Seq(2),true)

//    println(df.rows)
    println(df.row("row4"))
    println(df.flatten)
    println(df.index)
    println(df.columns)
    println(df.data)
    println(df.groups)
    val ff = df.dropna(COLUMNS)
    val ff3 = df.dropna(ROWS)
    //    println(ff.show())
    ff3.fillna(100).show()
    df.kurt
//    df.unique
    df.percentile(0.5)
    df.median
    df.stddev
    println(df.`var`)
    println(df.cov)
    println(df.cummax)
    println(df.cumsum)
    println(df.transpose)
    println(df.prod)
    println(df.max)
    println(df.min)
    println(df.sum)
    println(df.mean)
    println(df.count)
    println(df.collapse)
    println(df.explode)
    println(df.nonnumeric)
    println(df.numeric)
    println(df.cumprod)
    println(df.cummin)
    println(df.coalesce())
    println(df.percentChange)
    println(df.join(df2))
    val ndf = df.slice(2,5)
    ndf.append("newCol", Seq(1, 2, 3, 4, 5, 6))
//    ndf.add( Seq(2, 3, 4, 5))
    ndf.skew
    ndf.tail
    ndf.rename("age","age2")

//    ndf.show()
    println("try to concat....")
    ndf.reshape(3, 2)
//    println(df.merge(df))
//    println(df.concat(df))
//    println(df.heads(2).show())




//    println(df.describe)
//    df.show()
//    df.plot(LINE)
//    df.plot(SCATTER)
//    df.plot(AREA)
//    df.plot(LINE_AND_POINTS)
//    df.plot(GRID)
//    df.plot(SCATTER_WITH_TREND)
//    df.plot(GRID_WITH_TREND)
//    df.plot(BAR) //have bug
//    df.plot(PlotType.)


//    val df3 = df.concat(df) //merge(df2, JoinType.LEFT)
//    df3.show()
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
