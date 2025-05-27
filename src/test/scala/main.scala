import torch.DataFrame

import scala.collection.immutable.Seq
import scala.collection.{mutable, Set as KeySet}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@main
def main(): Unit =
  // TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
  // to see how IntelliJ IDEA suggests fixing it.
    (1 to 5).map(println)

    val cols: mutable.Set[Any] = mutable.Set("category", "name", "value")
    val rows: KeySet[Any] = KeySet("row1", "row2", "row3")
    val col1Data = Seq("test", "test", "test", "beta", "beta", "beta")
    val col2Data = Seq("one", "two", "three", "one", "two", "three")
//  val col3Data = Seq("5one", "7two", "0three", "6one", "3two", "2three") //
    val col3Data = Seq(10, 20, 30, 40, 50, 60)
    val data = List(col1Data, col2Data, col3Data)
    // this(index: KeySet[Any], columns: mutable.Set[Any], data: List[Seq[V]])
    val df = new DataFrame[String](rows, cols, data) // ("row1", "row2", "row3")//, , ("test", "test", "test", "beta", "beta", "beta"), ("one", "two", "three", "one", "two", "three"), (10, 20, 30, 40, 50, 60)))

    val csvPath =
      "D:\\data\\git\\storch-pandas\\src\\main\\resources\\industry_sic.csv"
    val csvdf = DataFrame.readCsv(csvPath, ",", " ", true)
    println(csvdf)
    println(df)
    println(df.heads(6))
//  val csvPath = ""
//  DataFrame.readCsv("")
    println(df.getColumns.mkString(","))
    for (i <- 1 to 5) do
      // TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
      // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
      println(s"i = $i")
