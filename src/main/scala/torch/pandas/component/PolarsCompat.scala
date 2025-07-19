package torch
package pandas.component
import torch.pandas.DataFrame
import torch.polars.api.{LazyFrame, Series, io, DataFrame as PolarsDataFrame}
import torch.polars.Polars
import torch.polars.api.io.Scannable
import torch.polars.example.utils.CommonUtils

import scala.jdk.CollectionConverters.*
import java.io.InputStream
import java.time.{LocalDate, ZonedDateTime}
import java.util.Date


object PolarsCompat {

  def polarsDataFrameConvertPandasDataFrame(polarsDataFrame: PolarsDataFrame): DataFrame[AnyRef] = {
    val rows = polarsDataFrame.rows()
    val shape = polarsDataFrame.shape
    val columnCnt = shape._1
    val rowCnt = shape._2
    val schema = polarsDataFrame.schema.getFieldNames
    val df = new DataFrame[AnyRef](schema*)
    while (rows.hasNext) {
      df.append(rows.next().toArray)
    }
    df.convert()
  }

  def pandasDataFrameConvertPolarsDataFrame(pandasDataFrame: DataFrame[AnyRef]): PolarsDataFrame = {

    val rows = pandasDataFrame.iterrows
    val schema = pandasDataFrame.getColumns
    val colsIter = pandasDataFrame.itercols
    val seriesIter = colsIter.zipWithIndex.map(ele =>
      val realData = ele._1.asInstanceOf[List[AnyRef]] match {
      case arr: List[Boolean] => Series.ofBoolean(schema(ele._2).toString,ele._1.map(_.asInstanceOf[Boolean]))
      case arr: List[Int] => Series.ofInt(schema(ele._2).toString,ele._1.map(_.asInstanceOf[Int]))
      case arr: List[Long] => Series.ofLong(schema(ele._2).toString,ele._1.map(_.asInstanceOf[Long]))
      case arr: List[Float] => Series.ofFloat(schema(ele._2).toString,ele._1.map(_.asInstanceOf[Float]))
      case arr: List[Double] => Series.ofDouble(schema(ele._2).toString,ele._1.map(_.asInstanceOf[Double]))
      case arr: List[String] => Series.ofString(schema(ele._2).toString,ele._1.map(_.asInstanceOf[String]).toArray)
      case arr: List[LocalDate] => Series.ofDate(schema(ele._2).toString,ele._1.map(_.asInstanceOf[LocalDate]).toIterable)
      case arr: List[ZonedDateTime] => Series.ofDateTime(schema(ele._2).toString,ele._1.map(_.asInstanceOf[ZonedDateTime]).toIterable)
      case arr: List[Array[AnyRef]] => Series.ofList(schema(ele._2).toString, ele._1.map(_.asInstanceOf[Array[AnyRef]]).toArray)
      case arr: List[Seq[AnyRef]]=> Series.ofList(schema(ele._2).toString, ele._1.map(_.asInstanceOf[Array[AnyRef]]).toArray)
      case arr: List[List[AnyRef]] => Series.ofList(schema(ele._2).toString, ele._1.map(_.asInstanceOf[Array[AnyRef]]).toArray)
      case _ => throw new IllegalArgumentException(s"Unsupported type: ${ele._1.getClass}")
    }
      realData
    )
    PolarsDataFrame.fromSeries(seriesIter.next(),seriesIter.toIterable)
  }
  /** Reads an Apache Arrow file. */
  def readJsonLine(file: String): DataFrame[AnyRef] = {
    val path = CommonUtils.getResource(file)
    val df = Polars.scan.jsonLines(path).collect()
    polarsDataFrameConvertPandasDataFrame(df)
  }

  def writeJsonLine(dataFrame: DataFrame[AnyRef],outPath:String):Unit ={
    val df = pandasDataFrameConvertPolarsDataFrame(dataFrame)
    df.write().jsonLines(outPath)
  }

  /** Reads an Apache Avro file. */
//  def readAvro(file: String, schema: InputStream): DataFrame[AnyRef] ={
//    val path = CommonUtils.getResource(file)
//    val df = Polars.scan.(path, schema).collect()
//    polarsDataFrameConvertPandasDataFrame(df)
//  }

  /** Reads an Apache Parquet file. */
  def readParquet(file: String): DataFrame[AnyRef] = {
    val path = CommonUtils.getResource(file)
    val df = Polars.scan.parquet(path).collect()
    polarsDataFrameConvertPandasDataFrame(df)
  }

  def readParquet(file: String,option: Map[String, String]): DataFrame[AnyRef] = {
    val path = CommonUtils.getResource(file)
    val df = Polars.scan.options(option.toIterator.toIterable).parquet(path).collect()
    polarsDataFrameConvertPandasDataFrame(df)
  }

  /** Reads an Apache Parquet file. */
  def readIPC(file: String): DataFrame[AnyRef] = {
    val path = CommonUtils.getResource(file)
    val df = Polars.scan.ipc(path).collect
    polarsDataFrameConvertPandasDataFrame(df)

  }

  /** Reads an Apache Avro file. */
  def writeAvro(pandasDataframe: DataFrame[AnyRef],outputPath: String, option: Option[Map[String, String]] = None): Unit ={
    val df = pandasDataFrameConvertPolarsDataFrame(pandasDataframe)
    if option.isDefined then
      df.write()
        .options(
          option.get
        )
        .avro(outputPath)
    else
      df.write()
        .options(
          Map(
            "write_compression" -> "zstd",
            "write_mode" -> "overwrite",
            "write_parquet_stats" -> "full"
          )
        )
        .avro(outputPath)
  }

  /** Reads an Apache Avro file. */
  def writeParquet(pandasDataframe: DataFrame[AnyRef],outputPath: String, option: Option[Map[String, String]] = None): Unit = {
    val df = pandasDataFrameConvertPolarsDataFrame(pandasDataframe)
    if option.isDefined then
      df.write()
        .options(
     option.get
        )
        .parquet(outputPath)
    else
      df.write()
        .options(
          Map(
            "write_compression" -> "zstd",
            "write_mode" -> "overwrite",
            "write_parquet_stats" -> "full"
          )
        )
        .parquet(outputPath)
  }

  /** Reads an Apache Avro file. */
  def writeIPC(pandasDataframe: DataFrame[AnyRef],outputPath: String, option: Option[Map[String, String]] = None):Unit ={
    val df = pandasDataFrameConvertPolarsDataFrame(pandasDataframe)
    if option.isDefined then
      df.write()
        .options(
          option.get
        )
        .ipc(outputPath)
    else
      df.write()
        .options(
          Map(
            "write_compression" -> "zstd",
            "write_mode" -> "overwrite",
            "write_parquet_stats" -> "full"
          )
        )
        .ipc(outputPath)
  }


}
