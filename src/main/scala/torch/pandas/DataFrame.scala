/*
 * storch -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package torch.pandas

import java.awt.Container
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util
import java.util.Comparator
import scala.collection.Set as KeySet
import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import torch.pandas.DataFrame.logger
//import java.lang.reflect.Array
import com.codahale.metrics.annotation.Timed

import torch.numpy.enums.DType
import torch.numpy.enums.DType.Float32
import torch.numpy.extern.NpyFile
import torch.numpy.matrix.NDArray
import torch.numpy.serve.Numpy
import torch.numpy.serve.TorchNumpy
import torch.pandas.DataFrame
import torch.pandas.DataFrame.Axis
import torch.pandas.DataFrame.Axis.ROWS
import torch.pandas.component.BlockManager
import torch.pandas.component.CSVCompat
import torch.pandas.component.Display
import torch.pandas.component.Index
import torch.pandas.component.JsonCompat
import torch.pandas.component.PickleCompat
import torch.pandas.component.PolarsCompat
import torch.pandas.component.Shell
import torch.pandas.component.Views
import torch.pandas.component.XlsxCompat
import torch.pandas.function.Timeseries
import torch.pandas.function.Transforms
import torch.pandas.operate
import torch.pandas.operate.Combining
import torch.pandas.operate.Grouping
import torch.pandas.operate.Pivoting
import torch.pandas.operate.Shaping
import torch.pandas.operate.Sorting
import torch.pandas.operate.SparseBitSet
import torch.pandas.service.Aggregation
import torch.pandas.service.Inspection
import torch.pandas.service.Selection
import torch.pandas.service.Serialization
import torch.pickle.objects.MulitNumpyNdArray

/** A data frame implementation in the spirit of <a
  * href="http://pandas.pydata.org">Pandas</a> or <a
  * href="http://cran.r-project.org/doc/manuals/r-release/R-intro.html#Data-frames">
  * R</a> data frames.
  *
  * <p>Below is a simple motivating example. When working in Java, data
  * operations like the following should be easy. The code below retrieves the
  * S&P 500 daily market data for 2008 from Yahoo! Finance and returns the
  * average monthly close for the three top months of the year.</p>
  *
  * <pre> {@code >
  * DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("gspc.csv")) >
  * .retain("Date", "Close") > .groupBy(row ->
  * Date.class.cast(row.get(0)).getMonth()) > .mean() > .sortBy("Close") >
  * .tail(3) > .apply(value -> Number.class.cast(value).intValue()) >
  * .col("Close"); [1370, 1378, 1403] }</pre>
  *
  * <p>Taking each step in turn: <ol> <li>{@link # readCsv ( String )} reads csv
  * data from files and urls</li> <li>{@link # retain ( Object...)} is used to
  * eliminate columns that are not needed</li> <li>{@link # groupBy (
  * KeyFunction )} with a key function is used to group the rows by month</li>
  * <li>{@link # mean ( )} calculates the average close for each month</li>
  * <li>{@link # sortBy ( Object...)} orders the rows according to average
  * closing price</li> <li>{@link # tail ( int )} returns the last three rows
  * (alternatively, sort in descending order and use head)</li> <li>{@link #
  * apply ( Function )} is used to convert the closing prices to Ints (this is
  * purely to ease comparisons for verifying the results</li> <li>finally,
  * {@link # col ( Object )} is used to extract the values as a list</li> </ol>
  * </p>
  *
  * <p>Find more details on the <a
  * href="http://github.com/cardillo/storch">github</a> project page.</p>
  *
  * @param <
  *   V> the type of values in this data frame
  */
object DataFrame {

  private val logger = LoggerFactory.getLogger(this.getClass)
  def fromSeq(
      data: Seq[Seq[?]],
      colNames: Seq[String],
      transpose: Boolean = true,
  ): DataFrame[?] = {
    val rows = data.size
    val cols = data.head.size
    val df: DataFrame[Any] =
      if transpose then
        new DataFrame[Any](
          colNames,
          (0 until rows).map(_.toString).toSeq,
          data.map(_.toSeq).toList,
        )
      else
        new DataFrame[Any](
          (0 until rows).map(_.toString).toSeq,
          colNames,
          data.map(_.toSeq).toList,
        )
    df
  }

  def fromNumpyNDArray[V](
      array: NDArray[V],
      transpose: Boolean = true,
  ): DataFrame[V] = {
    require(array.getNdim == 2, "Only 2D arrays are supported")
    val data: Array[Array[V]] = array.getArray.asInstanceOf[Array[Array[V]]]
    val rows = data.length
    val shape = array.getShape
    val cols = shape(1)
    val df = new DataFrame[V](
      (0 until rows).map(_.toString).toSeq,
      (0 until cols).map(_.toString).toSeq,
      data.transpose.map(_.toSeq).toList,
    )
    if transpose then return df.transpose else return df
  }

  def toNumpyNDArray[V: ClassTag](
      df: DataFrame[V],
      fillValue: Double = Double.NaN,
  ): NDArray[V] = {
    val data = df.toModelMatrixDataFrame.toModelMatrix(fillValue) // .toModelMatrix()
    //    val rows = data.size
    //    val cols = data.head.size
    //    val array = new NDArray[V](rows, cols)
    //    for (i <- 0 until rows) {
    //      for (j <- 0 until cols) {
    //      }}  //[Double,V]
    val ndArray: NDArray[V] = TorchNumpy.array(data)
    ndArray
  }

  def toNumpyNpyFile[V: ClassTag](df: DataFrame[V], file: String): Unit = {
    val array: NDArray[V] = toNumpyNDArray(df)
//    NpyFile.write(array, file)
    TorchNumpy.saveNpyFile[V](file, array)
  }

  def fromNumpyNpyFile[V: ClassTag](file: String): DataFrame[V] = {
    val array: NDArray[V] = TorchNumpy.loadNDArray(file)
    fromNumpyNDArray(array)
  }

  def readNumpyNpyFile[V: ClassTag](file: String): DataFrame[V] =
    fromNumpyNpyFile(file)

  def readNumpyNpzFile[V: ClassTag](file: String): Seq[DataFrame[?]] =
    fromNumpyNpzFile(file)

  def toNumpyNpzFile[V: ClassTag](
      dfSeq: Seq[DataFrame[V]],
      file: String,
  ): Unit = {
    val array: Seq[NDArray[V]] = dfSeq.map(df => toNumpyNDArray(df))
    //    NpyFile.write(array, file)
    TorchNumpy.saveNpzFile(file, array)
  }

  def fromNumpyNpzFile[V: ClassTag](file: String): Seq[DataFrame[?]] = {
    val ndArray: Seq[NDArray[?]] = TorchNumpy.loadNpzFile(file)
    ndArray.map(array => fromNumpyNDArray(array))
  }

  def fromNumpyCSVTextFile[V: ClassTag](
      file: String,
      shape: Seq[Int],
      header: Boolean = false,
      dtype: DType = DType.Float32,
  ): DataFrame[V] = {
    val array: NDArray[V] = TorchNumpy
      .loadNDArrayFromCSV(file, shape, header, dtype)
    fromNumpyNDArray(array)
  }

  def toNumpyCSVTextFile[V: ClassTag](
      df: DataFrame[V],
      file: String,
      is2dim: Boolean = false,
      rowFirst: Boolean = true,
  ): Unit = {
    val array: NDArray[V] = toNumpyNDArray(df)
    TorchNumpy.saveNDArrayToCSV(array, file, is2dim, rowFirst)
  }

  /** 将两个 DataFrame 分割为训练集和测试集，分别返回特征和标签的训练集、测试集。
    *
    * @param featureDf
    *   包含所有特征列的 DataFrame
    * @param labelDf
    *   仅包含标签列的 DataFrame
    * @param testSize
    *   测试集所占比例，范围在 0 到 1 之间
    * @param randomState
    *   随机种子，用于复现分割结果
    * @tparam V
    *   DataFrame 中值的类型
    * @return
    *   包含训练集特征、测试集特征、训练集标签、测试集标签的四元组
    */
  def train_test_split[V](
      featureDf: DataFrame[V],
      labelDf: DataFrame[V],
      testSize: Double = 0.2,
      randomState: Int = 42,
  ): (DataFrame[V], DataFrame[V], DataFrame[V], DataFrame[V]) = {
    require(testSize > 0 && testSize < 1, "test dataset ratio must set  0 ~ 1 ")
    require(
      featureDf.length == labelDf.length,
      "featureDf and labelDf must have the same number of rows",
    )
    // 设置随机种子
    scala.util.Random.setSeed(randomState)
    // 打乱索引
    val shuffledIndices = scala.util.Random.shuffle(0 until featureDf.length)
    // 计算分割点
    val splitIndex = (featureDf.length * (1 - testSize)).toInt
    // 分割索引
    val trainIndices = shuffledIndices.take(splitIndex)
    val testIndices = shuffledIndices.drop(splitIndex)

    // 提取训练集和测试集的特征和标签
    val X_train = featureDf.filterRows(trainIndices)
    val X_test = featureDf.filterRows(testIndices)
    val y_train = labelDf.filterRows(trainIndices)
    val y_test = labelDf.filterRows(testIndices)

    (X_train, X_test, y_train, y_test)
  }

  /** * 将case class转换为DataFrame
    *
    * @param records
    * @param transpose
    * @param tag
    * @tparam T
    * @return
    */
  def fromCaseClassSeq[T](records: Seq[T], transpose: Boolean = true)(implicit
      tag: ClassTag[T],
  ): DataFrame[T] = {
    val fieldNames = tag.runtimeClass.getDeclaredFields.map(_.getName).toSeq
    val rows = records.map(record =>
      fieldNames
        .map(fieldName => record.getClass.getMethod(fieldName).invoke(record)),
    )
    val rowIndex = (0 until fieldNames.size).map(_.toString).toSeq // rows.size
    val df =
      if transpose then
        new DataFrame[T](
          fieldNames.asInstanceOf[Seq[String]],
          rowIndex,
          rows.asInstanceOf[List[Seq[T]]],
        ).transpose
      else
        new DataFrame[T](
          rowIndex,
          fieldNames.asInstanceOf[Seq[String]],
          rows.asInstanceOf[List[Seq[T]]],
        )
    df
  }

  def decompressorGzip(
      gzipFilePath: String,
      outputFilePath: String,
      readCacheFactor: Int = 100,
  ): Unit = GzipDecompressor
    .decompressGzip(gzipFilePath, outputFilePath, readCacheFactor)

  def decompressorZip(
      zipFilePath: String,
      outputDirectory: String,
      readCacheFactor: Int = 100,
  ): Unit = ZipDecompressor
    .decompressZip(zipFilePath, outputDirectory, readCacheFactor)

  def parseJsonToCaseClassSeq[T](jsonPath: String, func: ujson.Value => T)(
      implicit tag: ClassTag[T],
  ): Seq[T] = JsonCompat.parseJsonFileToCaseClassSeq(jsonPath, func)

  def parseJsonToColumnMap(jsonPath: String): Map[String, Array[AnyRef]] =
    JsonCompat.parseJsonFileToColumnar(jsonPath)

  def parseJsonToCSVFile(
      jsonPath: String,
      csvPath: String,
      spliterator: String = ",",
  ): Unit = JsonCompat.parseJsonFileToCSVFile(jsonPath, csvPath, spliterator)

  def readCSV(file: String, limit: Int = -1): DataFrame[AnyRef] = CSVCompat
    .readCSV(file, limit)

  def readJson(
      jsonPath: String,
      isJsonLine: Boolean = false,
      recursionHeader: Boolean = false,
  ): DataFrame[AnyRef] =
    if !isJsonLine then JsonCompat.parseJsonFileToDataFrame(jsonPath)
    else readJsonLine(jsonPath, recursionHeader)

  def readJsonLine(
      jsonPath: String,
      recursionHeader: Boolean = false,
      limit: Int = -1,
  ): DataFrame[AnyRef] = JsonCompat
    .parseJsonLineToDataFrame(jsonPath, recursionHeader, limit)

  def readXlsx(xlsxPath: String): DataFrame[AnyRef] = XlsxCompat
    .readXlsx(xlsxPath)

  def readPickle[T: ClassTag](picklePath: String): DataFrame[T] = {
    val caseSeq = PickleCompat.readPickleForCaseClassSeq[T](picklePath)
    fromCaseClassSeq(caseSeq)
  }

  def readPickleForMap(picklePath: String): mutable.HashMap[String, AnyRef] = {
    val map = PickleCompat.readPickleForMap(picklePath)
    map
  }

  def readPickleForNumpy(picklePath: String): MulitNumpyNdArray = PickleCompat
    .readPickleForNumpy(picklePath)

  /** Reads an Apache Avro file. */
  def readIPC(file: String): DataFrame[AnyRef] = PolarsCompat.readIPC(file)

  /** Reads an Apache Parquet file. */
  def readParquet(file: String): DataFrame[AnyRef] = PolarsCompat
    .readParquet(file)

  def readJsonLinePolars(file: String): DataFrame[AnyRef] = PolarsCompat
    .readJsonLine(file)
//
//  /** Reads an ARFF file. */
//  def readArff(file: Path): DataFrame = Read.arff(file)
//
//  /** Reads a SAS7BDAT file. */
//  def readSas(file: String): DataFrame = Read.sas(file)
//
//  /** Reads a SAS7BDAT file. */
//  def readSas(file: Path): DataFrame = Read.sas(file)
//
//  /** Reads an Apache Arrow file. */
//  def readArrow(file: String): DataFrame = Read.arrow(file)
//
//  /** Reads an Apache Arrow file. */
//  def readArrow(file: Path): DataFrame = Read.arrow(file)

  /** Reads an Apache Avro file. */
//  def readAvro(file: String, schema: InputStream): DataFrame = Read.avro(file, schema)
//
//  /** Reads an Apache Avro file. */
//  def readAvro(file: String, schema: String): DataFrame = Read.avro(file, schema)

  /** Reads an Apache Avro file. */
//  def readAvro(file: String, schema: InputStream): DataFrame[AnyRef] = PolarsCompat.readAvro(file, schema)

  /** Reads an Apache Parquet file. */
//  def readParquet(file: Path): DataFrame = Read.parquet(file)

  /** Reads a LivSVM file. */
//  def readLibsvm(file: String): SparseDataset[Integer] = Read.libsvm(file)
//
//  /** Reads a LivSVM file. */
//  def readLibsvm(file: Path): SparseDataset[Integer] = Read.libsvm(file)
//
//

  def compare[V](df1: DataFrame[V], df2: DataFrame[V]): DataFrame[String] =
    Comparison.compare(df1, df2)

  /** Read the specified csv file and return the data as a data frame.
    *
    * @param file
    *   the csv file
    * @return
    *   a new data frame
    * @throws IOException
    *   if an error reading the file occurs
    */
  @throws[IOException]
  def readCsv(file: String, limit: Int = -1): DataFrame[AnyRef] = Serialization
    .readCsv(file, limit = limit)

  /** Read csv records from an input stream and return the data as a data frame.
    *
    * @param input
    *   the input stream
    * @return
    *   a new data frame
    * @throws IOException
    *   if an error reading the stream occurs
    */
  @throws[IOException]
  def readCsv(input: InputStream, limit: Int): DataFrame[AnyRef] = Serialization
    .readCsv(input, limit = limit)

  @throws[IOException]
  def readCsv(file: String, separator: String, limit: Int): DataFrame[AnyRef] =
    Serialization
      .readCsv(file, separator, NumberDefault.LONG_DEFAULT, limit = limit)

  @throws[IOException]
  def readCsv(
      input: InputStream,
      separator: String,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization
    .readCsv(input, separator, NumberDefault.LONG_DEFAULT, null, limit = limit)

  @throws[IOException]
  def readCsv(
      input: InputStream,
      separator: String,
      naString: String,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization.readCsv(
    input,
    separator,
    NumberDefault.LONG_DEFAULT,
    naString,
    limit = limit,
  )

  @throws[IOException]
  def readCsv(
      input: InputStream,
      separator: String,
      naString: String,
      hasHeader: Boolean,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization.readCsv(
    input,
    separator,
    NumberDefault.LONG_DEFAULT,
    naString,
    hasHeader,
    limit = limit,
  )

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      naString: String,
      hasHeader: Boolean,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization.readCsv(
    file,
    separator,
    NumberDefault.LONG_DEFAULT,
    naString,
    hasHeader,
    limit = limit,
  )

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      numberDefault: DataFrame.NumberDefault,
      naString: String,
      hasHeader: Boolean,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization
    .readCsv(file, separator, numberDefault, naString, hasHeader, limit = limit)

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      longDefault: DataFrame.NumberDefault,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization
    .readCsv(file, separator, longDefault, limit = limit)

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      longDefault: DataFrame.NumberDefault,
      naString: String,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization
    .readCsv(file, separator, longDefault, naString, limit = limit)

  @throws[IOException]
  def readCsv(
      input: InputStream,
      separator: String,
      longDefault: DataFrame.NumberDefault,
      limit: Int,
  ): DataFrame[AnyRef] = Serialization
    .readCsv(input, separator, longDefault, null, limit = limit)

  /** Read data from the specified excel workbook into a new data frame.
    *
    * @param file
    *   the excel workbook
    * @return
    *   a new data frame
    * @throws IOException
    *   if an error occurs reading the workbook
    */
  @throws[IOException]
  def readXls(file: String): DataFrame[AnyRef] = Serialization.readXls(file)

  /** Read data from the input stream as an excel workbook into a new data
    * frame.
    *
    * @param input
    *   the input stream
    * @return
    *   a new data frame
    * @throws IOException
    *   if an error occurs reading the input stream
    */
  @throws[IOException]
  def readXls(input: InputStream): DataFrame[AnyRef] = Serialization
    .readXls(input)

  /** Execute the SQL query and return the results as a new data frame.
    *
    * <pre> {@code > Connection c =
    * DriverManager.getConnection("jdbc:derby:memory:testdb;create=true"); >
    * c.createStatement().executeUpdate("create table data (a varchar(8), b
    * int)"); > c.createStatement().executeUpdate("insert into data values
    * ('test', 1)"); > DataFrame.readSql(c, "select * from data").flatten();
    * [test, 1] }</pre>
    *
    * @param c
    *   the database connection
    * @param sql
    *   the SQL query
    * @return
    *   a new data frame
    * @throws SQLException
    *   if an error occurs execution the query
    */
  @throws[SQLException]
  def readSql(c: Connection, sql: String): DataFrame[AnyRef] =
    try {
      val stmt = c.createStatement
      try readSql(stmt.executeQuery(sql))
      finally if (stmt != null) stmt.close()
    }

  /** Read data from the provided query results into a new data frame.
    *
    * @param rs
    *   the query results
    * @return
    *   a new data frame
    * @throws SQLException
    *   if an error occurs reading the results
    */
  @throws[SQLException]
  def readSql(rs: ResultSet): DataFrame[AnyRef] = Serialization.readSql(rs)

  /** A function that is applied to objects (rows or values) in a
    * {@linkplain DataFrame data frame}.
    *
    * <p>Implementors define {@link # apply ( Object )} to perform the desired
    * calculation and return the result.</p>
    *
    * @param <
    *   I> the type of the input values
    * @param <
    *   O> the type of the output values
    * @see
    *   DataFrame#apply(Function)
    * @see
    *   DataFrame#aggregate(Aggregate)
    */
  trait Function[I, O] {

    /** Perform computation on the specified input value and return the result.
      *
      * @param value
      *   the input value
      * @return
      *   the result
      */
    def apply(value: I): O
  }

  trait RowFunction[I, O] {
    def apply(values: Seq[I]): Seq[Seq[O]]
  }

  /** A function that converts {@linkplain DataFrame data frame} rows to index
    * or group keys.
    *
    * <p>Implementors define {@link # apply ( Object )} to accept a data frame
    * row as input and return a key value, most commonly used by
    * {@link DataFrame# groupBy ( KeyFunction )}.</p>
    *
    * @param <
    *   I> the type of the input values
    * @see
    *   DataFrame#groupBy(KeyFunction)
    */
  trait KeyFunction[I] extends DataFrame.Function[Seq[I], AnyRef] {}

  /** A function that converts lists of {@linkplain DataFrame data frame} values
    * to aggregate results.
    *
    * <p>Implementors define {@link # apply ( Object )} to accept a list of data
    * frame values as input and return an aggregate result.</p>
    *
    * @param <
    *   I> the type of the input values
    * @param <
    *   O> the type of the result
    * @see
    *   DataFrame#aggregate(Aggregate)
    */
  trait Aggregate[I, O] extends DataFrame.Function[Seq[I], O] {}

  /** An interface used to filter a {@linkplain DataFrame data frame}.
    *
    * <p>Implementors define {@link # apply ( Object )} to return {@code true}
    * for rows that should be included in the filtered data frame.</p>
    *
    * @param <
    *   I> the type of the input values
    * @see
    *   DataFrame#select(Predicate)
    */
  trait Predicate[I] extends DataFrame.Function[Seq[I], Boolean] {}

  enum SortDirection:
    case ASCENDING, DESCENDING

  /** An enumeration of join types for joining data frames together.
    */
  enum JoinType:
    case INNER, OUTER, LEFT, RIGHT

  /** An enumeration of plot types for displaying data frames with charts.
    */
  enum PlotType:
    case SCATTER, SCATTER_WITH_TREND, LINE, LINE_AND_POINTS, AREA, BAR, GRID,
      HEATMAP, GRID_WITH_TREND

  /** An enumeration of data frame axes.
    */
  enum Axis:
    case ROWS, COLUMNS

  enum NumberDefault:
    case LONG_DEFAULT, DOUBLE_DEFAULT

  /** Entry point to storch as a command line tool.
    *
    * The available commands are: <dl> <dt>show</dt><dd>display the specified
    * data frame as a swing table</dd> <dt>plot</dt><dd>display the specified
    * data frame as a chart</dd> <dt>compare</dt><dd>merge the specified data
    * frames and output the result</dd> <dt>shell</dt><dd>launch an interactive
    * javascript shell for exploring data</dd> </dl>
    *
    * @param args
    *   file paths or urls of csv input data
    * @throws IOException
    *   if an error occurs reading input
    */
  @throws[IOException]
  def main(args: Array[String]): Unit = {
    val frames = new ListBuffer[DataFrame[AnyRef]]
    for (i <- 1 until args.length) frames.append(DataFrame.readCsv(args(i)))
    if (args.length > 0 && "plot".equalsIgnoreCase(args(0)))
      if (frames.size == 1) {
        frames(0).plot()
        return
      }
    if (args.length > 0 && "show".equalsIgnoreCase(args(0)))
      if (frames.size == 1) {
        frames(0).show()
        return
      }
    if (args.length > 0 && "compare".equalsIgnoreCase(args(0)))
      if (frames.size == 2) {
        logger.info(s"${DataFrame.compare(frames(0),frames(1))}")
        return
      }
    if (args.length > 0 && "shell".equalsIgnoreCase(args(0))) {
      Shell.repl(frames.toList)
      return
    }
    logger.error(
      "usage: %s [compare|plot|show|shell] [csv-file ...]\n",
      classOf[DataFrame[?]].getCanonicalName,
    )
    System.exit(255)
  }
}

class DataFrame[V](
    var index: Index = new Index(mutable.Set.empty, 0),
    var columns: Index = new Index(mutable.Set.empty, 0),
    var data: BlockManager[V] = new BlockManager[V](List.empty),
    var groups: Grouping[V] = new Grouping[V](),
) extends Iterable[Seq[V]] {

  /** Construct an empty data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>(); > df.isEmpty();
    * true }</pre>
    */

//  this (Seq[ Seq[V]])
//  final private var index: Index = new Index(mutable.Set.empty, 0)
//  final private var columns: Index =  new Index(mutable.Set.empty, 0)
//  final private var data: operate.BlockManager[V] = new operate.BlockManager[V](List.empty)
//  final private var groups: Grouping[V] = new Grouping()

  /** Construct a new data frame using the specified data and indices.
    *
    * @param index
    *   the row names
    * @param columns
    *   the column names
    * @param data
    *   the data
    */
  def this() = this(
    new Index(mutable.Set.empty, 0),
    new Index(mutable.Set.empty, 0),
    new BlockManager[V](List.empty),
    new Grouping(),
  )

  def this(columns: String*) = this(
    new Index(mutable.Set.empty, 0),
    new Index(mutable.Set(columns*), columns.length),
    new BlockManager[V](List.empty),
    new Grouping(),
  )

//  def this(columns: Seq[Any]) = this(
//    new Index(mutable.Set.empty, 0),
//    new Index(columns, columns.size),
//    new BlockManager[V](List.empty),
//    new Grouping(),
//  )
// columns: mutable.Seq[Any]
  def this(index: Seq[AnyRef], columns: Seq[AnyRef]) = this(
    new Index(index, index.size),
    new Index(columns, columns.size),
    new BlockManager[V](List.empty),
    new Grouping(),
  )

  def this(data: List[Seq[V]]) = this(
    new Index(mutable.Set.empty, 0),
    new Index(mutable.Set.empty, 0),
    new BlockManager[V](data),
    new Grouping(),
  )

  def this(
      index: Seq[AnyRef],
      columns: Seq[AnyRef], // mutable.Seq[Any],
      data: List[Seq[V]],
  ) = {
    this()
    val mgr = new BlockManager[V](data)
    mgr.reshape(
      math.max(mgr.size(), columns.size),
      math.max(mgr.length(), index.size),
    )
    this.data = mgr
    this.columns = new Index(columns, mgr.size())
    this.index = new Index(index, mgr.length())
  }

  def resetColumns = {
    val colNums = this.getColumns.zipWithIndex
      .map(ele => ele._2.asInstanceOf[AnyRef])
    this.columns = new Index(colNums, colNums.size)
    this
  }
  def toNumpyNDArray[V1: ClassTag](fillValue: Double = Double.NaN): NDArray[?] = {
    val tag = DType.fromClassTag(implicitly[ClassTag[V1]])
    val castDf = this.resetColumns.cast(implicitly[ClassTag[V1]].runtimeClass)
//    val castDf = this.resetColumns.cast(implicitly[ClassTag[Double]].runtimeClass)
    val dataFrame = castDf.toModelMatrixDataFrame
    logger.info(
      s"Dataframe toNumpyNDArray, invoke from toModelMatrixDataFrame get dataframe: columns : ${dataFrame
          .getColumns.mkString(", ")} ,column size : ${dataFrame.getColumns
          .size}  shape: ${dataFrame.getShape}",
    )
    val data: Array[Array[V1]] = dataFrame.toModelMatrix(fillValue) // .toModelMatrix()

    val data2 = this.toModelMatrix(fillValue)
    logger.info(s"data2 ${data2.getClass.getName}, data2 size ${data2
        .length} tag  ${data2.mkString(", ")} ")
    logger.info(s"data ${data.getClass.getName} , data2 size ${data2
        .length} tag ${data.mkString(", ")} ")
    val rows = this.getShape._1 // data.size
    val cols = this.getShape._2 // data.head.size
    val flattenedData = data.flatMap {
      case arr: Array[V1] => arr
//      case seq: Seq[V1] => seq.toArray
      case single: V1 => Array(single)
      case ss: Array[AnyRef] =>
        logger.error(s"error case ${data.getClass.getName}")
        data.flatten.map { ele =>
          logger.error(s"ele ${ele.getClass.getName} $ele")
          ele.toString.asInstanceOf[V1]
        }.toArray
      case other =>
        logger.error(s"error case se ${data.getClass.getName}")
        try other.asInstanceOf[Array[V1]]
        catch {
          case _: ClassCastException =>
            logger.error(s"Failed to cast $other to Array[V1]. Using empty array.")
            Array.empty[V1]
        }
//        data.flatten.map(ele => {
//          println(s"ele ${ele.getClass.getName} ${ele}")
//          ele.toString.asInstanceOf[V1]
//        }).toArray
    }.map(elem =>
      try elem.asInstanceOf[V1] // .toString.toDouble
      catch {
        case _: NumberFormatException =>
          logger.error(s"Failed to convert $elem to Double. Using fillValue $fillValue instead.")
          fillValue.asInstanceOf[V1]
      },
    )
    // .map { elem =>
//      try {
//        elem.toString.toDouble
//      } catch {
//        case _: NumberFormatException =>
//          println(s"Failed to convert $elem to Double. Using fillValue $fillValue instead.")
//          fillValue
//      }
//    }
    logger.info(s" flattenedData ${flattenedData.getClass.getComponentType
        .getName} ${flattenedData.mkString(", ")}")

    val ndArray: NDArray[?] = TorchNumpy
      .array(flattenedData, Array(rows, cols), 2, tag)
    ndArray.reshape(rows, cols)
  }

  //    val array = new NDArray[V](rows, cols)
  //    for (i <- 0 until rows) {
  //      for (j <- 0 until cols) {
  //      }}  //[Double,V]

  def values[T: ClassTag](isAllBoolean: Boolean = false) = {

    val numDf = if isAllBoolean then this else this.numeric
    if (numDf.getColumns.size == 0) {
      logger.error(s"DataFrame must contain at least one numeric column.")
      throw new IllegalArgumentException(
      "DataFrame must contain at least one numeric column.",
    )
    } else numDf.transpose.toNumpyNDArray[T]()
      .reshape(numDf.getShape._1, numDf.getShape._2)

  }

  /** 获取 DataFrame 的行数和列数。
    *
    * @return
    *   包含行数和列数的元组，格式为 (行数, 列数)
    */
  def getShape: (Int, Int) = {
    val rows = data.length()
    val cols = data.size()
    (rows, cols)
  }

  /** 将 DataFrame 分割为训练集和测试集，同时分离特征和标签。
    *
    * @param labelCol
    *   标签列的名称
    * @param testSize
    *   测试集所占比例，范围在 0 到 1 之间
    * @param randomState
    *   随机种子，用于复现分割结果
    * @return
    *   包含训练集特征、测试集特征、训练集标签、测试集标签的四元组
    */
  def train_test_split(
      labelCol: AnyRef,
      testSize: Double = 0.2,
      randomState: Int = 42,
  ): (DataFrame[V], DataFrame[V], DataFrame[V], DataFrame[V]) = {
    require(testSize > 0 && testSize < 1, "test dataset ratio must set  0 ~ 1 ")
    require(
      this.getColumns.contains(labelCol),
      s"标签列 $labelCol 不存在于 DataFrame 中",
    )

    // 设置随机种子
    scala.util.Random.setSeed(randomState)
    // 打乱索引
    val shuffledIndices = scala.util.Random.shuffle(0 until this.length)
    // 计算分割点
    val splitIndex = (this.length * (1 - testSize)).toInt
    // 分割索引
    val trainIndices = shuffledIndices.take(splitIndex)
    val testIndices = shuffledIndices.drop(splitIndex)

    // 获取特征列名
    val featureCols = columns.names.filterNot(_ == labelCol)

    // 提取训练集和测试集的特征和标签
    val X_train = this.retain(featureCols.map(_.toString))
      .filterRows(trainIndices)
    val X_test = this.retain(featureCols.map(_.toString)).filterRows(testIndices)
    val y_train = this.retain(Seq(labelCol.toString)).filterRows(trainIndices)
    val y_test = this.retain(Seq(labelCol.toString)).filterRows(testIndices)

    (X_train, X_test, y_train, y_test)
  }

  /** 根据给定的行索引过滤 DataFrame。
    *
    * @param indices
    *   要保留的行索引序列
    * @return
    *   过滤后的 DataFrame
    */
  private def filterRows(indices: Seq[Int]): DataFrame[V] = {
    val newData = indices.map(i => this.data.getRow(i)).toList
    new DataFrame[V](
      index = new Index(indices.map(index.names(_)), indices.size),
      columns = columns,
      data = new BlockManager[V](newData.transpose),
      groups = groups,
    )
  }

  def writeParquet(dataFrame: DataFrame[AnyRef], outPath: String): Unit =
    PolarsCompat.writeParquet(dataFrame, outPath)

  def writeAvro(dataFrame: DataFrame[AnyRef], outPath: String): Unit =
    PolarsCompat.writeAvro(dataFrame, outPath)

  def writeJsonLine(dataFrame: DataFrame[AnyRef], outPath: String): Unit =
    PolarsCompat.writeJsonLine(dataFrame, outPath)

  def writeIPC(dataFrame: DataFrame[AnyRef], outPath: String): Unit =
    PolarsCompat.writeIPC(dataFrame, outPath)

//  def this(index: Index, columns: Index, data: BlockManager[V], groups: Grouping[V]) = this(index, columns, data, groups)

//  def this(index: Seq[?], columns: Seq[?], data:  Seq[? <:  Seq[? <: V]])={
//
//  }

  /** Construct a data frame from the specified list of columns.
    *
    * <pre> {@code > List<List<Object>> data = Arrays.asList( >
    * Arrays.<Object>asList("alpha", "bravo", "charlie"), >
    * Arrays.<Object>asList(1, 2, 3) > ); > DataFrame<Object> df = new
    * DataFrame<>(data); > df.row(0); [alpha, 1] }</pre>
    *
    * @param data
    *   a list of columns containing the data elements.
    */
//  def this(data:  Seq[? <:  Seq[? <: V]]) = {
//    this(Seq.empty, Seq.empty, data)
//  }

  /** Construct an empty data frame with the specified columns.
    *
    * <pre> {@code > List<String> columns = new ArrayList<>(); >
    * columns.add("name"); > columns.add("value"); > DataFrame<Object> df = new
    * DataFrame<>(columns); > df.columns(); [name, value] }</pre>
    *
    * @param columns
    *   the data frame column names.
    */
//  def this(columns: Seq[?]) ={
//    this(Seq.empty, columns, Seq[ Seq[V]])
//  }

  /** Construct an empty data frame with the specified columns.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.columns(); [name, value] }</pre>
    *
    * @param columns
    *   the data frame column names.
    */
//  def this(columns: String*) ={
  //    this(java.util.Arrays.asList(columns.asInstanceOf[Array[AnyRef]]))
  //  }

  /** Construct a data frame containing the specified rows and columns.
    *
    * <pre> {@code > List<String> rows = Arrays.asList("row1", "row2", "row3");
    * > List<String> columns = Arrays.asList("col1", "col2"); >
    * DataFrame<Object> df = new DataFrame<>(rows, columns); > df.get("row1",
    * "col1"); null }</pre>
    *
    * @param index
    *   the row names
    * @param columns
    *   the column names
    */
//  def this(index: Seq[?], columns: Seq[?]) ={
//    this(index, columns, Seq[ Seq[V]])
//  }

//  def this(index: Index, columns: Index, data: BlockManager[V], groups: Grouping) ={
//    this()
//    this.index = index
//    this.columns = columns
//    this.data = data
//    this.groups = groups
//  }

  /** * 将case class转换为DataFrame
    * @param records
    * @param transpose
    * @param tag
    * @tparam T
    * @return
    */
  def caseClassSeqToDataFrame[T](records: Seq[T], transpose: Boolean = true)(
      implicit tag: ClassTag[T],
  ): DataFrame[T] = {
    val fieldNames = tag.runtimeClass.getDeclaredFields.map(_.getName).toSeq
    val rows = records.map(record =>
      fieldNames
        .map(fieldName => record.getClass.getMethod(fieldName).invoke(record)),
    )
    val rowIndex = (0 until fieldNames.size).map(_.toString).toSeq // rows.size
    val df = new DataFrame[T](
      rowIndex,
      fieldNames.asInstanceOf[Seq[String]],
      rows.asInstanceOf[List[Seq[T]]],
    )
    if transpose then return df.transpose else return df
  }

  /** *
    *
    * @param args
    * @param tag
    * @tparam T
    * @return
    */
  def tagClassToInstance[T](args: List[Any])(implicit tag: ClassTag[T]): T = {
    val constructor = tag.runtimeClass.getConstructors.head
    //    val args = constructor.getParameterTypes.map(_ => null)
    constructor.newInstance(args*).asInstanceOf[T]
  }

  /** *
    *
    * @param df
    * @param transpose
    * @param tag
    * @tparam T
    * @return
    */
  def dataFrameToCaseClass[T](
      transpose: Boolean = false,
  )(implicit tag: ClassTag[T]): Seq[T] = {
    // 获取 case class 的字段名
    val fieldNames = tag.runtimeClass.getDeclaredFields.map(_.getName).toSeq
    // 获取 DataFrame 的数据行
    val rowValueSeq =
      if transpose then this.transpose.iterrows else this.iterrows
    val records = rowValueSeq.map(row =>
      // 获取 case class 的构造函数
      tagClassToInstance(row),
    )
    records.toSeq
  }

  /** Add new columns to the data frame.
    *
    * Any existing rows will have {@code null} values for the new columns.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>(); >
    * df.add("value"); > df.columns(); [value] }</pre>
    *
    * @param columns
    *   the new column names
    * @return
    *   the data frame with the columns added
    */
  def add(columns: AnyRef*): DataFrame[V] = {
    columns.foreach { column =>
      val values = List.fill(length)(null.asInstanceOf[V])
      add(column, values)
    }
    this
  }

//  def add(columns: AnyRef*): DataFrame[V] = {
//    for (column <- columns) {
//      val values = new  ListBuffer[V](length)
//      for (r <- 0 until values.size) {
//        values.add(null)
//      }
//      add(column, values)
//    }
//    this
//  }

  /** Add the list of values as a new column.
    *
    * @param values
    *   the new column values
    * @return
    *   the data frame with the column added
    */
//  def add(values:  Seq[V]): DataFrame[V] = add(length, values)

  /** Add a new column to the data frame containing the value provided.
    *
    * Any existing rows with indices greater than the size of the specified
    * column data will have {@code null} values for the new column.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>(); > df.add("value",
    * Arrays.<Object>asList(1)); > df.columns(); [value] }</pre>
    *
    * @param column
    *   the new column names
    * @param values
    *   the new column values
    * @return
    *   the data frame with the column added
    */
  def add(column: AnyRef, values: Seq[V]): DataFrame[V] = {
    columns.add(column, data.size())
    index.extend(values.size)
    data.add(values.toBuffer)
    this
  }

  def addColumn(column: AnyRef, values: Seq[V]): DataFrame[V] = {
    columns.add(column, data.size())
    index.extend(values.size)
    data.add(values.map(_.asInstanceOf[V]).toBuffer)
    this
  }

  /** Add the results of applying a row-wise function to the data frame as a new
    * column.
    *
    * @param column
    *   the new column name
    * @param function
    *   the function to compute the new column values
    * @return
    *   the data frame with the column added
    */
  def add(column: AnyRef, function: List[V] => V): DataFrame[V] = {
    val values = this.map(function).toList
    add(column, values)
  }
  def add(
      column: AnyRef,
      function: DataFrame.Function[Seq[V], V],
  ): DataFrame[V] = {
    val values = new ListBuffer[V]

    for (row <- this) values.append(function.apply(row))
    add(column, values)
  }

  /** Create a new data frame by leaving out the specified columns.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value",
    * "category"); > df.drop("category").columns(); [name, value] }</pre>
    *
    * @param cols
    *   the names of columns to be removed
    * @return
    *   a shallow copy of the data frame with the columns removed
    */
  def drop(cols: AnyRef*): DataFrame[V] = {
    logger.info(s"Class DataFrame will drop cols: ->  ${cols.mkString(", ")}")
    val dropCols = columns.indices(cols)
    dropWithIntCols(dropCols, indet = true)
  }

  def dropWithIntCols(cols: Seq[Int], indet: Boolean = true): DataFrame[V] = {
    val colNames = columns.names.toList
    val toDrop = cols.toSeq.map(colNames(_))
    logger.info("dataframe.drop cols name : " + toDrop.mkString(", "))
    val newColNames = colNames.filterNot(toDrop.contains)
    val keep = newColNames.map(ele => col(ele.toString)) // .map(_.asScala.toSeq)
    new DataFrame[V](index.names, Seq(newColNames*), keep) // mutable.Seq(newColNames*)
  }

  /** Create a new data frame by leaving out the specified columns.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value",
    * "category"); > df.drop(2).columns(); [name, value] }</pre>
    *
    * @param cols
    *   the indices of the columns to be removed
    * @return
    *   a shallow copy of the data frame with the columns removed
    */
  def drop(cols: Seq[Int], indet: Boolean = true): DataFrame[V] = {
    val colNames = columns.names.toList
    val toDrop = cols.toSeq.map(colNames(_))
//    println("dataframe.drop cols name : " + toDrop.mkString(", "))
    val newColNames = colNames.filterNot(toDrop.contains)
    val keep = newColNames.map(ele => col(ele.toString)) // .map(_.asScala.toSeq)
    new DataFrame[V](index.names, Seq(newColNames*), keep) // mutable.Seq(newColNames*)
  }
//  def drop(cols: Int*): DataFrame[V] = {
//    val colnames = new  ListBuffer[AnyRef](columns.names)
//    val todrop = new  ListBuffer[AnyRef](cols.length)
//    for (col <- cols) {
//      todrop.add(colnames.get(col))
//    }
//    colnames.removeAll(todrop)
//    val keep = new  ListBuffer[ Seq[V]](colnames.size)
//
//    for (col <- colnames) {
//      keep.add(col(col))
//    }
//    new DataFrame[V](index.names, colnames, keep)
//  }

  def dropna: DataFrame[V] = dropna(DataFrame.Axis.ROWS)

  def dropna(direction: DataFrame.Axis): DataFrame[V] = direction match {
    case ROWS => select(new Selection.DropNaPredicate[V])
    case _ => transpose.select(new Selection.DropNaPredicate[V]).transpose
  }

//  def dropna(direction: Axis): DataFrame[V] = {
//    direction match {
//      case Axis.ROWS => select(row => true) // ???????????
//      case _ => ???
//    }
//  }

  /** Returns a view of the of data frame with NA's replaced with {@code fill}.
    *
    * @param fill
    *   the value used to replace missing values
    * @return
    *   the new data frame
    */
  def fillna(fill: V): DataFrame[V] = apply(new Views.FillNaFunction[V](fill))

//  def fillna(fill: V): DataFrame[V] = apply(row => fill)

  /** Create a new data frame containing only the specified columns.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value",
    * "category"); > df.retain("name", "category").columns(); [name, category]
    * }</pre>
    *
    * @param cols
    *   the columns to include in the new data frame
    * @return
    *   a new data frame containing only the specified columns name
    *   '[Ljava.lang.String;@3c6d2f1' not in index indexMap: (category,0),(name,
    */
  def retain(cols: Seq[String]): DataFrame[V] = {
    logger.info(s"dataframe retain cols ${cols.length} cols ${cols.mkString(",")}")
    val indicesIndex = columns.indices(cols).toSeq
    retains(indicesIndex)
  }

//  def retain(cols: Any*): DataFrame[V] = retain(columns.indices(cols))

  /** Create a new data frame containing only the specified columns.
    *
    * <pre> {@code DataFrame<Object> df = new DataFrame<>("name", "value",
    * "category"); df.retain(0, 2).columns(); [name, category] }</pre>
    *
    * @param cols
    *   the columns to include in the new data frame
    * @return
    *   a new data frame containing only the specified columns
    */
  def retains(cols: Seq[Int]): DataFrame[V] = {
    val keep = cols.toSet
    val toDrop = (0 until size).filterNot(keep.contains)
    logger.info(s"dataframe retain keep ${keep.mkString(",")} toDrop ${toDrop
        .length} cols ${cols.mkString(",")}")
    dropWithIntCols(toDrop.toSeq)
  }
//  def retain(cols: Int*): DataFrame[V] = {
//    val keep = new HashSet[Int](java.util.Arrays.asList(cols))
//    val todrop = new Array[Int](size - keep.size)
//    var i = 0
//    var c = 0
//    while (c < size) {
//      if (!keep.contains(c)) todrop({
//        i += 1; i - 1
//      }) = c
//      c += 1
//    }
//    drop(todrop)
//  }

  /** Re-index the rows of the data frame using the specified column index,
    * optionally dropping the column from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two"); >
    * df.append("a", Arrays.asList("alpha", 1)); > df.append("b",
    * Arrays.asList("bravo", 2)); > df.reindex(0, true) > .index(); [alpha,
    * bravo] }</pre>
    *
    * @param col
    *   the column to use as the new index
    * @param drop
    *   true to remove the index column from the data, false otherwise
    * @return
    *   a new data frame with index specified
    */
  def reindex(col: Int, drop: Boolean): DataFrame[V] = {
    val df = Index.reindex(this, col)
    if (drop) df.drop(Seq(col)) else df
  }

  /** Re-index the rows of the data frame using the specified column indices,
    * optionally dropping the columns from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two",
    * "three"); > df.append("a", Arrays.asList("alpha", 1, 10)); >
    * df.append("b", Arrays.asList("bravo", 2, 20)); > df.reindex(new Int[] { 0,
    * 1 }, true) > .index(); [[alpha, 1], [bravo, 2]] }</pre>
    *
    * @param cols
    *   the column to use as the new index
    * @param drop
    *   true to remove the index column from the data, false otherwise
    * @return
    *   a new data frame with index specified
    */
  def reindex(cols: Array[Int], drop: Boolean): DataFrame[V] = {
    logger.info(s"dataframe reindex inner Array[Int] cols ${cols.mkString(",")}")
    val df = Index.reindex(this, cols*)
    if (drop) df.drop(cols.toSeq) else df
  }

  /** Re-index the rows of the data frame using the specified column indices and
    * dropping the columns from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two"); >
    * df.append("a", Arrays.asList("alpha", 1)); > df.append("b",
    * Arrays.asList("bravo", 2)); > df.reindex(0) > .index(); [alpha, bravo]
    * }</pre>
    *
    * @param cols
    *   the column to use as the new index
    * @return
    *   a new data frame with index specified
    */
//  def reindex(cols: Int*): DataFrame[V] = reindex(cols, true)

  /** Re-index the rows of the data frame using the specified column name,
    * optionally dropping the row from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two"); >
    * df.append("a", Arrays.asList("alpha", 1)); > df.append("b",
    * Arrays.asList("bravo", 2)); > df.reindex("one", true) > .index(); [alpha,
    * bravo] }</pre>
    *
    * @param col
    *   the column to use as the new index
    * @param drop
    *   true to remove the index column from the data, false otherwise
    * @return
    *   a new data frame with index specified
    */
  def reindex(col: AnyRef, drop: Boolean): DataFrame[V] =
    reindex(columns.get(col), drop)

  /** Re-index the rows of the data frame using the specified column names,
    * optionally dropping the columns from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two",
    * "three"); > df.append("a", Arrays.asList("alpha", 1, 10)); >
    * df.append("b", Arrays.asList("bravo", 2, 20)); > df.reindex(new String[] {
    * "one", "two" }, true) > .index(); [[alpha, 1], [bravo, 2]] }</pre>
    *
    * @param cols
    *   the column to use as the new index
    * @param drop
    *   true to remove the index column from the data, false otherwise
    * @return
    *   a new data frame with index specified
    */
  def reindex(cols: Array[AnyRef], drop: Boolean = false): DataFrame[V] =
    logger.info(s"dataframe reindex outer cols: Array[AnyRef] -> cols ${cols.toSeq
        .mkString(",")}")
    reindex(columns.indices(cols), drop)

  /** Re-index the rows of the data frame using the specified column names and
    * removing the columns from the data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two"); >
    * df.append("a", Arrays.asList("alpha", 1)); > df.append("b",
    * Arrays.asList("bravo", 2)); > df.reindex("one", true) > .index(); [alpha,
    * bravo] }</pre>
    *
    * @param cols
    *   the column to use as the new index
    * @return
    *   a new data frame with index specified
    */
//  def reindex(cols: AnyRef): DataFrame[V] = {
//    println(s"dataframe reindex outer cols: AnyRef* -> cols ${cols.toSeq.mkString(",")}")
//    val colInts = columns.indices(cols.asInstanceOf[Array[Seq[AnyRef]]].flatten.toSeq)
//    reindex(colInts, true)
//  }

  /** Return a new data frame with the default index, rows names will be reset
    * to the string value of their Int index.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("one", "two"); >
    * df.append("a", Arrays.asList("alpha", 1)); > df.append("b",
    * Arrays.asList("bravo", 2)); > df.resetIndex() > .index(); [0, 1] }</pre>
    *
    * @return
    *   a new data frame with the default index.
    */
  def resetIndex: DataFrame[V] = Index.reset(this)

  def rename(old: AnyRef, name: AnyRef): DataFrame[V] = rename(Map(old -> name))

//  def rename(old: Any, name: Any): DataFrame[V] = rename(mutable.Map(old -> name))

  def rename(names: Map[AnyRef, AnyRef]): DataFrame[V] = {
    columns.rename(names)
    this
  }

//  def append(name: Any, row: Seq[V]): DataFrame[V] = {
//    val len = length()
//    index.add(name, len)
//    columns.extend(row.size)
//    data.reshape(columns.names().size, len + 1)
//    row.zipWithIndex.foreach { case (value, c) =>
//      data.set(value, c, len)
//    }
//    this
//  }
  def append(name: AnyRef, row: Array[V]): DataFrame[V] = append(name, row.toSeq)

  /** Append rows to the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append(Arrays.asList("alpha", 1)); > df.append(Arrays.asList("bravo",
    * 2)); > df.length(); 2 }</pre>
    *
    * @param row
    *   the row to append
    * @return
    *   the data frame with the new data appended
    */
  def append(row: Seq[? <: V]): DataFrame[V] =
    append(length.asInstanceOf[AnyRef], row)

  /** Append rows indexed by the the specified name to the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append("row1", Arrays.asList("alpha", 1)); > df.append("row2",
    * Arrays.asList("bravo", 2)); > df.index(); [row1, row2] }</pre>
    *
    * @param name
    *   the row name to add to the index
    * @param row
    *   the row to append
    * @return
    *   the data frame with the new data appended
    */
  @Timed
  def append(name: AnyRef, row: Seq[? <: V]): DataFrame[V] = {
    val len = length
    index.add(name, len)
    columns.extend(row.size)
    data.reshape(columns.names.size, len + 1)
//    row.zipWithIndex.foreach { case (value, c) => data.set(value, c, len) }
    for (c <- 0 until data.size()) data
      .set(if c < row.size then row(c) else null.asInstanceOf[V], c, len)
    this
  }

  /** Reshape a data frame to the specified dimensions.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("0", "1", "2"); >
    * df.append("0", Arrays.asList(10, 20, 30)); > df.append("1",
    * Arrays.asList(40, 50, 60)); > df.reshape(3, 2) > .length(); 3 }</pre>
    *
    * @param rows
    *   the number of rows the new data frame will contain
    * @param cols
    *   the number of columns the new data frame will contain
    * @return
    *   a new data frame with the specified dimensions
    */
  def reshape(rows: Int, cols: Int): DataFrame[V] = Shaping
    .reshape(this, rows, cols)

  /** Reshape a data frame to the specified indices.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("0", "1", "2"); >
    * df.append("0", Arrays.asList(10, 20, 30)); > df.append("1",
    * Arrays.asList(40, 50, 60)); > df.reshape(Arrays.asList("0", "1", "2"),
    * Arrays.asList("0", "1")) > .length(); 3 }</pre>
    *
    * @param rows
    *   the names of rows the new data frame will contain
    * @param cols
    *   the names of columns the new data frame will contain
    * @return
    *   a new data frame with the specified indices
    */
  def reshape(rows: Seq[AnyRef], cols: Seq[AnyRef]): DataFrame[V] = Shaping
    .reshape(this, rows, cols)

  /** Return a new data frame created by performing a left outer join of this
    * data frame with the argument and using the row indices as the join key.
    *
    * <pre> {@code > DataFrame<Object> left = new DataFrame<>("a", "b"); >
    * left.append("one", Arrays.asList(1, 2)); > left.append("two",
    * Arrays.asList(3, 4)); > left.append("three", Arrays.asList(5, 6)); >
    * DataFrame<Object> right = new DataFrame<>("c", "d"); > right.append("one",
    * Arrays.asList(10, 20)); > right.append("two", Arrays.asList(30, 40)); >
    * right.append("four", Arrays.asList(50, 60)); > left.join(right) >
    * .index(); [one, two, three] }</pre>
    *
    * @param other
    *   the other data frame
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join(other: DataFrame[V]): DataFrame[V] = {
    val df = join(other, DataFrame.JoinType.LEFT, null)
    logger.info(s"Dataframe finish Join df index -> ${df.getIndex.mkString(",")} ")
    df
  }

  /** Return a new data frame created by performing a join of this data frame
    * with the argument using the specified join type and using the row indices
    * as the join key.
    *
    * @param other
    *   the other data frame
    * @param join
    *   the join type
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join(other: DataFrame[V], join: DataFrame.JoinType): DataFrame[V] =
    join_func(other, join, null)

  /** Return a new data frame created by performing a left outer join of this
    * data frame with the argument using the specified key function.
    *
    * @param other
    *   the other data frame
    * @param on
    *   the function to generate the join keys
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join(
      other: DataFrame[V],
      on: DataFrame.KeyFunction[V],
  ): DataFrame[V] = join(other, DataFrame.JoinType.LEFT, on)

  /** Return a new data frame created by performing a join of this data frame
    * with the argument using the specified join type and the specified key
    * function.
    *
    * @param other
    *   the other data frame
    * @param join
    *   the join type
    * @param on
    *   the function to generate the join keys
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join(
      other: DataFrame[V],
      join: DataFrame.JoinType,
      on: DataFrame.KeyFunction[V],
  ): DataFrame[V] = {
    val df = Combining.join(this, other, join, on)
    logger.info(
      s"Dataframe Inner Join finish df index -> ${df.getIndex.mkString(",")} ",
    )
    df
  }

  final def join_func(
      other: DataFrame[V],
      join: DataFrame.JoinType,
      on: DataFrame.KeyFunction[V],
  ): DataFrame[V] = Combining.join(this, other, join, on)

  /** Return a new data frame created by performing a left outer join of this
    * data frame with the argument using the column values as the join key.
    *
    * @param other
    *   the other data frame
    * @param cols
    *   the indices of the columns to use as the join key
    * @return
    *   the result of the join operation as a new data frame
    */
  final def joinOn(other: DataFrame[V], cols: Int*): DataFrame[V] =
    joinOn(other, DataFrame.JoinType.LEFT, cols)

  /** Return a new data frame created by performing a join of this data frame
    * with the argument using the specified join type and the column values as
    * the join key.
    *
    * @param other
    *   the other data frame
    * @param join
    *   the join type
    * @param cols
    *   the indices of the columns to use as the join key
    * @return
    *   the result of the join operation as a new data frame
    */
  final def joinOn(
      other: DataFrame[V],
      join: DataFrame.JoinType,
      cols: Seq[Int],
  ): DataFrame[V] = Combining.joinOn(this, other, join, cols*)

  /** Return a new data frame created by performing a left outer join of this
    * data frame with the argument using the column values as the join key.
    *
    * @param other
    *   the other data frame
    * @param cols
    *   the names of the columns to use as the join key
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join_on(other: DataFrame[V], cols: AnyRef*): DataFrame[V] =
    joinOn(other, DataFrame.JoinType.LEFT, cols.map(_.asInstanceOf[Int]))

  /** Return a new data frame created by performing a join of this data frame
    * with the argument using the specified join type and the column values as
    * the join key.
    *
    * @param other
    *   the other data frame
    * @param join
    *   the join type
    * @param cols
    *   the names of the columns to use as the join key
    * @return
    *   the result of the join operation as a new data frame
    */
  final def join_on(
      other: DataFrame[V],
      join: DataFrame.JoinType,
      cols: AnyRef*,
  ): DataFrame[V] = join_on(other, join, columns.indices(cols))

  /** Return a new data frame created by performing a left outer join of this
    * data frame with the argument using the common, non-numeric columns from
    * each data frame as the join key.
    *
    * @param other
    *   the other data frame
    * @return
    *   the result of the merge operation as a new data frame
    */
  final def merge(other: DataFrame[V]): DataFrame[V] =
    merge(other, DataFrame.JoinType.LEFT)

  /** Return a new data frame created by performing a join of this data frame
    * with the argument using the specified join type and the common,
    * non-numeric columns from each data frame as the join key.
    *
    * @param other
    *   the other data frame
    * @return
    *   the result of the merge operation as a new data frame
    */
  final def merge(other: DataFrame[V], join: DataFrame.JoinType): DataFrame[V] =
    Combining.merge(this, other, join)

  /** Update the data frame in place by overwriting the any values with the
    * non-null values provided by the data frame arguments.
    *
    * @param others
    *   the other data frames
    * @return
    *   this data frame with the overwritten values
    */
  @SafeVarargs
  final def update(others: DataFrame[? <: V]*): DataFrame[V] = {
    Combining.update(this, true, others*)
    this
  }

  /** Concatenate the specified data frames with this data frame and return the
    * result.
    *
    * <pre> {@code > DataFrame<Object> left = new DataFrame<>("a", "b", "c"); >
    * left.append("one", Arrays.asList(1, 2, 3)); > left.append("two",
    * Arrays.asList(4, 5, 6)); > left.append("three", Arrays.asList(7, 8, 9)); >
    * DataFrame<Object> right = new DataFrame<>("a", "b", "d"); >
    * right.append("one", Arrays.asList(10, 20, 30)); > right.append("two",
    * Arrays.asList(40, 50, 60)); > right.append("four", Arrays.asList(70, 80,
    * 90)); > left.concat(right).length(); 6 }</pre>
    *
    * @param others
    *   the other data frames
    * @return
    *   the data frame containing all the values
    */
  @SafeVarargs
  final def concat(others: DataFrame[? <: V]*): DataFrame[V] = Combining
    .concat(this, others*)

  /** Update the data frame in place by overwriting any null values with any
    * non-null values provided by the data frame arguments.
    *
    * @param others
    *   the other data frames
    * @return
    *   this data frame with the overwritten values
    */
  @SafeVarargs
  final def coalesce(others: DataFrame[? <: V]*): DataFrame[V] = {
    Combining.update(this, false, others*)
    this
  }

  /** Return the size (number of columns) of the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.size(); 2 }</pre>
    *
    * @return
    *   the number of columns
    */
  override def size: Int = data.size()

  /** Return the length (number of rows) of the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append(Arrays.asList("alpha", 1)); > df.append(Arrays.asList("bravo",
    * 2)); > df.append(Arrays.asList("charlie", 3)); > df.length(); 3 }</pre>
    *
    * @return
    *   the number of rows
    */
  def length: Int = data.length()

  /** Return {@code true} if the data frame contains no data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>(); > df.isEmpty();
    * true }</pre>
    *
    * @return
    *   the number of columns
    */
  override def isEmpty: Boolean = length == 0

  /** Return the index names for the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append("row1", Arrays.asList("one", 1)); > df.index(); [row1] }</pre>
    *
    * @return
    *   the index names
    */
  def getIndex: Seq[AnyRef] = index.names.toSeq

  /** Return the column names for the data frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.columns(); [name, value] }</pre>
    *
    * @return
    *   the column names
    */
  def getColumns: Seq[AnyRef] = columns.names.toSeq

  /** Return the value located by the (row, column) names.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<Object>( >
    * Arrays.asList("row1", "row2", "row3"), > Arrays.asList("name", "value"), >
    * Arrays.asList( > Arrays.asList("alpha", "bravo", "charlie"), >
    * Arrays.asList(10, 20, 30) > ) > ); > df.get("row2", "name"); bravo }</pre>
    *
    * @param row
    *   the row name
    * @param col
    *   the column name
    * @return
    *   the value
    */
  def get(row: AnyRef, col: AnyRef): V = {
    logger.info(
      "DataFrame.get: row class " + row.getClass.getName + ", col class " +
        col.getClass.getName,
    )
    val rows = index.get(row)
    val colz = columns.get(col)
//    val view = data.get(colz.toInt, rows.toInt)
//    view
//    get(rows.toInt,colz.toInt)
    getFromIndex(
      index.get(row).asInstanceOf[Int],
      columns.get(col).asInstanceOf[Int],
    )
  }

  /** Return the value located by the (row, column) coordinates.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<Object>( >
    * Seq.empty(), > Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.asList("alpha", "bravo", "charlie"), > Arrays.asList(10, 20, 30) >
    * ) > ); > df.get(1, 0); bravo }</pre>
    *
    * @param row
    *   the row index
    * @param col
    *   the column index
    * @return
    *   the value
    */
  def getFromIndex(row: Int, col: Int): V = data.get(col, row)

  def indexSelect(rowStart: Int, rowEnd: Int): DataFrame[V] =
    slice(rowStart, rowEnd, 0, size)

  //  def this(
  //      index: Seq[AnyRef],
  //      columns: Seq[AnyRef], //mutable.Seq[Any],
  //      data: List[Seq[V]],

  def indexSelect(indexSeq: Seq[AnyRef]): DataFrame[V] = {
    val selectIndexNums = indexSeq.map(indexName => index.get(indexName))
    val selectRowSeq = this.iterrows.filter(selectIndexNums.contains(_))
      .map(_.toSeq).toList
    val selectDF = new DataFrame(
      selectIndexNums.map(_.asInstanceOf[AnyRef]),
      this.getColumns,
      selectRowSeq,
    )
    selectDF
  }

  def indexSelect(
      indexSeq: Seq[Int],
      isNumber: Boolean = true,
  ): DataFrame[V] = {
    val selectIndexNums = indexSeq.map(indexName => this.index.getInt(indexName))
    val selectRowSeq = this.iterrows.zipWithIndex
      .filter(rowIndex => selectIndexNums.contains(rowIndex._2)).map(_._1)
      .toList
    val selectDF = new DataFrame(
      selectIndexNums.map(_.asInstanceOf[AnyRef]),
      this.getColumns,
      selectRowSeq.transpose,
    )
    selectDF
  }
//    val selectIndexNums = indexSeq.map(indexName => index.getInt(indexName))
//    val selectRowSeq = this.iterrows.zipWithIndex.filter( rowIndex => selectIndexNums.contains(rowIndex._2)).toList
//    val selectDF = new DataFrame(selectIndexNums.map(_.asInstanceOf[AnyRef]), this.getColumns, selectRowSeq)
//    selectDF

  def columnSelect(colStart: Int, colEnd: Int): DataFrame[V] =
    slice(0, length, colStart, colEnd)

  def columnSelect(cols: Seq[AnyRef]): DataFrame[V] =
    val featureCols = cols.map(col => columns.names.filter(_.equals(col)))
      .flatten
    val selectDF = this.retain(featureCols.map(_.toString))
    selectDF

  def slice(rowStart: AnyRef, rowEnd: AnyRef): DataFrame[V] =
    slice(index.get(rowStart), index.get(rowEnd), 0, size)

  def slice(
      rowStart: AnyRef,
      rowEnd: AnyRef,
      colStart: AnyRef,
      colEnd: AnyRef,
  ): DataFrame[V] = slice(
    index.get(rowStart),
    index.get(rowEnd),
    columns.get(colStart),
    columns.get(colEnd),
  )

  override def slice(rowStart: Int, rowEnd: Int): DataFrame[V] =
    slice(rowStart, rowEnd, 0, size)

  def slice(
      rowStart: Int,
      rowEnd: Int,
      colStart: Int,
      colEnd: Int,
  ): DataFrame[V] = {
    val slice = Selection.slice(this, rowStart, rowEnd, colStart, colEnd)
    new DataFrame[V](
      Selection.select(index, slice(0)),
      Selection.select(columns, slice(1)),
      Selection.select(data, slice(0), slice(1)),
      new Grouping,
    )
  }

  /** Set the value located by the names (row, column).
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( >
    * Arrays.asList("row1", "row2"), > Arrays.asList("col1", "col2") > ); >
    * df.set("row1", "col2", new Int(7)); > df.col(1); [7, null] }</pre>
    *
    * @param row
    *   the row name
    * @param col
    *   the column name
    * @param value
    *   the new value
    */
  def set(row: AnyRef, col: AnyRef, value: V): Unit =
    set(index.get(row), columns.get(col), value)

  /** Set the value located by the coordinates (row, column).
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( >
    * Arrays.asList("row1", "row2"), > Arrays.asList("col1", "col2") > ); >
    * df.set(1, 0, new Int(7)); > df.col(0); [null, 7] }</pre>
    *
    * @param row
    *   the row index
    * @param col
    *   the column index
    * @param value
    *   the new value
    */
  def set(row: Int, col: Int, value: V): Unit = data.set(value, col, row)

  /** Return a data frame column as a list.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "bravo", "charlie"), >
    * Arrays.<Object>asList(1, 2, 3) > ) > ); > df.col("value"); [1, 2, 3]
    * }</pre>
    *
    * @param column
    *   the column name
    * @return
    *   the list of values
    */
//  def col(column: Int):  Seq[V] = col(columns.get(column))

  def col(column: AnyRef): Seq[V] = col_with_view(columns.get(column)).toSeq

  def colInt(column: Int, index: Boolean = true) =
//    println("DataFrame.col: column index:  " + column)
    val views = new Views.SeriesListView[V](this, column, true)
    views.toSeq

  /** Return a data frame column as a list.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "bravo", "charlie"), >
    * Arrays.<Object>asList(1, 2, 3) > ) > ); > df.col(1); [1, 2, 3] }</pre>
    *
    * @param column
    *   the column index
    * @return
    *   the list of values
    */
  def col_with_view(column: Int) =
    new Views.SeriesListView[V](this, column, true)

  /** Return a data frame row as a list.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( >
    * Arrays.asList("row1", "row2", "row3"), > Seq.empty(), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "bravo", "charlie"), >
    * Arrays.<Object>asList(1, 2, 3) > ) > ); > df.row("row2"); [bravo, 2]
    * }</pre>
    *
    * @param row
    *   the row name
    * @return
    *   the list of values
    */
  def row(row: AnyRef): Seq[V] = {
    val indexNum = index.get(row)
    val view = new Views.SeriesListView[V](this, indexNum, false)
    view.toSeq
//    val indexValueSeq = row_index(indexNum).asScala
//    indexValueSeq.toSeq
  }

  /** Return a data frame row as a list.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Seq.empty(), > Arrays.asList( > Arrays.<Object>asList("alpha", "bravo",
    * "charlie"), > Arrays.<Object>asList(1, 2, 3) > ) > ); > df.row(1); [bravo,
    * 2] }</pre>
    *
    * @param row
    *   the row index
    * @return
    *   the list of values
    */
  def row_index(row: Int) = {
    logger.debug(s"Dataframe row_index method row: $row")
    val view = new Views.SeriesListView[V](this, row, false)
    logger.debug(s"Dataframe row_index method view: $view")
    view.toSeq
  }

  /** Select a subset of the data frame using a predicate function.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * for (int i = 0; i < 10; i++) > df.append(Arrays.asList("name" + i, i)); >
    * df.select(new Predicate<Object>() { > @Override > public Boolean
    * apply(List<Object> values) { > return
    * Int.class.cast(values.get(1)).intValue() % 2 == 0; > } > }) > .col(1); [0,
    * 2, 4, 6, 8] } </pre>
    *
    * @param predicate
    *   a function returning true for rows to be included in the subset
    * @return
    *   a subset of the data frame
    */
  def select(predicate: DataFrame.Predicate[V]): DataFrame[V] = {
    val selected = Selection.select(this, predicate)
    new DataFrame[V](
      Selection.select(index, selected),
      columns,
      Selection.select(data, selected),
      new Grouping,
    )
  }

  /** Return a data frame containing the first ten rows of this data frame.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); > for (int i =
    * 0; i < 20; i++) > df.append(Arrays.asList(i)); > df.head() >
    * .col("value"); [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] }</pre>
    *
    * @return
    *   the new data frame
    */
  def head(num: Int = 10): DataFrame[V] = heads(num)

  /** Return a data frame containing the first {@code limit} rows of this data
    * frame.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); > for (int i =
    * 0; i < 20; i++) > df.append(Arrays.asList(i)); > df.head(3) >
    * .col("value"); [0, 1, 2] }</pre>
    *
    * @param limit
    *   the number of rows to include in the result
    * @return
    *   the new data frame
    */
  def heads(limit: Int): DataFrame[V] = {
    val selected = new SparseBitSet
    val boundary = Math.min(limit, length)
    selected.set(0, boundary)
    new DataFrame[V](
      Selection.select(index, selected),
      columns,
      Selection.select(data, selected),
      new Grouping,
    )
  }

  /** Return a data frame containing the last ten rows of this data frame.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); > for (int i =
    * 0; i < 20; i++) > df.append(Arrays.asList(i)); > df.tail() >
    * .col("value"); [10, 11, 12, 13, 14, 15, 16, 17, 18, 19] }</pre>
    *
    * @return
    *   the new data frame
    */
  override def tail: DataFrame[V] = tail(10)

  /** Return a data frame containing the last {@code limit} rows of this data
    * frame.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); > for (int i =
    * 0; i < 20; i++) > df.append(Arrays.asList(i)); > df.tail(3) >
    * .col("value"); [17, 18, 19] }</pre>
    *
    * @param limit
    *   the number of rows to include in the result
    * @return
    *   the new data frame
    */
  def tail(limit: Int): DataFrame[V] = {
    val selected = new SparseBitSet
    val len = length
    selected.set(Math.max(len - limit, 0), len)
    new DataFrame[V](
      Selection.select(index, selected),
      columns,
      Selection.select(data, selected),
      new Grouping,
    )
  }

  /** Return the values of the data frame as a flat list.
    *
    * <pre> {@code > DataFrame<String> df = new DataFrame<>( > Arrays.asList( >
    * Arrays.asList("one", "two"), > Arrays.asList("alpha", "bravo") > ) > ); >
    * df.flatten(); [one, two, alpha, bravo] }</pre>
    *
    * @return
    *   the list of values
    */
  def flatten = new Views.FlatView[V](this)

  /** Transpose the rows and columns of the data frame.
    *
    * <pre> {@code > DataFrame<String> df = new DataFrame<>( > Arrays.asList( >
    * Arrays.asList("one", "two"), > Arrays.asList("alpha", "bravo") > ) > ); >
    * df.transpose().flatten(); [one, alpha, two, bravo] }</pre>
    *
    * @return
    *   a new data frame with the rows and columns transposed
    *   //index.names.asInstanceOf[mutable.Seq[Any]],
    */
  def transpose = new DataFrame[V](
    columns.names,
    index.names,
    new Views.ListView[V](this, true).map(_.toSeq).toList,
  )

  /** Apply a function to each value in the data frame.
    *
    * <pre> {@code > DataFrame<Number> df = new DataFrame<>( >
    * Arrays.<List<Number>>asList( > Arrays.<Number>asList(1, 2), >
    * Arrays.<Number>asList(3, 4) > ) > ); > df = df.apply(new Function<Number,
    * Number>() { > public Number apply(Number value) { > return
    * value.intValue() * value.intValue(); > } > }); > df.flatten(); [1, 4, 9,
    * 16] }</pre>
    *
    * @param function
    *   the function to apply
    * @return
    *   a new data frame with the function results
    *   //columns.names.asInstanceOf[mutable.Seq[Any]],
    */
  def apply[U](function: DataFrame.Function[V, U]) = new DataFrame[U](
    index.names,
    columns.names,
    new Views.TransformedView[V, U](this, function, false).map(_.toSeq).toList,
  )

  def transform[U](transform: DataFrame.RowFunction[V, U]): DataFrame[U] = {
    val transformed = new DataFrame[U](columns.names.map(_.toString)*)
    val it = this.getIndex.iterator
    for (row <- this) for (trans <- transform.apply(row)) transformed.append(
      if (it.hasNext) it.next else transformed.length.asInstanceOf[AnyRef],
      trans,
    )
    transformed
  }

  /** Attempt to infer better types for object columns.
    *
    * <p>The following conversions are performed where applicable: <ul>
    * <li>Floating point numbers are converted to {@code Double} values</li>
    * <li>Whole numbers are converted to {@code Long} values</li> <li>True,
    * false, yes, and no are converted to {@code Boolean} values</li> <li>Date
    * strings in the following formats are converted to {@code Date} values:<br>
    * {@literal 2000-01-01T00:00:00+1, 2000-01-01T00:00:00EST, 2000-01-01}</li>
    * <li>Time strings in the following formats are converted to {@code Date}
    * values:<br>
    * {@literal 2000/01/01, 1/01/2000, 12:01:01 AM, 23:01:01, 12:01 AM, 23:01}</li>
    * </li> </ul> </p>
    *
    * <p>Note, the conversion process replaces existing values with values of
    * the converted type.</p>
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value",
    * "date"); > df.append(Arrays.asList("one", "1", new Date())); >
    * df.convert(); > df.types(); [class java.lang.String, class java.lang.Long,
    * class java.util.Date] }</pre>
    *
    * @return
    *   the data frame with the converted values
    */
  def convert: DataFrame[V] = {
    Conversion.convert(this)
    this
  }

  def convert(
      numDefault: DataFrame.NumberDefault,
      naString: String,
  ): DataFrame[V] = {
    Conversion.convert(this, numDefault, naString)
    this
  }

  /** Convert columns based on the requested types.
    *
    * <p>Note, the conversion process replaces existing values with values of
    * the converted type.</p>
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("a", "b", "c"); >
    * df.append(Arrays.asList("one", 1, 1.0)); > df.append(Arrays.asList("two",
    * 2, 2.0)); > df.convert( > null, // leave column "a" as is > Long.class, //
    * convert column "b" to Long > Number.class // convert column "c" to Double
    * > ); > df.types(); [class java.lang.String, class java.lang.Long, class
    * java.lang.Double] }</pre>
    *
    * @param columnTypes
    * @return
    *   the data frame with the converted values
    */
  @SafeVarargs
  final def convert(columnTypes: Class[? <: V]*): DataFrame[V] = {
    Conversion.convert(this, columnTypes*)
    this
  }

  /** Create a new data frame containing boolean values such that {@code null}
    * object references in the original data frame yield {@code true} and valid
    * references yield {@code false}.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<Object>( >
    * Arrays.asList( > Arrays.asList("alpha", "bravo", null), >
    * Arrays.asList(null, 2, 3) > ) > ); > df.isnull().row(0); [false, true]
    * }</pre>
    *
    * @return
    *   the new boolean data frame2
    */
  def isnull: DataFrame[Boolean] = Conversion.isnull(this)

  /** Create a new data frame containing boolean values such that valid object
    * references in the original data frame yield {@code true} and {@code null}
    * references yield {@code false}.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "bravo", null), >
    * Arrays.<Object>asList(null, 2, 3) > ) > ); > df.notnull().row(0); [true,
    * false] }</pre>
    *
    * @return
    *   the new boolean data frame
    */
  def notnull: DataFrame[Boolean] = Conversion.notnull(this)

  /** Copy the values of contained in the data frame into a flat array of length
    * {@code #size()} * {@code #length()}.
    *
    * @return
    *   the array
    */
//  def toArray: Array[AnyRef] = {
//    val arr = new Array[AnyRef](size * length)
//    toArray(arr)
//  }
//todo need work
  /** Copy the values of contained in the data frame into the specified array.
    * If the length of the provided array is less than length {@code #size()} *
    * {@code #length()} a new array will be created.
    *
    * @return
    *   the array
    */
  def toArray[U <: V](array: Array[U]): Array[U] = {
    val view = new Views.FlatView[V](this).toBuffer[V]
      .asInstanceOf[mutable.Buffer[U]]
    view.addAll(array.toSeq)
    view.asInstanceOf[Array[U]]
//    view.asInstanceOf[util.AbstractSeq[U]].toArray(array)
  }

// todo  need work
  @SuppressWarnings(Array("unchecked"))
  def toArray[U](array: Array[Array[U]]): Array[Array[U]] = {
    if (array.length >= size && array.length > 0 && array(0).length >= length)
      for (c <- 0 until size) for (r <- 0 until length)
        array(r)(c) = getFromIndex(r, c).asInstanceOf[U]
    toArray(array.getClass).asInstanceOf[Array[Array[U]]]
  }

  /** Copy the values of contained in the data frame into a array of the
    * specified type. If the type specified is a two dimensional array, for
    * example {@code double[][].class}, a row-wise copy will be made.
    *
    * @throws IllegalArgumentException
    *   if the values are not assignable to the specified component type
    * @return
    *   the array
    */
  def toArray[U](cls: Class[U]): U = {
    var dim = 0
    var currentType: Class[?] = cls // Use Class[?] for unknown component type
    while (currentType.getComponentType != null) {
      currentType = currentType.getComponentType
      dim += 1
    }

    val numCols = this.size // Assuming size() returns column count
    val numRows = this.length // Assuming length() returns row count

    if (dim == 1) {
      // Create a 1D array using Java reflection
      val array = java.lang.reflect.Array
        .newInstance(currentType, numCols * numRows) // // Cast to U

      // Populate the 1D array
      for (c <- 0 until numCols) // Iterate columns
        for (r <- 0 until numRows) // Iterate rows
          // Assuming data.get(c, r) gets value at col c, row r
          java.lang.reflect.Array.set(array, c * numRows + r, data.get(c, r))
      array.asInstanceOf[U]

    } else if (dim == 2) {
      // Create a 2D array using Java reflection
      val array = java.lang.reflect.Array
        .newInstance(currentType, numRows, numCols) // Cast to U

      // Populate the 2D array
      for (r <- 0 until numRows) { // Iterate rows
        val innerArray = java.lang.reflect.Array.get(array, r) // Get the inner 1D array for the row
        for (c <- 0 until numCols) // Iterate columns
          // Assuming get(r, c) gets value at row r, col c
          java.lang.reflect.Array.set(innerArray, c, getFromIndex(r, c))
        // Set the modified inner array back (might be redundant for Object arrays but kept for fidelity)
        java.lang.reflect.Array.set(array, r, innerArray)
      }
      array.asInstanceOf[U]

    } else
      // Throw exception for unsupported dimensions
      throw new IllegalArgumentException("class must be a 1D or 2D array class")
  }
//  def toArray[U](cls: Class[U]): U = {
//    var dim = 0
//    var dtype = cls
//    while (dtype.getComponentType != null) {
//      dtype = dtype.getComponentType
//      dim += 1
//    }
//    val size = size
//    val len = length
//    if (dim == 1) {
//      @SuppressWarnings(Array("unchecked"))
//      val array = Array.newInstance(dtype, size * len).asInstanceOf[U]
//      for (c <- 0 until size) {
//        for (r <- 0 until len) {
//          Array.set(array, c * len + r, data.get(c, r))
//        }
//      }
//      return array
//    }
//    else if (dim == 2) {
//      @SuppressWarnings(Array("unchecked"))
//      val array = Array.newInstance(dtype, Array[Int](len, size)).asInstanceOf[U]
//      for (r <- 0 until len) {
//        val aa = Array.get(array, r)
//        for (c <- 0 until size) {
//          Array.set(aa, c, get(r, c))
//        }
//        Array.set(array, r, aa)
//      }
//      return array
//    }
//    throw new IllegalArgumentException("class must be an array class")
//  }

  /** Encodes the DataFrame as a model matrix, converting nominal values to
    * dummy variables but does not add an intercept column.
    *
    * More methods with additional parameters to control the conversion to the
    * model matrix are available in the <code>Conversion</code> class.
    *
    * @param fillValue
    *   value to replace NA's with
    * @return
    *   a model matrix
    */
  def toModelMatrix[V1: ClassTag](fillValue: Any): Array[Array[V1]] = {
    val tag = implicitly[ClassTag[V1]]
    val matrix = Conversion
      .toModelMatrix(this.asInstanceOf[DataFrame[V1]], fillValue)
    matrix.asInstanceOf[Array[Array[V1]]]
  }

//  def toModelMatrix(fillValue: Double): Array[Array[Double]] = {
//    val matrix = Conversion.toModelMatrix(this, fillValue.asInstanceOf[V])
//    matrix
//  }

  /** Encodes the DataFrame as a model matrix, converting nominal values to
    * dummy variables but does not add an intercept column.
    *
    * More methods with additional parameters to control the conversion to the
    * model matrix are available in the <code>Conversion</code> class.
    *
    * @return
    *   a model matrix
    */
  def toModelMatrixDataFrame: DataFrame[V] = {
    val matrix = Conversion.toModelMatrixDataFrame(this)
    matrix
  }

  /** Group the data frame rows by the specified column names.
    *
    * @param cols
    *   the column names
    * @return
    *   the grouped data frame
    */
  @Timed
  def groupBy(cols: AnyRef*): DataFrame[V] = {
    logger.debug(s"data groupBy ${cols.mkString(",")}")
    val indices = columns.indices(cols)
    groupBy_index(indices*)
  }

  def groupBy(cols: AnyRef, index: Boolean = true): DataFrame[V] = {
//    logger.debug(s"data groupBy ${cols.mkString(",")}")
    val indices = columns.indices(cols.asInstanceOf[Array[Int]].map(_.toString))
    groupBy_index(indices*)
  }

  /** Group the data frame rows by the specified columns.
    *
    * @param cols
    *   the column indices
    * @return
    *   the grouped data frame
    */
  @Timed
  def groupBy_index(cols: Int*) =
    new DataFrame[V](index, columns, data, new Grouping(this, cols.toArray*))

  /** Group the data frame rows using the specified key function.
    *
    * @param function
    *   the function to reduce rows to grouping keys
    * @return
    *   the grouped data frame
    */
  @Timed
  def groupBy(function: DataFrame.KeyFunction[V]) =
    new DataFrame[V](index, columns, data, new Grouping(this, function))

  def getGroups: Grouping[V] = groups

  /** Return a map of group names to data frame for grouped data frames. Observe
    * that for this method to have any effect a {@code groupBy} call must have
    * been done before.
    *
    * @return
    *   a map of group names to data frames
    */
  def explode: LinkedHashMap[AnyRef, DataFrame[V]] = {
    val explodedMap = new mutable.LinkedHashMap[AnyRef, DataFrame[V]]

    for (entry <- groups) {
      val selected = entry._2
      explodedMap.put(
        entry._1,
        new DataFrame[V](
          Selection.select(index, selected),
          columns,
          Selection.select(data, selected),
          new Grouping,
        ),
      )
    }
    explodedMap
  }

  /** Apply an aggregate function to each group or the entire data frame if the
    * data is not grouped.
    *
    * @param function
    *   the aggregate function
    * @return
    *   the new data frame
    */
  def aggregate[U](function: DataFrame.Aggregate[V, U]): DataFrame[V] = groups
    .apply(this, function)

  @Timed
  def count: DataFrame[V] = groups.apply(this, new Aggregation.Count[V])

  def collapse: DataFrame[V] = groups.apply(this, new Aggregation.Collapse[V])

  def unique: DataFrame[V] = groups.apply(this, new Aggregation.Unique[V])

  /** Compute the sum of the numeric columns for each group or the entire data
    * frame if the data is not grouped.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo"), >
    * Arrays.<Object>asList(1, 2, 3, 4, 5) > ) > ); > df.groupBy("name") >
    * .sum() > .col("value"); [6.0, 9.0]} </pre>
    *
    * @return
    *   the new data frame
    */
  @Timed
  def sum: DataFrame[V] = groups.apply(this, new Aggregation.Sum[V])

  /** Compute the product of the numeric columns for each group or the entire
    * data frame if the data is not grouped.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo"), >
    * Arrays.<Object>asList(1, 2, 3, 4, 5) > ) > ); > df.groupBy("name") >
    * .prod() > .col("value"); [6.0, 20.0]} </pre>
    *
    * @return
    *   the new data frame
    */
  @Timed
  def prod: DataFrame[V] = groups.apply(this, new Aggregation.Product[V])

  /** Compute the mean of the numeric columns for each group or the entire data
    * frame if the data is not grouped.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); >
    * df.append("one", Arrays.asList(1)); > df.append("two", Arrays.asList(5));
    * > df.append("three", Arrays.asList(3)); > df.append("four",
    * Arrays.asList(7)); > df.mean().col(0); [4.0] }</pre>
    *
    * @return
    *   the new data frame
    */
  @Timed
  def mean: DataFrame[V] = groups.apply(this, new Aggregation.Mean[V])

  /** Compute the percentile of the numeric columns for each group or the entire
    * data frame if the data is not grouped.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); >
    * df.append("one", Arrays.asList(1)); > df.append("two", Arrays.asList(5));
    * > df.append("three", Arrays.asList(3)); > df.append("four",
    * Arrays.asList(7)); > df.mean().col(0); [4.0] }</pre>
    *
    * @return
    *   the new data frame
    */
  @Timed
  def percentile(quantile: Double): DataFrame[V] = groups
    .apply(this, new Aggregation.Percentile[V](quantile))

  /** Compute the standard deviation of the numeric columns for each group or
    * the entire data frame if the data is not grouped.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>( > Seq.empty(), >
    * Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo",
    * "bravo"), > Arrays.<Object>asList(1, 2, 3, 4, 6, 8) > ) > ); >
    * df.groupBy("name") > .stddev() > .col("value"); [1.0, 2.0]} </pre>
    *
    * @return
    *   the new data frame
    */
  @Timed
  def stddev: DataFrame[V] = groups.apply(this, new Aggregation.StdDev[V])

  @Timed
  def `var`: DataFrame[V] = groups.apply(this, new Aggregation.Variance[V])

  @Timed
  def skew: DataFrame[V] = groups.apply(this, new Aggregation.Skew[V])

  @Timed
  def kurt: DataFrame[V] = groups.apply(this, new Aggregation.Kurtosis[V])

  @Timed
  def min: DataFrame[V] = groups
    .apply(this.numeric.asInstanceOf[DataFrame[V]], new Aggregation.Min[V])

  @Timed
  def max: DataFrame[V] = groups
    .apply(this.numeric.asInstanceOf[DataFrame[V]], new Aggregation.Max[V])

  @Timed
  def median: DataFrame[V] = groups.apply(this, new Aggregation.Median[V])

  @Timed
  def cov: DataFrame[Number] = Aggregation
    .cov(this.numeric.asInstanceOf[DataFrame[V]])

  @Timed
  def cumsum: DataFrame[V] = groups
    .apply(this.numeric.asInstanceOf[DataFrame[V]], new Transforms.CumulativeSum)

  @Timed
  def cumprod: DataFrame[V] = groups.apply(
    this.numeric.asInstanceOf[DataFrame[V]],
    new Transforms.CumulativeProduct,
  )

  @Timed
  def cummin: DataFrame[V] = groups
    .apply(this.numeric.asInstanceOf[DataFrame[V]], new Transforms.CumulativeMin)

  @Timed
  def cummax: DataFrame[V] = groups
    .apply(this.numeric.asInstanceOf[DataFrame[V]], new Transforms.CumulativeMax)

  @Timed
  def describe: DataFrame[V] = {
    val df = this.numeric.asInstanceOf[DataFrame[V]]
    val conv = groups.apply(df, new Aggregation.Describe[V])
    Aggregation.describe(conv)
  }

  def pivot(row: AnyRef, col: AnyRef, values: AnyRef*): DataFrame[V] =
    pivot(List(row), List(col), values)

  def pivot(
      rows: Seq[AnyRef],
      cols: Seq[AnyRef],
      values: Seq[AnyRef],
  ): DataFrame[V] = {

    val rowsArray = columns.indices(rows)
    val colsArray = columns.indices(cols)
    logger.info(
      s"DataFrame Pivoting -> pivot rows -> ${rows.mkString(",")} | ${rowsArray
          .mkString(",")} cols-> ${cols.mkString(",")} ${colsArray
          .mkString(",")} values ${values.mkString(",")}",
    )
    val valuesArray = columns.indices(values) // .asInstanceOf[Array[AnyRef]])  =Array[Int](2,3) //
    pivot(rowsArray, colsArray, valuesArray)
  }

  def pivot(row: Int, col: Int, values: Int*): DataFrame[V] =
    pivot(Array[Int](row), Array[Int](col), values)

  @Timed
  def pivot(
      rows: Array[Int],
      cols: Array[Int],
      values: Array[Int],
  ): DataFrame[V] = {
    logger.info(s"DataFrame Pivoting -> pivot rows ${rows.mkString(",")} cols ${cols
        .mkString(",")} values ${values.mkString(",")}")
    Pivoting.pivot(this, rows, cols, values)
  }

  @Timed
  def pivot[U](
      rows: DataFrame.KeyFunction[V],
      cols: DataFrame.KeyFunction[V],
      values: LinkedHashMap[Int, DataFrame.Aggregate[V, U]],
  ): DataFrame[U] = Pivoting.pivot(this, rows, cols, values)

  def sortBy(cols: AnyRef*): DataFrame[V] = {
    val sortCols = new mutable.LinkedHashMap[Int, DataFrame.SortDirection]
    for (col <- cols) {
      val str = if (col.isInstanceOf[String]) classOf[String].cast(col) else ""
      val dir =
        if (str.startsWith("-")) DataFrame.SortDirection.DESCENDING
        else DataFrame.SortDirection.ASCENDING
      val c = columns.get(if (str.startsWith("-")) str.substring(1) else col)
      sortCols.put(c, dir)
    }
    Sorting.sort(this, sortCols)
  }

  @Timed
  def sortBy_index(cols: Int*): DataFrame[V] = {
    val sortCols = new mutable.LinkedHashMap[Int, DataFrame.SortDirection]
    for (c <- cols) {
      val dir =
        if (c < 0) DataFrame.SortDirection.DESCENDING
        else DataFrame.SortDirection.ASCENDING
      sortCols.put(Math.abs(c), dir)
    }
    Sorting.sort(this, sortCols)
  }

  def sortBy(comparator: Comparator[Seq[V]]): DataFrame[V] = Sorting
    .sort(this, comparator)

  /** Return the types for each of the data frame columns.
    *
    * @return
    *   the list of column types
    */
  def types: Seq[Class[?]] = Inspection.types(this)

  /** Return a data frame containing only columns with numeric data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append(Arrays.asList("one", 1)); > df.append(Arrays.asList("two", 2));
    * > df.numeric().columns(); [value] }</pre>
    *
    * @return
    *   a data frame containing only the numeric columns
    */
  def numeric: DataFrame[Number] = {
    val numeric = Inspection.numeric(this)
    val keep = Selection.select(columns, numeric).names
    val keepArray = Array("value", "version", "age", "score") // keep.toSeq//.map(_.asInstanceOf[String])
    logger.info(s"dataframe keep elect names ->: ${keep
        .mkString(", ")} numeric keepArray ${keepArray.length} cols ${keepArray
        .mkString(",")}")
//    retain(keepArray).cast(classOf[Number])
    retain(keep.map(_.toString)).cast(classOf[Number])
  }

  /** Return a data frame containing only columns with non-numeric data.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append(Arrays.asList("one", 1)); > df.append(Arrays.asList("two", 2));
    * > df.nonnumeric().columns(); [name] }</pre>
    *
    * @return
    *   a data frame containing only the non-numeric columns
    */
//  def nonnumeric: DataFrame[V] = {
//    val nonnumeric = Inspection.nonnumeric(this)
//    val keep = Selection.select(columns, nonnumeric).names
//    retain(keep.toArray(new Array[AnyRef](keep.size)))
//  }

  def nonnumeric: DataFrame[V] = {
    // Call the nonnumeric method from the Inspection object/class
    val nonnumeric: SparseBitSet = Inspection.nonnumeric(this)
    val scalaKeepSet = Selection.select(columns, nonnumeric).names
    // Convert the Scala Set to an Array[Any] (which corresponds to Object[] in Java)
    val keepArray = scalaKeepSet.toSeq.map(_.asInstanceOf[String])

    // Retain the columns specified by the array of names
    retain(keepArray)
  }

  /** Return an iterator over the rows of the data frame. Also used implicitly
    * with {@code foreach} loops.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>( > Arrays.asList( >
    * Arrays.asList(1, 2), > Arrays.asList(3, 4) > ) > ); > List<Int> results =
    * new ArrayList<>(); > for (List<Int> row : df) > results.add(row.get(0)); >
    * results; [1, 2] }</pre>
    *
    * @return
    *   an iterator over the rows of the data frame.
    */
  override def iterator: Iterator[List[V]] = iterrows

  def iterrows: Iterator[List[V]] = new Views.ListView[V](this, true).iterator
    .map(_.toList).iterator

  def itercols: Iterator[List[V]] = new Views.ListView[V](this, false).iterator
    .map(_.toList).iterator

  def itermap: Iterator[Map[Any, V]] = new Views.MapView[V](this, true).iterator
    .map(_.toMap).iterator

//  def itermap:  Iterator[LinkedHashMap[AnyRef, V]] = new Views.MapView[V](this, true).iterator

  def itervalues: Iterator[V] = new Views.FlatView[V](this).iterator

  /** Cast this data frame to the specified type.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<>("name", "value"); >
    * df.append(Arrays.asList("one", "1")); > DataFrame<String> dfs =
    * df.cast(String.class); > dfs.get(0, 0).getClass().getName();
    * java.lang.String }</pre>
    *
    * @param cls
    * @return
    *   the data frame cast to the specified type
    */
  @SuppressWarnings(Array("unchecked"))
  def cast[T](cls: Class[T]): DataFrame[T] = {
    def convertValue(value: Any): T =
      if (value == null) null.asInstanceOf[T]
      else if (cls.isInstance(value)) value.asInstanceOf[T]
      else cls match {
        case java.lang.Integer.TYPE => value.toString.toInt.asInstanceOf[T]
        case java.lang.Long.TYPE => value.toString.toLong.asInstanceOf[T]
        case java.lang.Double.TYPE => value.toString.toDouble.asInstanceOf[T]
        case java.lang.Float.TYPE => value.toString.toFloat.asInstanceOf[T]
        case java.lang.Short.TYPE => value.toString.toShort.asInstanceOf[T]
        case java.lang.Byte.TYPE => value.toString.toByte.asInstanceOf[T]
        case java.lang.Boolean.TYPE => value.toString.toBoolean.asInstanceOf[T]
        case _ => value.toString.asInstanceOf[T]
//          case _ =>
//            throw new IllegalArgumentException(s"Unsupported target type: ${cls.getName}")
      }

    val data: List[Seq[T]] = this.itercols.map(_.map(convertValue)).toList // this.itercols.map(_.map(cls.cast(_))).toList
    val indexSeq: Seq[AnyRef] = index.names
    val columnsSeq: Seq[AnyRef] = columns.names
    new DataFrame[T](indexSeq, columnsSeq, data)
//    new DataFrame[T](index, columns, data, groups)
//    this.asInstanceOf[DataFrame[T]]
  }

  @SuppressWarnings(Array("unchecked"))
//  def castz[T](cls: Class[T]): DataFrame[T] = {
//    // 定义一个转换函数，根据目标类型进行不同的转换操作
//    def convertValue(value: Any): T = {
//      if (value == null) {
//        null.asInstanceOf[T]
//      } else if (cls.isInstance(value)) {
//        value.asInstanceOf[T]
//      } else {
//        cls match {
//          case java.lang.Integer.TYPE | classOf[java.lang.Integer] =>
//        value.toString.toInt.asInstanceOf[T]
//          case java.lang.Long.TYPE | classOf[java.lang.Long] =>
//        value.toString.toLong.asInstanceOf[T]
//          case java.lang.Double.TYPE | classOf[java.lang.Double] =>
//        value.toString.toDouble.asInstanceOf[T]
//          case java.lang.Float.TYPE | classOf[java.lang.Float] =>
//        value.toString.toFloat.asInstanceOf[T]
//          case java.lang.Short.TYPE | classOf[java.lang.Short] =>
//        value.toString.toShort.asInstanceOf[T]
//          case java.lang.Byte.TYPE | classOf[java.lang.Byte] =>
//        value.toString.toByte.asInstanceOf[T]
//          case java.lang.Boolean.TYPE | classOf[java.lang.Boolean] =>
//        value.toString.toBoolean.asInstanceOf[T]
//          case classOf[java.lang.String] =>
//        value.toString.asInstanceOf[T]
//          case _ =>
//            throw new IllegalArgumentException(s"Unsupported target type: ${cls.getName}")
//        }
//      }
//    }
//
//    // 对数据框中的每个元素进行类型转换
//    val newData = data.map(_.map(convertValue))
//
//    // 创建一个新的数据框，使用转换后的数据
//    new DataFrame[T](index, columns, newData, groups)
//  }

  /** Return a map of index names to rows.
    *
    * <pre> {@code > DataFrame<Int> df = new DataFrame<>("value"); >
    * df.append("alpha", Arrays.asList(1)); > df.append("bravo",
    * Arrays.asList(2)); > df.map(); {alpha=[1], bravo=[2]}}</pre>
    *
    * @return
    *   a map of index names to rows.
    */
  def map: LinkedHashMap[Any, Seq[V]] = {
    val m = new mutable.LinkedHashMap[Any, Seq[V]]
    val len = length
    val names = index.names.iterator
    for (r <- 0 until len) {
      val name = if (names.hasNext) names.next else r
      m.put(name, row_index(r).toSeq)
    }
    m
  }

  def map(key: AnyRef, value: AnyRef): LinkedHashMap[Any, Seq[Any]] =
    map(columns.get(key), columns.get(value))

  def map(key: Int, value: Int): LinkedHashMap[Any, Seq[Any]] = {
    val m = new mutable.LinkedHashMap[Any, Seq[Any]]
    val len = length
    for (r <- 0 until len) {
      val name = data.get(key, r)
      var values = m.get(name).get.toBuffer
      if (values == null) {
        values = new ListBuffer[Any]()
        m.put(name, values.toSeq)
      }
      values.append(data.get(value, r))
    }
    m
  }

  def unique(cols: AnyRef*): DataFrame[V] = unique(columns.indices(cols))

  def unique_index(cols: Int*): DataFrame[V] = {
    val unique = new DataFrame[V](columns.names.map(_.toString)*)
    val seen = new mutable.HashSet[Seq[Any]]
    val key = new ListBuffer[Any]() // cols.length)
    val len = length
    for (r <- 0 until len) {
      for (c <- cols) key.append(data.get(c, r))
      if (!seen.contains(key.toSeq)) {
        unique.append(row_index(r).toSeq)
        seen.add(key.toSeq)
      }
      key.clear()
    }
    unique
  }

  def diff: DataFrame[V] = diff(1)

  def diff(period: Int): DataFrame[V] = Timeseries.diff(this, period)

  def percentChange: DataFrame[V] = percentChange(1)

  def percentChange(period: Int): DataFrame[V] = Timeseries
    .percentChange(this, period)

  def rollapply(function: DataFrame.Function[Seq[V], V]): DataFrame[V] =
    rollapply(function, 1)

  def rollapply(
      function: DataFrame.Function[Seq[V], V],
      period: Int,
  ): DataFrame[V] = Timeseries.rollapply(this, function, period)

  /** Display the numeric columns of this data frame as a line chart in a new
    * swing frame.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<Object>( >
    * Seq.empty(), > Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.asList("alpha", "bravo", "charlie"), > Arrays.asList(10, 20, 30) >
    * ) > ); > df.plot(); } </pre>
    */
  final def plot(): Unit = plot(DataFrame.PlotType.LINE)

  /** Display the numeric columns of this data frame as a chart in a new swing
    * frame using the specified type.
    *
    * <pre> {@code > DataFrame<Object> df = new DataFrame<Object>( >
    * Seq.empty(), > Arrays.asList("name", "value"), > Arrays.asList( >
    * Arrays.asList("alpha", "bravo", "charlie"), > Arrays.asList(10, 20, 30) >
    * ) > ); > df.plot(PlotType.AREA); } </pre>
    * @param type
    *   the type of plot to display
    */
  final def plot(`type`: DataFrame.PlotType): Unit = Display.plot(this, `type`)

  /** Draw the numeric columns of this data frame as a chart in the specified
    * {@link Container}.
    *
    * @param container
    *   the container to use for the chart
    */
  final def draw(container: Container): Unit = Display
    .draw(this, container, DataFrame.PlotType.LINE)

  /** Draw the numeric columns of this data frame as a chart in the specified
    * {@link Container} using the specified type.
    *
    * @param container
    *   the container to use for the chart
    * @param type
    *   the type of plot to draw
    */
  final def draw(container: Container, types: DataFrame.PlotType): Unit =
    Display.draw(this, container, types)

  final def show(): Unit = Display.show(this)

  /** Write the data from this data frame to the specified file as comma
    * separated values.
    *
    * @param file
    *   the file to write
    * @throws IOException
    *   if an error occurs writing the file
    */
  @throws[IOException]
  final def writeCsv(file: String): Unit = Serialization
    .writeCsv(this, new FileOutputStream(file))

  /** Write the data from this data frame to the provided output stream as comma
    * separated values.
    *
    * @param output
    * @throws IOException
    */
  @throws[IOException]
  final def writeCsv(output: OutputStream): Unit = Serialization
    .writeCsv(this, output)

  /** Write the data from the data frame to the specified file as an excel
    * workbook.
    *
    * @param file
    *   the file to write
    * @throws IOException
    *   if an error occurs writing the file
    */
  @throws[IOException]
  final def writeXls(file: String): Unit = Serialization
    .writeXls(this, new FileOutputStream(file))

  /** Write the data from the data frame to the provided output stream as an
    * excel workbook.
    *
    * @param file
    *   the file to write
    * @throws IOException
    *   if an error occurs writing the file
    */
  @throws[IOException]
  final def writeXls(output: OutputStream): Unit = Serialization
    .writeXls(this, output)

  /** Write the data from the data frame to a database by executing the
    * specified SQL statement.
    *
    * @param c
    *   the database connection
    * @param sql
    *   the SQL statement
    * @throws SQLException
    *   if an error occurs executing the statement
    */
  @throws[SQLException]
  final def writeSql(c: Connection, sql: String): Unit =
    writeSql(c.prepareStatement(sql))

  /** Write the data from the data frame to a database by executing the provided
    * prepared SQL statement.
    *
    * @param stmt
    *   a prepared insert statement
    * @throws SQLException
    *   if an error occurs executing the statement
    */
  @throws[SQLException]
  final def writeSql(stmt: PreparedStatement): Unit = Serialization
    .writeSql(this, stmt)

  final def toString(limit: Int): String = Serialization.toString(this, limit)

  override def toString: String = toString(10)

//  override def head: Seq[V] = super.head
}
