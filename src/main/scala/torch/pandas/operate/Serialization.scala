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
package torch.pandas.operate

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.math.BigInteger
import java.net.URL
import java.sql.ParameterMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import org.apache.poi.hssf.usermodel.HSSFDataFormat
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellStyle
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DateUtil
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.supercsv.cellprocessor.ConvertNullTo
import org.supercsv.cellprocessor.FmtDate
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvListReader
import org.supercsv.io.CsvListWriter
import org.supercsv.prefs.CsvPreference

import torch.DataFrame
import torch.DataFrame.NumberDefault
object Serialization {
  private val EMPTY_DF_STRING = "[empty data frame]"
  private val ELLIPSES = "..."
  private val NEWLINE = "\n"
  private val DELIMITER = "\t"
  private val INDEX_KEY = new AnyRef
  private val MAX_COLUMN_WIDTH = 20

  def toString(df: DataFrame[?], limit: Int): String = {
    val len = df.length
    if (len == 0) return EMPTY_DF_STRING
    val sb = new StringBuilder
    val width = new LinkedHashMap[Any, Int]
    val types = df.types
    val columns = new ListBuffer[Any]() // df.columns)
    df.getColumns.foreach(col => columns.append(col))
    // determine index width
    width.put(INDEX_KEY, 0)

    for (row <- df.getIndex) {
      val rowClass = if (row == null) null else row.getClass
      width.put(
        INDEX_KEY,
        clamp(
          width.get(INDEX_KEY).get,
          MAX_COLUMN_WIDTH,
          fmt(rowClass, row).length,
        ),
      )
    }
    // determine column widths
    for (c <- 0 until columns.size) {
      val column = columns(c)
      width.put(column, String.valueOf(column).length)
      for (r <- 0 until df.length) width.put(
        column,
        clamp(
          width.get(column).get,
          MAX_COLUMN_WIDTH,
          fmt(types(c), df.get(r, c)).length,
        ),
      )
    }
    // output column names
    sb.append(lpad("", width.get(INDEX_KEY).get))
    for (c <- 0 until columns.size) {
      sb.append(DELIMITER)
      val column = columns(c)
      sb.append(lpad(column, width.get(column).get))
    }
    sb.append(NEWLINE)
    // output rows
    val names = df.getIndex.iterator
    var r = 0
    while (r < len) {
      // output row name
      var w = width.get(INDEX_KEY).get
      val row =
        if (names.hasNext) {
          val next = names.next() match {
            case a: Int => a
            case b: String => b
          }
          println(next)
//        names.next.asInstanceOf[Int]
          next
        } else r
      val rowClass = row.getClass // if (row ) null else
      sb.append(truncate(lpad(fmt(rowClass, row), w), w))
      // output rows
      for (c <- 0 until df.size) {
        sb.append(DELIMITER)
        val cls = types(c)
        w = width.get(columns(c)).get
        println(s"col ${columns(c).toString} w $w")
        if (classOf[Number].isAssignableFrom(cls)) sb
          .append(lpad(fmt(cls, df.get(r, c)), w))
        else sb.append(truncate(rpad(fmt(cls, df.get(r, c)), w), w))
      }
      sb.append(NEWLINE)
      // skip rows if necessary to limit output
      if (limit - 3 < r && r < (limit << 1) && r < len - 4) {
        sb.append(NEWLINE).append(ELLIPSES).append(" ").append(len - limit)
          .append(" rows skipped ").append(ELLIPSES).append(NEWLINE)
          .append(NEWLINE)
        while (r < len - 2) {
          if (names.hasNext) names.next
          r += 1
        }
      }
      r += 1
    }
    sb.toString
  }

  private def clamp(lower: Int, upper: Int, value: Int) = Math
    .max(lower, Math.min(upper, value))

  private def lpad(o: Any, w: Int) = {
    val sb = new StringBuilder
    val value = String.valueOf(o)
    for (i <- value.length until w) sb.append(' ')
    sb.append(value)
    sb.toString
  }

  private def rpad(o: AnyRef, w: Int) = {
    val sb = new StringBuilder
    val value = String.valueOf(o)
    sb.append(value)
    for (i <- value.length until w) sb.append(' ')
    sb.toString
  }

  private def truncate(o: AnyRef, w: Int) = {
    val value = String.valueOf(o)
    if (value.length - ELLIPSES.length > w) value
      .substring(0, w - ELLIPSES.length) + ELLIPSES
    else value
  }

  private def fmt(cls: Class[?], o: Any): String = {
    if (cls == null) return "null"
    var s: String = null
    if (o.isInstanceOf[Number])
      if (
        classOf[Short] == cls || classOf[Int] == cls || classOf[Long] == cls ||
        classOf[BigInteger] == cls
      ) s = String.format("% d", classOf[Number].cast(o).longValue)
      else s = String.format("% .8f", classOf[Number].cast(o).doubleValue)
    else if (o.isInstanceOf[Date]) {
      val dt = classOf[Date].cast(o)
      val cal = Calendar.getInstance
      cal.setTime(dt)
      val fmt = new SimpleDateFormat(
        if (
          cal.get(Calendar.HOUR_OF_DAY) == 0 && cal.get(Calendar.MINUTE) == 0 &&
          cal.get(Calendar.SECOND) == 0
        ) "yyyy-MM-dd"
        else "yyyy-MM-dd'T'HH:mm:ssXXX",
      )
      s = fmt.format(dt)
    } else s = if (o != null) String.valueOf(o) else ""
    s
  }

  @throws[IOException]
  def readCsv(file: String): DataFrame[AnyRef] = readCsv(
    if (file.contains("://")) new URL(file).openStream
    else new FileInputStream(file),
    ",",
    NumberDefault.LONG_DEFAULT,
    null,
  )

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      numDefault: DataFrame.NumberDefault,
  ): DataFrame[AnyRef] = readCsv(
    if (file.contains("://")) new URL(file).openStream
    else new FileInputStream(file),
    separator,
    numDefault,
    null,
  )

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      numDefault: DataFrame.NumberDefault,
      naString: String,
  ): DataFrame[AnyRef] = readCsv(
    if (file.contains("://")) new URL(file).openStream
    else new FileInputStream(file),
    separator,
    numDefault,
    naString,
  )

  @throws[IOException]
  def readCsv(
      file: String,
      separator: String,
      numDefault: DataFrame.NumberDefault,
      naString: String,
      hasHeader: Boolean,
  ): DataFrame[AnyRef] = readCsv(
    if (file.contains("://")) new URL(file).openStream
    else new FileInputStream(file),
    separator,
    numDefault,
    naString,
    hasHeader,
  )

  @throws[IOException]
  def readCsv(input: InputStream): DataFrame[AnyRef] =
    readCsv(input, ",", NumberDefault.LONG_DEFAULT, null)

  @throws[IOException]
  def readCsv(
      input: InputStream,
      separator: String,
      numDefault: DataFrame.NumberDefault,
      naString: String,
  ): DataFrame[AnyRef] = readCsv(input, separator, numDefault, naString, true)

  def readCsv(
      input: InputStream,
      separator: String,
      numDefault: NumberDefault,
      naString: String,
      hasHeader: Boolean,
  ): DataFrame[AnyRef] =
    // Determine CsvPreference based on separator using Scala match
    val csvPreference: CsvPreference = separator match
      case "\\t" => CsvPreference.TAB_PREFERENCE
      case "," => CsvPreference.STANDARD_PREFERENCE
      case ";" => CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE
      case "|" => new CsvPreference.Builder('"', '|', "\n").build()
      case _ => throw new IllegalArgumentException(
          s"Separator: $separator is not currently supported",
        )

    // Use a try-finally block to ensure the reader is closed
    var reader: CsvListReader | Null = null // Use Scala 3 union type for nullable
    try {
      reader = new CsvListReader(new InputStreamReader(input), csvPreference)

      var header: List[String] = null // Use Scala List for header
      var df: DataFrame[AnyRef] = null // Use AnyRef for DataFrame generic type
      var procs: Array[CellProcessor] = null // new Array[CellProcessor](2)//header.size) //null// Use Scala Array for processors, allow null

      if (hasHeader) {
        // reader.getHeader(true) returns Java String[], convert to Scala List[String]
        header = reader.getHeader(true).toList
        procs = new Array[CellProcessor](header.size)
        println(s"header ${header.mkString(",")}")
        // Create Scala Array of CellProcessors, initialized with nulls
//        procs = new CellProcessor(header.size) //(null)
        // Create DataFrame with Scala List header
        df = new DataFrame[AnyRef](header*) // DataFrame constructor might expect Java List
      } else {
        // Read the first row to figure out how many columns we have
        reader.read()
        // Use Scala ListBuffer for mutable header building
        val headerBuffer = ListBuffer[String]()
        // reader.length() returns the number of columns in the last read row
        for (i <- 0 until reader.length()) headerBuffer += s"V$i" // Use Scala string interpolation
        header = headerBuffer.toList // Convert to immutable Scala List
        // Create Scala Array of CellProcessors, initialized with nulls
//        procs = Array.fill[CellProcessor](header.size)(null)
        procs = new Array[CellProcessor](header.size)
        // Create DataFrame with Scala List header
        df = new DataFrame[AnyRef](header*) // DataFrame constructor might expect Java List
        // The following line executes the procs on the previously read row again
        // reader.executeProcessors returns Java List<Object>, convert to Scala List[AnyRef]

        df.append(reader.executeProcessors(procs*).asScala.toList)
      }
      // Read remaining rows using a while loop
      var row: mutable.Buffer[AnyRef] = new mutable.ListBuffer[AnyRef]() // Use Java List for the row read by CsvListReader
      while ({ row = reader.read(procs*).asScala; row != null }) { // Assign and check in the while condition
        // reader.read returns Java List<Object>, convert to Scala List[AnyRef] before appending
        println(s"row ${row.mkString(",")}")
        df.append(row.toSeq)
      }

      // Call convert method on the DataFrame
      df.convert(numDefault, naString)

    } finally
      // Ensure the reader is closed if it was successfully created
      if (reader != null)
        try reader.close()
        catch {
          case e: IOException => // Log or handle the close exception if necessary
            System.err.println(s"Error closing CsvListReader: ${e.getMessage}")
        }
  // @throws[IOException]
//  def readCsv(input: InputStream, separator: String, numDefault: DataFrame.NumberDefault, naString: String, hasHeader: Boolean): DataFrame[AnyRef] = {
//    var csvPreference: CsvPreference = null
//    separator match {
//      case "\\t" =>
//        csvPreference = CsvPreference.TAB_PREFERENCE
//      case "," =>
//        csvPreference = CsvPreference.STANDARD_PREFERENCE
//      case ";" =>
//        csvPreference = CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE
//      case "|" =>
//        csvPreference = new CsvPreference.Builder('"', '|', "\n").build
//      case _ =>
//        throw new IllegalArgumentException("Separator: " + separator + " is not currently supported")
//    }
//    try {
//      val reader = new CsvListReader(new InputStreamReader(input), csvPreference)
//      try {
//        var header:  ListBuffer[String] = null
//        var df: DataFrame[AnyRef] = null
//        var procs: Array[CellProcessor] = null
//        if (hasHeader) {
//          header = List(reader.getHeader(true))
//          procs = new Array[CellProcessor](header.size)
//          df = new DataFrame[AnyRef](header)
//        }
//        else {
//          // Read the first row to figure out how many columns we have
//          reader.read
//          header = new  ListBuffer[String]()
//          for (i <- 0 until reader.length) {
//            header.append("V" + i)
//          }
//          procs = new Array[CellProcessor](header.size)
//          df = new DataFrame[AnyRef](header)
//          // The following line executes the procs on the previously read row again
//          df.append(reader.executeProcessors(procs*))
//        }
//        var row = reader.read(procs*)
//        while (row != null) {
//          df.append(new ListBuffer[AnyRef]())//row))
//          row = reader.read(procs*)
//        }
//        df.convert(numDefault, naString)
//      } finally if (reader != null) reader.close()
//    }
//  }

  @throws[IOException]
  def writeCsv[V](df: DataFrame[V], output: String): Unit =
    writeCsv(df, new FileOutputStream(output))

  @throws[IOException]
  def writeCsv[V](df: DataFrame[V], output: OutputStream): Unit =
    try {
      val writer = new CsvListWriter(
        new OutputStreamWriter(output),
        CsvPreference.STANDARD_PREFERENCE,
      )
      try {
        val header = new Array[String](df.size)
        val it = df.getColumns.iterator
        for (c <- 0 until df.size)
          header(c) = String.valueOf(if (it.hasNext) it.next else c)
        writer.writeHeader(header*)
        val procs = new Array[CellProcessor](df.size)
        val types = df.types
        for (c <- 0 until df.size) {
          val cls = types(c)
          if (classOf[Date].isAssignableFrom(cls)) procs(c) =
            new ConvertNullTo("", new FmtDate("yyyy-MM-dd'T'HH:mm:ssXXX"))
          else procs(c) = new ConvertNullTo("")
        }

        for (row <- df) writer.write(row, procs)
      } finally if (writer != null) writer.close()
    }

  @throws[IOException]
  def readXls(file: String): DataFrame[AnyRef] = readXls(
    if (file.contains("://")) new URL(file).openStream
    else new FileInputStream(file),
  )

  @throws[IOException]
  def readXls(input: InputStream): DataFrame[AnyRef] = {
    val wb = new HSSFWorkbook(input)
    val sheet = wb.getSheetAt(0)
    val columns = new ListBuffer[Any]
    val data = new ListBuffer[Seq[AnyRef]]

    for (row <- sheet.asScala)
      if (row.getRowNum == 0)
        // read header
        for (cell <- row.asScala) columns.append(readCell(cell))
      else {
        // read data values
        val values = new ListBuffer[AnyRef]

        for (cell <- row.asScala) values
          .append(readCell(cell).asInstanceOf[AnyRef])
        data.append(values.toSeq)
      }
    // create data frame
    val df = new DataFrame[AnyRef](columns.map(_.toString).toArray*)

    for (row <- data) df.append(row)
    df.convert
  }

  @throws[IOException]
  def writeXls[V](df: DataFrame[V], output: String): Unit =
    writeXls(df, new FileOutputStream(output))

  @throws[IOException]
  def writeXls[V](df: DataFrame[V], output: OutputStream): Unit = {
    val wb = new HSSFWorkbook
    val sheet = wb.createSheet
    // add header
    var row = sheet.createRow(0)
    val it = df.getColumns.iterator
    for (c <- 0 until df.size) {
      val cell = row.createCell(c)
      writeCell(cell, if (it.hasNext) it.next else c)
    }
    // add data values
    for (r <- 0 until df.length) {
      row = sheet.createRow(r + 1)
      for (c <- 0 until df.size) {
        val cell = row.createCell(c)
        writeCell(cell, df.get(r, c))
      }
    }
    //  write to stream
    wb.write(output)
    output.close()
  }

  private def readCell(cell: Cell): Any = cell.getCellType match {
    case CellType.NUMERIC =>
      if (DateUtil.isCellDateFormatted(cell))
        return DateUtil.getJavaDate(cell.getNumericCellValue)
      cell.getNumericCellValue
    case CellType.BOOLEAN => cell.getBooleanCellValue
    case _ => cell.getStringCellValue
  }

  private def writeCell(cell: Cell, value: Any): Unit =
    if (value.isInstanceOf[Number]) {
      cell.setCellType(CellType.NUMERIC)
      cell.setCellValue(classOf[Number].cast(value).doubleValue)
    } else if (value.isInstanceOf[Date]) {
      val style = cell.getSheet.getWorkbook.createCellStyle
      style.setDataFormat(HSSFDataFormat.getBuiltinFormat("m/d/yy h:mm"))
      cell.setCellStyle(style)
      cell.setCellType(CellType.NUMERIC)
      cell.setCellValue(classOf[Date].cast(value))
    } else if (value.isInstanceOf[Boolean]) cell.setCellType(CellType.BOOLEAN)
    else {
      cell.setCellType(CellType.STRING)
      cell.setCellValue(if (value != null) String.valueOf(value) else "")
    }

  @throws[SQLException]
  def readSql(rs: ResultSet): DataFrame[AnyRef] =
    try {
      val md = rs.getMetaData
      val columns = new ListBuffer[String]()
      for (i <- 1 to md.getColumnCount) columns.append(md.getColumnLabel(i))
      val df = new DataFrame[AnyRef](columns.toArray*)
      val row = new ListBuffer[AnyRef]() // columns.size)
      while (rs.next) {

        for (c <- columns) row.append(rs.getString(c))
        df.append(row.toSeq)
        row.clear()
      }
      df
    } finally rs.close()

  @throws[SQLException]
  def writeSql[V](df: DataFrame[V], stmt: PreparedStatement): Unit =
    try {
      val md = stmt.getParameterMetaData
      val columns = new ListBuffer[Int]
      for (i <- 1 to md.getParameterCount) columns.append(md.getParameterType(i))
      for (r <- 0 until df.length) {
        for (c <- 1 to df.size) stmt.setObject(c, df.get(r, c - 1))
        stmt.addBatch()
      }
      stmt.executeBatch
    } finally stmt.close()
}
