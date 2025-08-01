package torch.pandas.component

import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import torch.csv.{CSVFormat, CSVReader, QUOTE_MINIMAL, Quoting}
import torch.pandas.DataFrame
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object CSVCompat {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def readCSVs(csvPath: String, limit: Int = -1, spliterator: String = ",", header:Option[Seq[String]] = None, needConvert: Boolean = false): DataFrame[AnyRef] = {

    def getCSVFormat(spliterator:String = ","):CSVFormat ={
      given customCSVFormat: CSVFormat = new CSVFormat {
        override val delimiter: Char = spliterator match {
          case ":" => ':'
          case ";" => ';'
          case "\\t" => '\t'
          case " " => ' '
          case "\\" => '\\'
          case _ => ','
        }

        override val quoteChar: Char = '"'

        override val escapeChar: Char = '\\'

        val recordSeparator: String = "\n"

        val ignoreLeadingWhitespace: Boolean = true

        val ignoreTrailingWhitespace: Boolean = true

        // 实现缺失的成员
        override val lineTerminator: String = "\n"
        override val quoting: Quoting = QUOTE_MINIMAL
        override val treatEmptyLineAsNil: Boolean = true
      }
      customCSVFormat
    }
    val csv = CSVReader.open(csvPath)(using getCSVFormat(spliterator))
    //    val csvSeq = csv.toStream.map(_.toSeq).toList
//    val csvHeader = csv.readNext()
//    println(s"csv header ${csvHeader.mkString(",")}")

//    val count = 150 // csvSeq.size
    val df = if header.isDefined then new DataFrame[AnyRef](header.get *) else  new DataFrame[AnyRef](csv.readNext().get *)
    val iter = csv.iterator
    var index = 0
    val mainStartTime = System.nanoTime()
    var preTmpEndTime = System.nanoTime()
    breakable {
      while (iter.hasNext)
        if (index < limit || limit <= -1) {
          val line = iter.next()
//          println(s"num $index, line length ${line.length} line ${line.mkString(",")}")
          df.append(index.toString, line)
          index += 1
          if (index % 10000 == 0) {
            val endTime = System.nanoTime() // 记录结束时间
            val duration = (endTime - mainStartTime) / 1e9 // 将纳秒转换为秒
            val zduration = (endTime - preTmpEndTime) / 1e9
            println(s"csv read progress $index time cost all： $duration s ,this duration time cost $zduration s")
            preTmpEndTime = endTime
          }
        } else {
          println(s"csv read finish! data count $index")
          break
        }

    }
    if needConvert then df.convert else df
  }

  def readCSV(csvPath: String, limit: Int = -1, needConvert: Boolean = false): DataFrame[AnyRef] = {

    val csv = CSVReader.open(csvPath)
    //    val csvSeq = csv.toStream.map(_.toSeq).toList
    val csvHeader = csv.readNext()
    println(s"csv header ${csvHeader.mkString(",")}")
    val count = 150 // csvSeq.size
    val df = new DataFrame[AnyRef](csvHeader.get*)
    val iter = csv.iterator
    var index = 0
    val mainStartTime = System.nanoTime()
    var preTmpEndTime = System.nanoTime()
    breakable {
      while (iter.hasNext)
        if (index < limit || limit == -1) {
          val line = iter.next()
          //          println(s"num $index, line ${line.mkString(",")}")
          df.append(index.toString, line)
          index += 1
          if (index % 10000 == 0) {
            val endTime = System.nanoTime() // 记录结束时间
            val duration = (endTime - mainStartTime) / 1e9 // 将纳秒转换为秒
            val zduration = (endTime - preTmpEndTime) / 1e9
            println(s"csv read progress $index time cost all： $duration s ,this duration time cost $zduration s")
            preTmpEndTime = endTime
          }
        } else {
          println(s"csv read finish! data count $index")
          break
        }

    }
    if needConvert then df.convert else df
  }
}
