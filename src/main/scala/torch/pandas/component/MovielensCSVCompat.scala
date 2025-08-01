package torch.pandas.component

import java.nio.file.Files
import java.nio.file.Paths
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import torch.csv.CSVFormat
import torch.csv.CSVReader
import torch.csv.QUOTE_MINIMAL
import torch.csv.Quoting
import torch.pandas.DataFrame

object RatingCSVCompat {

  def main(args: Array[String]): Unit = {
    val header = Seq("userId", "movieId", "rating", "timestamp")
    val path = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\ratings.dat"
    val path2 = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\movies.dat"
    val path3 = "D:\\data\\git\\testNumpy\\src\\main\\resources\\ml-1m\\users.dat"
    val df = RatingCSVCompat.readCSV(
      path,
      Some(header),limit =500
    )(using customCSVFormat)
    df.heads(100).show()
  }

  given customCSVFormat: CSVFormat = new CSVFormat {
    override val delimiter: Char = ':'

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

  def readCSV(
      csvPath: String,
      header: Option[Seq[String]] = None,
      limit: Int = -1,
      needConvert: Boolean = false
  )(using customCSVFormat: CSVFormat = RatingCSVCompat.customCSVFormat): DataFrame[AnyRef] = {
    // 使用自定义的 CSVFormat 打开 CSV 文件
    val csv = CSVReader.open(csvPath)(using customCSVFormat)

//    val csvHeader = csv.readNext()
//    val count = 150 // csvSeq.size
   
    val iter = csv.iterator
    val df = if header.isDefined then new DataFrame[AnyRef](header.get *) else new DataFrame[AnyRef](iter.next() *)
    var index = 0
    val mainStartTime = System.nanoTime()
    var preTmpEndTime = System.nanoTime()
    breakable {
      while (iter.hasNext)
        if (index < limit || limit <= -1) {
          val line = iter.next()
          val processedLine = line.sliding(2, 2).collect {
            case Seq(value, _) => value
            case Seq(value) => value // 处理最后一个单独的元素
          }.toSeq

          df.append(index.toString, processedLine)
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

// 处理由于分隔符为 :: 导致的多余空字段
//          val processedLine = line.grouped(2).collect {
//            case Seq(value, _ ) => value
//          }.toSeq
