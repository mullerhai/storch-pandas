package torch.pandas.component

import java.nio.file.Files
import java.nio.file.Paths

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import torch.csv.CSVReader
import torch.pandas.DataFrame
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object CSVCompat {

  private val logger = LoggerFactory.getLogger(this.getClass)
  
  def readCSV(csvPath: String, limit: Int = -1): DataFrame[AnyRef] = {

    val csv = CSVReader.open(csvPath)
    //    val csvSeq = csv.toStream.map(_.toSeq).toList
    val csvHeader = csv.readNext()
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
          if (index % 1000 == 0) {
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
    df
  }
}
