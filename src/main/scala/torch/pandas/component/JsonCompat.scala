package torch
package pandas
package component

import com.google.gson.JsonParser
import ujson.*
import scala.util.control.Breaks.{break, breakable}
import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import torch.pandas.DataFrame

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.ClassTag

case class Person(
                   name: String,
                   age: Int,
                   tall: Float,
                   weight: Float,
                   address: String
                 )
object JsonCompat {

  /***
   *
   * @param jsonPath
   * @param limit
   * @return
   */
  def parseJsonLineFromJsonFile(jsonPath: String, limit: Int = -1): Seq[ujson.Value] = {
    val jsonBuffer = new ListBuffer[ujson.Value]()
    val mainStartTime = System.nanoTime()
    var preTmpEndTime = System.nanoTime()
    var index = 0
    breakable{
      Source.fromFile(jsonPath).getLines().foreach { line =>
        try {
          if (index < limit || limit == -1) {
            jsonBuffer.append(ujson.read(line))
            index += 1
            if (index % 1000 == 0) {
              val endTime = System.nanoTime() // 记录结束时间
              val duration = (endTime - mainStartTime) / 1e9 // 将纳秒转换为秒
              val zduration = (endTime - preTmpEndTime) / 1e9
              println(s"json read progress $index time cost all: ${duration} s ,this duration time cost ${zduration} s")
              preTmpEndTime = endTime
            }
          } else {
            println(s"json read finish! data count $index")
            break
          }
        } catch {
          case e: Exception =>
            println(s"parse json line $line meet error: ${e.getMessage}")
        }
      }
    }

    jsonBuffer.toSeq
  }

  /***
   *
   * @param jsonPath
   * @param recursionHeader
   * @param limit
   * //        val value = item(header) match {
   * //          case Str(s) => s
   * //          case Num(n) => n
   * //          case Bool(b) => b
   * //          case Null => null
   * //          case Arr(_) => item(header).toString
   * //          case Obj(_) => item(header).toString
   * //          case other => other.toString
   * //        }
   * @return
   */
  def parseJsonLineToColumnMap(jsonPath: String, recursionHeader: Boolean = false, limit: Int = -1): Map[String, Array[AnyRef]] = {
    val data = parseJsonLineFromJsonFile(jsonPath, limit)
    if (data.isEmpty) return Map.empty
    val result = mutable.Map[String, mutable.ArrayBuffer[AnyRef]]()
    val headers = if !recursionHeader then data.head.obj.keys else {
      data.flatMap(_.obj.keys).toSet
    }
    headers.foreach { header =>
      result(header) = mutable.ArrayBuffer.empty[AnyRef]
    }
    data.foreach { item =>

      headers.foreach { header =>
        val value = item.obj.get(header) match {
          case Some(v) => v match {
            case Str(s) => s
            case Num(n) => n
            case Bool(b) => b
            case Null => null
            case Arr(_) => v.toString
            case Obj(_) => v.toString
            case other => other.toString
          }
          case None => null
        }
        result(header) += value.asInstanceOf[AnyRef]
      }
    }
    result.map { case (k, v) => k -> v.toArray }.toMap
  }

  /***
   *
   * @param jsonPath
   * @param recursionHeader
   * @param limit
   * @return
   */
  def parseJsonLineToDataFrame(jsonPath: String, recursionHeader: Boolean = false, limit: Int = -1): DataFrame[AnyRef] = {
    val jsonStrArrayMap = parseJsonLineToColumnMap(jsonPath, recursionHeader, limit)
    val dataAsSeq = jsonStrArrayMap.values.map(_.toSeq).toList
    val count = jsonStrArrayMap.values.head.length
    new DataFrame[AnyRef]((0 to count).map(_.toString).toSeq, jsonStrArrayMap.keys.toSeq, dataAsSeq)
  }

  /***
   *
   * @param jsonPath
   * @param csvPath
   * @param spliterator
   */
  def parseJsonFileToCSVFile(jsonPath: String, csvPath: String, spliterator:String = ","): Unit = {
//    val jsonStr = Files.readString(Paths.get(jsonPath))
    val jsonBytes = Files.readAllBytes(Paths.get(jsonPath))
    val jsonStr = new String(jsonBytes)
    val jsonData = ujson.read(jsonStr)
    val headers = jsonData.arr.head.obj.keys.toSeq
    val csvHeader = headers.mkString(",") + "\n"
    val csvRows = jsonData.arr.map { item =>
      headers.map { field =>
        item.obj(field).match {
          case Str(s) => s
          case Num(n) => n.toString
          case Bool(b) => b.toString
          case Null => ""
          case other => other.toString
        }
      }.mkString(spliterator)
    }.mkString("\n")
    Files.write(Paths.get(csvPath), (csvHeader + csvRows).getBytes)
  }

  /***
   *
   * @param jsonPath
   * @return
   */
  def parseJsonFileToDataFrame(jsonPath: String): DataFrame[AnyRef] = {
    val jsonStrArrayMap = parseJsonFileToColumnar(jsonPath)
    val dataAsSeq = jsonStrArrayMap.values.map(_.toSeq).toList
    val count = jsonStrArrayMap.values.head.length
    new DataFrame[AnyRef]((0 to count).map(_.toString).toSeq, jsonStrArrayMap.keys.toSeq, dataAsSeq)
  }

  /***
   *
   * @param json
   * @return
   */
  def jsonToCaseClassExample(json: ujson.Value): Person = {
    val name = json("name").str
    val age = json("age").num.toInt
    val tall = json("tall").num.toFloat
    val weight = json("weight").num.toFloat
    val address = json("address").str
    Person(name, age, tall, weight, address)
  }

  /***
   *
   * @param jsonPath
   * @param jsonConvFunc
   * @param tag
   * @tparam T
   * @return
   */
  def parseJsonFileToCaseClassSeq[T](jsonPath: String, jsonConvFunc: ujson.Value => T)(implicit tag: ClassTag[T]): Seq[T] = {
    import java.io.File
    import scala.io.Source
    try
      val fileContent = Source.fromFile(new File(jsonPath)).mkString
      val fileJson = ujson.read(fileContent)
      val jsonSeq = fileJson.arr
      jsonSeq.foreach(ele => println(ele.toString))
      val caseClassSeq = jsonSeq.map(ele => jsonConvFunc(ele))
      caseClassSeq.toSeq
    catch
      case e: Exception =>
        println(s"read json file to case class seq error: ${e.getMessage}")
        Seq.empty[T]
  }

  /***
   *
   * @param jsonPath
   * @return
   */
  def parseJsonFileToColumnar(jsonPath: String): Map[String, Array[AnyRef]] = {
//    val jsonStr = Files.readString(Paths.get(jsonPath))
    val jsonBytes = Files.readAllBytes(Paths.get(jsonPath))
    val jsonStr = new String(jsonBytes)
    val data = ujson.read(jsonStr).arr
    if (data.isEmpty) return Map.empty
    val result = mutable.Map[String, mutable.ArrayBuffer[AnyRef]]()
    val headers = data.head.obj.keys
    headers.foreach { header =>
      result(header) = mutable.ArrayBuffer.empty[AnyRef]
    }
    data.foreach { item =>
      headers.foreach { header =>
        val value = item(header) match {
          case Str(s) => s
          case Num(n) => n
          case Bool(b) => b
          case Null => null
          case other => other.toString
        }
        result(header) += value.asInstanceOf[AnyRef]
      }
    }
    result.map { case (k, v) => k -> v.toArray }.toMap
  }
}
