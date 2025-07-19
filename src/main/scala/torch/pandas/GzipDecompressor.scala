package torch.pandas

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream, RandomAccessFile}
import java.util.zip.GZIPInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object MultiThreadedGzipDecompressor {

  // 每个线程处理的块大小，单位为字节
  private val CHUNK_SIZE = 1024 * 1024 * 10 // 10 MB

  def decompressGzip(gzipFilePath: String, outputFilePath: String): Unit = {
    // 获取执行上下文，用于管理线程
    given ExecutionContext = ExecutionContext.global

    // 读取 GZIP 文件
    val gzipFile = new RandomAccessFile(gzipFilePath, "r")
    val fileLength = gzipFile.length()

    // 计算块的数量
    val chunkCount = (fileLength + CHUNK_SIZE - 1) / CHUNK_SIZE

    // 存储每个块的解压任务
    val futures = (0 until chunkCount.toInt).map { chunkIndex =>
      Future {
        decompressChunk(gzipFilePath, outputFilePath, chunkIndex, CHUNK_SIZE)
      }
    }

    // 等待所有任务完成
    val allFutures = Future.sequence(futures)
    allFutures.onComplete {
      case Success(_) =>
        println("GZIP decompress done")
        gzipFile.close()
      case Failure(ex) =>
        println(s"Error during decompression: ${ex.getMessage}")
        gzipFile.close()
    }

    // 阻塞主线程，直到所有任务完成
    scala.concurrent.Await.result(allFutures, scala.concurrent.duration.Duration.Inf)
  }

  private def decompressChunk(gzipFilePath: String, outputFilePath: String, chunkIndex: Int, chunkSize: Long): Unit = {
    val startOffset = chunkIndex * chunkSize
    var gis: GZIPInputStream = null
    var fos: FileOutputStream = null
    try {
      // 创建 GZIP 输入流
      val fis = new FileInputStream(gzipFilePath)
      fis.skip(startOffset)
      gis = new GZIPInputStream(new BufferedInputStream(fis))

      // 创建文件输出流，以追加模式打开
      fos = new FileOutputStream(outputFilePath, true)
      val buffer = new Array[Byte](1024 * 1024)
      var len: Int = 0
      var bytesRead: Long = 0

      // 从 GZIP 输入流读取数据并写入输出文件
      while ( {
        len = gis.read(buffer)
        len > -1 && bytesRead < chunkSize
      }) {
        fos.write(buffer, 0, len)
        bytesRead += len
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 关闭资源
      if (gis != null) gis.close()
      if (fos != null) fos.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val gzipFilePath = "train.gz"
    val outputFilePath = "train.csv"
    decompressGzip(gzipFilePath, outputFilePath)
  }
}
object GzipDecompressor {

  def decompressGzip(gzipFilePath: String, outputFilePath: String, readCache: Int = 100): Unit = {
    var gis: GZIPInputStream = null
    var fos: FileOutputStream = null
    try {
      println("Begin decompress GZIP file")
      // 创建 GZIP 输入流
      gis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFilePath)))
      // 创建文件输出流
      fos = new FileOutputStream(outputFilePath)
      val buffer = new Array[Byte](1024 * 1024 * readCache)
      var len: Int = 0
      // 从 GZIP 输入流读取数据并写入输出文件
      while ( {
        len = gis.read(buffer)
        len > -1
      }) {
        fos.write(buffer, 0, len)
      }
      println("data convert decompress done")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 关闭资源
      if (gis != null) gis.close()
      if (fos != null) fos.close()
      println("GZIP decompress done")
    }
  }


}
