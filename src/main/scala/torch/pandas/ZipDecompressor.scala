package torch.pandas

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

object ZipDecompressor {

  def decompressZip(zipFilePath: String, outputDirectory: String, readCache: Int = 100): Unit = {
 
    val outputDir = new File(outputDirectory)
    if (!outputDir.exists()) {
      outputDir.mkdirs()
    }
    val fis = new FileInputStream(zipFilePath)
    val bis = new BufferedInputStream(fis)
    val zis = new ZipInputStream(bis)
    try {
      var entry: ZipEntry = zis.getNextEntry()
      while (entry != null) {
        val entryFilePath = new File(outputDirectory, entry.getName)
        if (entry.isDirectory) {
          entryFilePath.mkdirs()
        } else {
          entryFilePath.getParentFile().mkdirs()
          val fos = new FileOutputStream(entryFilePath)
          val bos = new BufferedOutputStream(fos)
          try {
            val buffer = new Array[Byte](1024 * 1024 * readCache)
            var len: Int = zis.read(buffer)
            while (len > 0) {
              bos.write(buffer, 0, len)
              len = zis.read(buffer)
            }
          } finally {
            bos.close()
            fos.close()
          }
        }
        zis.closeEntry()
        entry = zis.getNextEntry()
      }
      println("ZIP decompress done")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      zis.close()
      bis.close()
      fis.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val zipFilePath = "file.zip"
    val outputDirectory = "directory"
    decompressZip(zipFilePath, outputDirectory)
  }
}
