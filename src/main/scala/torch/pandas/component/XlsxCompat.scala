package torch.pandas.component

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import torch.pandas.DataFrame

import java.io.FileInputStream
import scala.collection.mutable.ListBuffer

object XlsxCompat {

  def readCell(cell: org.apache.poi.ss.usermodel.Cell): AnyRef = {
    cell.getCellType match {
      case org.apache.poi.ss.usermodel.CellType.STRING => cell.getStringCellValue
      case org.apache.poi.ss.usermodel.CellType.NUMERIC =>
        if (org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(cell)) {
          cell.getDateCellValue
        } else {
          cell.getNumericCellValue.asInstanceOf[AnyRef]
        }
      case org.apache.poi.ss.usermodel.CellType.BOOLEAN => cell.getBooleanCellValue.asInstanceOf[AnyRef]
      case org.apache.poi.ss.usermodel.CellType.FORMULA => cell.getCellFormula
      case _ => null
    }
  }

  def readXlsx(xlsxFilePath: String): DataFrame[AnyRef] = {
    val path = new FileInputStream(xlsxFilePath)
    //    import org.apache.poi.ss.
    val wb = new XSSFWorkbook(path)
    //    val wb = new HSSFWorkbook(path)
    val sheet = wb.getSheetAt(0)
    val columns = new ListBuffer[Any]
    val data = new ListBuffer[Seq[AnyRef]]
    val sheetIterator = sheet.iterator()

    while (sheetIterator.hasNext) {
      val row = sheetIterator.next()
      val rowIter = row.iterator()

      if (row.getRowNum == 0)
        // read header
        while (rowIter.hasNext) {
          columns.append(readCell(rowIter.next()))
        }
      else {
        // read data values
        val values = new ListBuffer[AnyRef]

        while (rowIter.hasNext) {
          val cell = rowIter.next()
          values
            .append(readCell(cell).asInstanceOf[AnyRef])
        }
        data.append(values.toSeq)
      }
    }
    // create data frame
    val df = new DataFrame[AnyRef](columns.map(_.toString).toArray *)

    for (row <- data) df.append(row)
    df.convert

  }
}
