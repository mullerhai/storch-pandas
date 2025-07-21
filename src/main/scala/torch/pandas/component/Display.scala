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
package torch.pandas.component

//import org.knowm.xchart.style.C
import com.xeiam.xchart.StyleManager.ChartType
import com.xeiam.xchart.*
import org.apache.commons.math3.stat.regression.SimpleRegression
//import org.knowm.xchart.XChartPanel
//import org.knowm.xchart.internal.ChartBuilder
//import org.knowm.xchart.internal.chartpart.Chart
//import org.knowm.xchart.internal.series.Series
import torch.pandas.DataFrame
import torch.pandas.DataFrame.PlotType

import java.awt.{Color, Container, GridLayout}
import java.util
import java.util.{Calendar, Date}
import javax.swing.{JFrame, JScrollPane, JTable, SwingUtilities}
import javax.swing.table.AbstractTableModel
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.jdk.CollectionConverters.*

object Display {
  def draw[C <: Container, V](
      df: DataFrame[V],
      container: C,
      plotType: DataFrame.PlotType,
  ): C = {
    val panels = new ListBuffer[XChartPanel]
    val numeric: DataFrame[Number] = df.numeric.fillna(0)
    val rows = Math.ceil(Math.sqrt(numeric.size)).toInt
    val cols = numeric.size / rows + 1
    var xdata = new ListBuffer[Any]() // df.length)
    val it = df.getIndex.iterator
    for (i <- 0 until df.length) {
      val value = if (it.hasNext) it.next else i
      if (value.isInstanceOf[Number] || value.isInstanceOf[Date])
        xdata.append(value)
      else if (PlotType.BAR == plotType) {
        println(s"xdata ${xdata.mkString(", ")} |-> value ${value} |-> value str ${String.valueOf(value)}")
        xdata.append(String.valueOf(value))
      } else xdata.append(i)
    }
    if (Set(PlotType.GRID, PlotType.GRID_WITH_TREND).contains(plotType))

      for (col <- numeric.getColumns) {
        val chart = new ChartBuilder().chartType(chartType(plotType))
          .width(800 / cols).height(800 / cols).title(String.valueOf(col)).build
        val numericCol = numeric.col(col.toString).map(_.doubleValue()).toArray
        val series = chart.addSeries(
          String.valueOf(col),
          xdata.map(_.asInstanceOf[Int]*1.0d).toArray,//.asInstanceOf[Array[Double]],
          numericCol, //numeric.col(col).map(_.asInstanceOf[Int]*1.0d).toArray.asInstanceOf[Array[Double]],
        )
        if (plotType eq PlotType.GRID_WITH_TREND) {
          addTrend(chart, series, xdata.toSeq)
          series.setLineStyle(SeriesLineStyle.NONE)
        }
        chart.getStyleManager.setLegendVisible(false)
        chart.getStyleManager.setDatePattern(dateFormat(xdata.toSeq))
        panels.append(new XChartPanel(chart))
      }
    else {
      val chart = new ChartBuilder().chartType(chartType(plotType)).build
      chart.getStyleManager.setDatePattern(dateFormat(xdata.toSeq))
      plotType match {
        case PlotType.SCATTER =>
        case PlotType.SCATTER_WITH_TREND =>
        case PlotType.LINE_AND_POINTS =>
        case _ => chart.getStyleManager.setMarkerSize(0)
      }

      for (col <- numeric.getColumns) {
        println(s"xdata ${xdata.mkString(", ")}")
        val xdata2 = Array(1,2,3,4,5,6)
        val series = chart.addSeries(
          String.valueOf(col),
          xdata2.map(_.asInstanceOf[Int].toDouble).toArray, //.asInstanceOf[Array[Double]],
          numeric.col(col.toString).map(_.doubleValue()).toArray,
        )
        if (plotType eq PlotType.SCATTER_WITH_TREND) {
          addTrend(chart, series, xdata2.toSeq)
          series.setLineStyle(SeriesLineStyle.NONE)
        }
      }
      panels.append(new XChartPanel(chart))
    }
    if (panels.size > 1) container.setLayout(new GridLayout(rows, cols))

    for (p <- panels) container.add(p)
    container
  }

  def plot[V](df: DataFrame[V], plotType: DataFrame.PlotType): Unit =
    SwingUtilities.invokeLater(new Runnable() {
      override def run(): Unit = {
        val frame = draw(df, new JFrame(title(df)), plotType)
        frame.setDefaultCloseOperation(2) // DISPOSE_ON_CLOSE)
        frame.pack()
        frame.setVisible(true)
      }
    })

  def show[V](df: DataFrame[V]): Unit = {
    val columns = new ListBuffer[Any]() // df.columns)
    columns.addAll(df.getColumns)
    val types = df.types
    SwingUtilities.invokeLater {
      new Runnable() {
        override def run(): Unit = {
          val frame = new JFrame(title(df))
          val table = new JTable(new AbstractTableModel() {
            override def getRowCount: Int = df.length

            override def getColumnCount: Int = df.size

            override def getValueAt(row: Int, col: Int): V = df.getFromIndex(row, col)

            override def getColumnName(col: Int): String = String
              .valueOf(columns(col))

            override def getColumnClass(col: Int): Class[?] = types(col)
          })
          table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF)
          frame.setDefaultCloseOperation(2) // JFrame.DISPOSE_ON_CLOSE)
          frame.add(new JScrollPane(table))
          frame.pack()
          frame.setVisible(true)
        }
      }
    }
  }

  private def chartType(plotType: DataFrame.PlotType): ChartType =
    // Use a Scala match expression which is the equivalent of a Java switch
    plotType match {
      case PlotType.AREA => ChartType.Area
      case PlotType.BAR => ChartType.Bar
      // Combine multiple cases using |
      case PlotType.GRID | PlotType.SCATTER => ChartType.Scatter
      // Combine multiple cases using |
      case PlotType.SCATTER_WITH_TREND | PlotType.GRID_WITH_TREND | PlotType
            .LINE => ChartType.Line
      // The default case
      case _ => ChartType.Line // Explicitly handle default, though the previous case covers LINE
    }
//  private def chartType(plotType: DataFrame.PlotType) = plotType match {
//    case PlotType.AREA =>
//      ChartType.Area
//    case PlotType.BAR =>
//      ChartType.Bar
//    case PlotType.GRID =>
//      ChartType.Scatter
//    case PlotType.SCATTER =>
//      ChartType.Scatter
//    case PlotType.SCATTER_WITH_TREND =>
//      ChartType.Scatter
//    case PlotType.GRID_WITH_TREND =>
//
//    case PlotType.LINE =>
//    case _ =>
//      ChartType.Line
//  }

  private def title(df: DataFrame[?]) = String.format(
    "%s (%d rows x %d columns)",
    df.getClass.getCanonicalName,
    df.length,
    df.size,
  )

  private def dateFormat(xdata: Seq[Any]): String = {
    val fields = Array[Int](
      Calendar.YEAR,
      Calendar.MONTH,
      Calendar.DAY_OF_MONTH,
      Calendar.HOUR_OF_DAY,
      Calendar.MINUTE,
      Calendar.SECOND,
    )
    val formats = Array[String](" yyy", "-MMM", "-d", " H", ":mm", ":ss")
    val c1 = Calendar.getInstance
    val c2 = Calendar.getInstance
    if (!xdata.isEmpty && xdata(0).isInstanceOf[Date]) {
      var format = ""
      var first = 0
      var last = 0
      c1.setTime(classOf[Date].cast(xdata(0)))
      // iterate over all x-axis values comparing dates
      for (i <- 1 until xdata.size) {
        // early exit for non-date elements
        if (!xdata(i).isInstanceOf[Date]) return formats(0).substring(1)
        c2.setTime(classOf[Date].cast(xdata(i)))
        // check which components differ, those are the fields to output
        for (j <- 1 until fields.length)
          if (c1.get(fields(j)) != c2.get(fields(j))) {
            first = Math.max(j - 1, first)
            last = Math.max(j, last)
          }
      }
      // construct a format string for the fields that differ
      var i = first
      while (i <= last && i < formats.length) {
        val formatTmp =
          if (format.isEmpty) formats(i).substring(1) else formats(i)
        format += formatTmp
        i += 1
      }
      return format
    }
    formats(0).substring(1)
  }

  private def addTrend(chart: Chart, series: Series, xdata: Seq[Any]): Unit = {
    val model = new SimpleRegression
    val y = series.getYData.iterator
    var x = 0
    while (y.hasNext) {
      model.addData(x, y.next.doubleValue)
      x += 1
    }
    val mc = series.getMarkerColor
    val c = new Color(mc.getRed, mc.getGreen, mc.getBlue, 0x60)
    val x2: Array[Double] = Array(
      xdata(0).asInstanceOf[Int]*1.0d,
      xdata(xdata.size - 1).asInstanceOf[Int]*1.0d,
    )
    val x3 = Array(model.predict(0), model.predict(xdata.size - 1))
    val trend = chart.addSeries(series.getName + " (trend)", x2, x3)
    trend.setLineColor(c)
    trend.setMarker(SeriesMarker.NONE)
  }
}
