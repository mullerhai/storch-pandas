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

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import torch.pandas.DataFrame
import torch.pandas.DataFrame.Aggregate
import torch.pandas.DataFrame.KeyFunction
import torch.pandas.service.Aggregation
import torch.pandas.service.Aggregation.Unique
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object Pivoting {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def pivot[V](
      df: DataFrame[V],
      rows: Array[Int],
      cols: Array[Int],
      values: Array[Int],
  ): DataFrame[V] = {
//    println(s"DataFrame Pivoting ->groupBy  pivot rows ${rows.mkString(",")} cols ${cols.mkString(",")} values ${values.mkString(",")}")
    val grouped = df.groupBy_index(rows*)
    val exploded = grouped.explode
    val aggregates = new mutable.LinkedHashMap[Int, Aggregation.Unique[V]]

    for (entry <- exploded.toSeq) exploded
      .put(entry._1, entry._2.groupBy_index(cols*))
    for (v <- values) aggregates.put(v, new Aggregation.Unique[V])
    val groupCols = grouped.groups.getColumns()
    pivot(exploded, aggregates, groupCols)
  }

  def pivot[I, O](
      df: DataFrame[I],
      rows: DataFrame.KeyFunction[I],
      cols: DataFrame.KeyFunction[I],
      values: LinkedHashMap[Int, ? <: DataFrame.Aggregate[I, O]],
  ): DataFrame[O] = {
    val grouped = df.groupBy(rows)
    val exploded = grouped.explode

    for (entry <- exploded) exploded.put(entry._1, entry._2.groupBy(cols))
    pivot(exploded, values, grouped.groups.getColumns())
  }
// columns: mutable.LinkedHashSet[Int]
  private def pivot[I, O](
      grouped: mutable.LinkedHashMap[AnyRef, DataFrame[I]],
      values: mutable.LinkedHashMap[Int, ? <: Aggregate[I, O]],
      columns: Set[Int],
  ): DataFrame[O] = {
    val pivotCols = mutable.LinkedHashSet.empty[AnyRef]
    val pivotData = mutable.LinkedHashMap
      .empty[AnyRef, mutable.LinkedHashMap[AnyRef, mutable.ListBuffer[I]]]
    val pivotFunctions = mutable.LinkedHashMap.empty[AnyRef, Aggregate[I, O]]
    val colNames = new ListBuffer[AnyRef]() //
    grouped.values.iterator.next.getColumns.foreach(colNames.append(_))
//    val colNames = mutable.ListBuffer.from(grouped.values.head.columns())

    // allocate row -> column -> data maps
    for ((rowKey, rowDf) <- grouped) {
      val rowData = mutable.LinkedHashMap.empty[AnyRef, mutable.ListBuffer[I]]
      for (c <- columns) {
        val colName = colNames(c)
        rowData(colName) = mutable.ListBuffer.empty[I]
        pivotCols.add(colName)
      }
      for (colKey <- rowDf.groups.keys()) for (c <- values.keys) {
        val colName = name(colKey, colNames(c), values)
        rowData(colName) = mutable.ListBuffer.empty[I]
        pivotCols.add(colName)
        pivotFunctions(colName) = values(c)
      }
      pivotData(rowKey) = rowData
    }

    // collect data for row and column groups
    for ((rowKey, rowDf) <- grouped) {
      val rowData = pivotData(rowKey)
      val byCol = rowDf.explode
      for ((colKey, colDf) <- byCol) {
        // add columns used as pivot rows
        for (c <- columns) {
          val colName = colNames(c)
          val colData = rowData(colName)
          // optimization, only add first value
          // since the values are all the same (due to grouping)
          colData += colDf.getFromIndex(0, c)
        }

        // add values for aggregation
        for (c <- values.keys) {
          val colName = name(colKey, colNames(c), values)
          val colData = rowData(colName)
          colData ++= colDf.colInt(c.toInt)
        }
      }
    }

    val pivotEmptyData: List[Seq[O]] = ListBuffer[Seq[O]]().toList
    // iterate over row, column pairs and apply aggregate functions
    val pivot =
      new DataFrame[O](pivotData.keys.toSeq, pivotCols.toSeq, pivotEmptyData)
    for {
      col <- pivot.getColumns
      row <- pivot.getIndex
    } pivotData.get(row).flatMap(_.get(col)) match {
      case Some(data) => pivotFunctions.get(col) match {
          case Some(func) => pivot
              .set(row, col, func.apply(data.toSeq).asInstanceOf[O])
          case None => pivot.set(row, col, data.head.asInstanceOf[O])
        }
      case None =>
    }

    pivot
  }

  @SuppressWarnings(Array("unchecked"))
  private def pivotOld[I, O](
      grouped: LinkedHashMap[AnyRef, DataFrame[I]],
      values: LinkedHashMap[Int, ? <: DataFrame.Aggregate[I, O]],
      columns: Set[Int],
  ) = {
    val pivotColsBak = new ListBuffer[AnyRef]
    val pivotCols = new mutable.LinkedHashSet[AnyRef]
    val pivotData =
      new mutable.LinkedHashMap[AnyRef, LinkedHashMap[AnyRef, Seq[AnyRef]]]
    val pivotFunctions =
      new mutable.LinkedHashMap[AnyRef, DataFrame.Aggregate[I, ?]]
    val colNames = new ListBuffer[AnyRef]() //
    grouped.values.iterator.next.getColumns.foreach(colNames.append(_))
    // allocate row -> column -> data maps

    for (rowEntry <- grouped) {
      val rowData = new mutable.LinkedHashMap[AnyRef, Seq[AnyRef]]

      for (c <- columns) {
        val colName = colNames(c)
        rowData.put(colName, Seq.empty)
        pivotCols.add(colName)
        pivotColsBak.append(colName)
      }

      for (colKey <- rowEntry._2.groups.keys())

        for (c <- values.keySet) {
          val colName = name(colKey, colNames(c), values)
          rowData.put(colName, Seq.empty)
          pivotCols.add(colName)
          pivotColsBak.append(colName)
          pivotFunctions.put(colName, values.get(c).get)
        }
//      println(s"DataFrame Pivoting 1 ->pivot row ${rowEntry._1} rowData ${rowData.mkString(",")}")
      pivotData.put(rowEntry._1, rowData)
    }
    // collect data for row and column groups

    for (rowEntry <- grouped) {
      val rowName = rowEntry._1
      val rowData = pivotData.get(rowName)
      val byCol = rowEntry._2.explode

      for (colEntry <- byCol) {
        // add columns used as pivot rows

        for (c <- columns) {
          val colName = colNames(c)
          val colData: ArrayBuffer[I] = rowData.get(colName).toBuffer
            .asInstanceOf[ArrayBuffer[I]]
          // optimization, only add first value
          // since the values are all the same (due to grouping)
          val kk = colEntry._2.getFromIndex(0, c) // .asInstanceOf[Seq[AnyRef]]
          colData.append(kk)
        }
        // add values for aggregation

        for (c <- values.keySet) {
          val colName = name(colEntry._1, colNames(c), values)
          val colData = rowData.get(colName)
          colData.++(colEntry._2.colInt(c))
        }
      }
    }
    // iterate over row, column pairs and apply aggregate functions
    val data: List[Seq[O]] = ListBuffer[Seq[O]]().toList
    val pivotIndex = pivotData.toSeq.map(_._1)
    val pivotNewCols = pivotCols.toSeq.asInstanceOf[Seq[AnyRef]]
    val pivot: DataFrame[O] = new DataFrame[O](pivotIndex, pivotNewCols, data)

    for (col <- pivot.getColumns)

      for (row <- pivot.getIndex) {
        val data = pivotData.get(row).get(col)
//        println(s"DataFrame Pivoting ->pivot row ${row} col ${col} data ${data}")
        if (data != null && data.size > 0) {
          val func = pivotFunctions.getOrElse(col, null)
          if (func != null) pivot.set(
            row,
            col,
            func.apply(data.map(_.asInstanceOf[I])).asInstanceOf[O],
          )
          else pivot.set(row, col, data(0).asInstanceOf[O])
        }
      }
    pivot
  }

  private def name(key: AnyRef, name: AnyRef, values: LinkedHashMap[?, ?]) = {
    var colName = key
    // if multiple value columns are requested the
    // value column name must be added to the pivot column name
    if (values.size > 1) {
      val tmp = new ListBuffer[AnyRef]()
      tmp.append(name)
      if (key.isInstanceOf[Seq[AnyRef]])

        for (col <- classOf[Seq[AnyRef]].cast(key)) tmp.append(col)
      else tmp.append(key)
      colName = tmp
    }
    colName
  }

}
