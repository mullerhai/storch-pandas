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
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import Aggregation.Unique
import torch.DataFrame
import torch.DataFrame.Aggregate
import torch.DataFrame.KeyFunction

object Pivoting {
  def pivot[V](
      df: DataFrame[V],
      rows: Array[Int],
      cols: Array[Int],
      values: Array[Int],
  ): DataFrame[V] = {
    val grouped = df.groupBy(rows)
    val exploded = grouped.explode
    val aggregates = new mutable.LinkedHashMap[Int, Aggregation.Unique[V]]

    for (entry <- exploded.toSeq) exploded.put(entry._1, entry._2.groupBy(cols))
    for (v <- values) aggregates.put(v, new Aggregation.Unique[V])
    pivot(exploded, aggregates, grouped.groups.getColumns())
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

  @SuppressWarnings(Array("unchecked"))
  private def pivot[I, O](
      grouped: LinkedHashMap[Any, DataFrame[I]],
      values: LinkedHashMap[Int, ? <: DataFrame.Aggregate[I, O]],
      columns: Set[Int],
  ) = {
    val pivotCols = new mutable.LinkedHashSet[Any]
    val pivotData = new mutable.LinkedHashMap[Any, LinkedHashMap[Any, Seq[Any]]]
    val pivotFunctions = new mutable.LinkedHashMap[Any, DataFrame.Aggregate[I, ?]]
    val colNames = new ListBuffer[AnyRef]() // grouped.values.iterator.next.columns)
    // allocate row -> column -> data maps

    for (rowEntry <- grouped) {
      val rowData = new mutable.LinkedHashMap[Any, Seq[Any]]

      for (c <- columns) {
        val colName = colNames(c)
        rowData.put(colName, Seq.empty)
        pivotCols.add(colName)
      }

      for (colKey <- rowEntry._2.groups.keys())

        for (c <- values.keySet) {
          val colName = name(colKey, colNames(c), values)
          rowData.put(colName, Seq.empty)
          pivotCols.add(colName)
          pivotFunctions.put(colName, values.get(c).get)
        }
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
          val colData = rowData.get(colName).toBuffer
          // optimization, only add first value
          // since the values are all the same (due to grouping)
          colData.append(colEntry._2.get(0, c))
        }
        // add values for aggregation

        for (c <- values.keySet) {
          val colName = name(colEntry._1, colNames(c), values)
          val colData = rowData.get(colName)
          colData.++(colEntry._2.col(c))
        }
      }
    }
    // iterate over row, column pairs and apply aggregate functions
    val pivot: DataFrame[O] = new DataFrame[O](pivotData.keySet, pivotCols)

    for (col <- pivot.getColumns)

      for (row <- pivot.getIndex) {
        val data = pivotData.get(row).get(col)
        if (data != null) {
          val func = pivotFunctions.get(col).get
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

  private def name(key: Any, name: AnyRef, values: LinkedHashMap[?, ?]) = {
    var colName = key
    // if multiple value columns are requested the
    // value column name must be added to the pivot column name
    if (values.size > 1) {
      val tmp = new ListBuffer[Any]()
      tmp.append(name)
      if (key.isInstanceOf[Seq[AnyRef]])

        for (col <- classOf[Seq[AnyRef]].cast(key)) tmp.append(col)
      else tmp.append(key)
      colName = tmp
    }
    colName
  }

}
