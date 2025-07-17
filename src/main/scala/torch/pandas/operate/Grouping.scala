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

import torch.pandas.function.CumulativeFunction
import torch.pandas.operate.SparseBitSet

import scala.collection.Set as KeySet
import scala.language.postfixOps
//import torch.pandas.operate.Transforms.CumulativeFunction
import torch.pandas.DataFrame
import torch.pandas.DataFrame.{Aggregate, Function, KeyFunction}

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}

class Grouping[V] extends Iterable[(AnyRef, SparseBitSet)] {

//  def this() ={ }

  private var groups: mutable.LinkedHashMap[AnyRef, SparseBitSet] =
    mutable.LinkedHashMap.empty

  private var columns: mutable.LinkedHashSet[Int] = mutable.LinkedHashSet.empty

  def this(df: DataFrame[V], function: KeyFunction[V], cols: Int*) = {
    this()
    val iter = df.iterator
    var r = 0
    while (iter.hasNext) {
      val row = iter.next()
      val key = function.apply(row.toList)
      val group = groups.getOrElseUpdate(key, new SparseBitSet())
      group.set(r)
      r += 1
    }
    cols.foreach(columns.add)
  }

  def this(df: DataFrame[V], cols: Int*) = this(
    df,
    if (cols.length == 1) new KeyFunction[V] {

//          override def apply(value: Seq[V]): AnyRef = value(cols(0))

//          override def apply(value: java.util.List[V]): Any = value.get(cols(0))

      override def apply(value: Seq[V]): AnyRef = value(cols(0))
        .asInstanceOf[AnyRef]
    }
    else new KeyFunction[V] {

//          override def apply(value: java.util.List[V]): Any = {
//            val keyList = new mutable.ListBuffer[Any]()
//            cols.foreach(c => keyList.addOne(value.get(c)))
//            keyList.toList
//          }

      override def apply(value: Seq[V]): AnyRef = {
        val keyList = new mutable.ListBuffer[AnyRef]()
        cols.foreach(c => keyList.addOne(value(c).asInstanceOf[AnyRef]))
        keyList.toList
      }
    },
    cols*,
  )

  def apply(df: DataFrame[V], function: Function[?, ?]): DataFrame[V] = {
    if (df.isEmpty) return df

    val grouped = mutable.ListBuffer[mutable.ListBuffer[V]]()
    val names = df.getColumns.toList
    val newcols = mutable.ListBuffer[AnyRef]()
    val index = mutable.ListBuffer[AnyRef]()

    // construct new row index
    if (function.isInstanceOf[Aggregate[?, ?]] && groups.nonEmpty)
      then index.addAll(groups.keys)

    // add key columns
    columns.foreach(c =>
      if (function.isInstanceOf[Aggregate[?, ?]] && groups.nonEmpty) {
        val column = mutable.ListBuffer[V]()
        groups.foreach { case (_, rows) =>
          val r = rows.nextSetBit(0)
          column.addOne(df.getFromIndex(r, c))
        }
        grouped.addOne(column)
        newcols.addOne(names(c))
      } else {
        grouped.addOne(df.colInt(c).to(mutable.ListBuffer))
        newcols.addOne(names(c))
      },
    )

    // add aggregated data columns
    for (c <- 0 until df.size) if (!columns.contains(c)) {
      val column = mutable.ListBuffer[V]()
      if (groups.isEmpty) {
        try
          if (function.isInstanceOf[Aggregate[?, ?]]) {
            val colValue =  df.colInt(c).toList
            println(s"Grouping apply here if  -> colValue ${colValue. mkString(",")}")
            val conv = function.asInstanceOf[Aggregate[V, V]].apply(colValue)
            val convv = conv.asInstanceOf[V]
            column.addOne(convv)
//            column.addOne(
//            function.asInstanceOf[Aggregate[V, V]].apply()
//              .asInstanceOf[V],
//          )
          } else for (r <- 0 until df.length)
            val rowValue = df.getFromIndex(r, c)
            println("Group apply here else")
            val conv =  function.asInstanceOf[Function[V, V]].apply(rowValue).asInstanceOf[V]
            column.addOne(conv)
        catch { case ex: ClassCastException =>
          println(s"Grouping apply meet Exception-> ex: ${ex.getMessage}")
          throw ex }

        if (function.isInstanceOf[CumulativeFunction[?, ?]]) function
          .asInstanceOf[CumulativeFunction[V, V]].reset()
      } else groups.foreach { case (_, rows) =>
        try
          if (function.isInstanceOf[Aggregate[?, ?]]) {
            val values = mutable.ListBuffer[V]()
            var r = rows.nextSetBit(0)
            while (r >= 0) {
              values.addOne(df.getFromIndex(r, c))
              r = rows.nextSetBit(r + 1)
            }
            column.addOne(
              function.asInstanceOf[Aggregate[V, V]].apply(values.toList)
                .asInstanceOf[V],
            )
          } else {
            var r = rows.nextSetBit(0)
            while (r >= 0) {
              column.addOne(
                function.asInstanceOf[Function[V, V]].apply(df.getFromIndex(r, c))
                  .asInstanceOf[V],
              )
              r = rows.nextSetBit(r + 1)
            }
          }
        catch { case _: ClassCastException => () }

        if (function.isInstanceOf[CumulativeFunction[?, ?]]) function
          .asInstanceOf[CumulativeFunction[V, V]].reset()
      }

      if (column.nonEmpty) {
        grouped.addOne(column)
        newcols.addOne(names(c))
      }
    }
//    Seq( "value", "version", "age").foreach(n => newcols.addOne(n))
    if (newcols.size <= columns.size) throw new IllegalArgumentException(
      s"no results for aggregate function ${function.getClass.getSimpleName}, newcols.size ${newcols.size} | columns.size ${columns.size}",
    )

    new DataFrame[V](
      index.toSeq,//.asInstanceOf[Seq[Any]],
      newcols.toSeq,//.asInstanceOf[Seq[Any]],
      grouped.map(_.toSeq).toList,
    )
  }

  def keys(): Set[AnyRef] = groups.keySet.toSet

  def getColumns(): Set[Int] = columns.toSet

  override def iterator: Iterator[(AnyRef, SparseBitSet)] = groups.iterator
}
//class Grouping extends Iterable[LinkedHashMap.Entry[AnyRef, SparseBitSet]] {
//  final private val groups = new mutable.LinkedHashMap[AnyRef, SparseBitSet]
//  final private val columns = new util.LinkedHashSet[Int]
//
//  def this
//
//  [V]
//  (df: DataFrame[V], function: DataFrame.KeyFunction[V], columns: Int *
//  )
//
//  def this
//
//  [V]
//  (df: DataFrame[V], columns: Int *
//  )
//  {
//    this (df, if (columns.length == 1) new DataFrame.KeyFunction[V]() {
//      override def apply(value:  Seq[V]): AnyRef = value.get(columns(0))
//    }
//    else new DataFrame.KeyFunction[V]() {
//      override def apply(value:  Seq[V]): AnyRef = {
//        val key = new  ListBuffer[AnyRef](columns.length)
//        for (column <- columns) {
//          key.add(value.get(column))
//        }
//        Collections.unmodifiableList(key)
//      }
//    }, columns)
//  }
//
//  @SuppressWarnings(Array("unchecked")) def apply[V](df: DataFrame[V], function: DataFrame.Function[_, _]): DataFrame[V] = {
//    if (df.isEmpty) return df
//    val grouped = new  ListBuffer[ Seq[V]]
//    val names = new  ListBuffer[AnyRef](df.columns)
//    val newcols = new  ListBuffer[AnyRef]
//    val index = new  ListBuffer[AnyRef]
//    // construct new row index
//    if (function.isInstanceOf[DataFrame.Aggregate[_, _]] && !groups.isEmpty) {
//
//      for (key <- groups.keySet) {
//        index.add(key)
//      }
//    }
//    // add key columns
//
//    for (c <- columns) {
//      if (function.isInstanceOf[DataFrame.Aggregate[_, _]] && !groups.isEmpty) {
//        val column = new  ListBuffer[V]
//
//        for (entry <- groups.entrySet) {
//          val rows = entry.getValue
//          val r = rows.nextSetBit(0)
//          column.add(df.get(r, c))
//        }
//        grouped.add(column)
//        newcols.add(names.get(c))
//      }
//      else {
//        grouped.add(df.col(c))
//        newcols.add(names.get(c))
//      }
//    }
//    // add aggregated data columns
//    for (c <- 0 until df.size) {
//      if (!columns.contains(c)) {
//        val column = new  ListBuffer[V]
//        if (groups.isEmpty) {
//          try if (function.isInstanceOf[DataFrame.Aggregate[_, _]]) column.add(classOf[DataFrame.Aggregate[_, _]].cast(function).apply(df.col(c)).asInstanceOf[V])
//          else for (r <- 0 until df.length) {
//            column.add(classOf[DataFrame.Function[_, _]].cast(function).apply(df.get(r, c)).asInstanceOf[V])
//          }
//          catch {
//            case ignored: ClassCastException =>
//          }
//          if (function.isInstanceOf[Transforms.CumulativeFunction[_, _]]) classOf[Transforms.CumulativeFunction[_, _]].cast(function).reset()
//        }
//        else {
//
//          for (entry <- groups.entrySet) {
//            val rows = entry.getValue
//            try if (function.isInstanceOf[DataFrame.Aggregate[_, _]]) {
//              val values = new  ListBuffer[V](rows.cardinality)
//              var r = rows.nextSetBit(0)
//              while (r >= 0) {
//                values.add(df.get(r, c))
//                r = rows.nextSetBit(r + 1)
//              }
//              column.add(classOf[DataFrame.Aggregate[_, _]].cast(function).apply(values).asInstanceOf[V])
//            }
//            else {
//              var r = rows.nextSetBit(0)
//              while (r >= 0) {
//                column.add(classOf[DataFrame.Function[_, _]].cast(function).apply(df.get(r, c)).asInstanceOf[V])
//                r = rows.nextSetBit(r + 1)
//              }
//            }
//            catch {
//              case ignored: ClassCastException =>
//            }
//            if (function.isInstanceOf[Transforms.CumulativeFunction[_, _]]) classOf[Transforms.CumulativeFunction[_, _]].cast(function).reset()
//          }
//        }
//        if (!column.isEmpty) {
//          grouped.add(column)
//          newcols.add(names.get(c))
//        }
//      }
//    }
//    if (newcols.size <= columns.size) throw new IllegalArgumentException("no results for aggregate function " + function.getClass.getSimpleName)
//    new DataFrame[V](index, newcols, grouped)
//  }
//
//  def keys: util.Set[AnyRef] = groups.keySet
//
//  def columns: util.Set[Int] = columns
//
//  override def iterator: util.Iterator[LinkedHashMap.Entry[AnyRef, SparseBitSet]] = groups.entrySet.iterator
//}
