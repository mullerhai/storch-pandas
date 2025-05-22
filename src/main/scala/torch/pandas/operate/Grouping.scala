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

import torch.DataFrame
import torch.DataFrame.Aggregate
import torch.DataFrame.Function
import torch.DataFrame.KeyFunction
//import torch.pandas.operate.Transforms.CumulativeFunction
import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}

class Grouping[V] private(
                           private val groups: mutable.LinkedHashMap[Any, SparseBitSet] = mutable.LinkedHashMap.empty,
                           private val columns: mutable.LinkedHashSet[Int] = mutable.LinkedHashSet.empty
                         ) extends Iterable[(Any, SparseBitSet)] {

  def this() =()

  def this(df: DataFrame[V], function: KeyFunction[V], cols: Int*) = {
    this()
    val iter = df.iterator().asScala
    var r = 0
    while (iter.hasNext) {
      val row = iter.next()
      val key = function.apply(row.asScala.toList)
      val group = groups.getOrElseUpdate(key, new SparseBitSet())
      group.set(r)
      r += 1
    }
    cols.foreach(columns.add)
  }

  def this(df: DataFrame[V], cols: Int*) = {
    this(
      df,
      if (cols.length == 1) {
        new KeyFunction[V] {
          override def apply(value: java.util.List[V]): Any = value.get(cols(0))
        }
      } else {
        new KeyFunction[V] {
          override def apply(value: java.util.List[V]): Any = {
            val keyList = new mutable.ListBuffer[Any]()
            cols.foreach(c => keyList.addOne(value.get(c)))
            keyList.toList
          }
        }
      },
      cols: _*
    )
  }

  def apply(df: DataFrame[V], function: Function[?, ?]): DataFrame[V] = {
    if (df.isEmpty) return df

    val grouped = mutable.ListBuffer[mutable.ListBuffer[V]]()
    val names = df.columns.toList
    val newcols = mutable.ListBuffer[Any]()
    val index = mutable.ListBuffer[Any]()

    // construct new row index
    if (function.isInstanceOf[Aggregate[?, ?]] && groups.nonEmpty) {
      index.addAll(groups.keys)
    }

    // add key columns
    columns.foreach { c =>
      if (function.isInstanceOf[Aggregate[?, ?]] && groups.nonEmpty) {
        val column = mutable.ListBuffer[V]()
        groups.foreach { case (_, rows) =>
          val r = rows.nextSetBit(0)
          column.addOne(df.get(r, c))
        }
        grouped.addOne(column)
        newcols.addOne(names(c))
      } else {
        grouped.addOne(df.col(c).asScala.to(mutable.ListBuffer))
        newcols.addOne(names(c))
      }
    }

    // add aggregated data columns
    for (c <- 0 until df.size) {
      if (!columns.contains(c)) {
        val column = mutable.ListBuffer[V]()
        if (groups.isEmpty) {
          try {
            if (function.isInstanceOf[Aggregate[?, ?]]) {
              column.addOne(function.asInstanceOf[Aggregate[V, V]].apply(df.col(c).asScala.toList).asInstanceOf[V])
            } else {
              for (r <- 0 until df.length) {
                column.addOne(function.asInstanceOf[Function[V, V]].apply(df.get(r, c)).asInstanceOf[V])
              }
            }
          } catch {
            case _: ClassCastException => ()
          }

          if (function.isInstanceOf[CumulativeFunction[?, ?]]) {
            function.asInstanceOf[CumulativeFunction[V, V]].reset()
          }
        } else {
          groups.foreach { case (_, rows) =>
            try {
              if (function.isInstanceOf[Aggregate[?, ?]]) {
                val values = mutable.ListBuffer[V]()
                var r = rows.nextSetBit(0)
                while (r >= 0) {
                  values.addOne(df.get(r, c))
                  r = rows.nextSetBit(r + 1)
                }
                column.addOne(function.asInstanceOf[Aggregate[V, V]].apply(values.toList).asInstanceOf[V])
              } else {
                var r = rows.nextSetBit(0)
                while (r >= 0) {
                  column.addOne(function.asInstanceOf[Function[V, V]].apply(df.get(r, c)).asInstanceOf[V])
                  r = rows.nextSetBit(r + 1)
                }
              }
            } catch {
              case _: ClassCastException => ()
            }

            if (function.isInstanceOf[CumulativeFunction[?, ?]]) {
              function.asInstanceOf[CumulativeFunction[V, V]].reset()
            }
          }
        }

        if (column.nonEmpty) {
          grouped.addOne(column)
          newcols.addOne(names(c))
        }
      }
    }

    if (newcols.size <= columns.size) {
      throw new IllegalArgumentException(
        s"no results for aggregate function ${function.getClass.getSimpleName}"
      )
    }

    new DataFrame[V](index, newcols, grouped)
  }

  def keys(): Set[Any] = groups.keySet.toSet

  def getColumns(): Set[Int] = columns.toSet

  override def iterator: Iterator[(Any, SparseBitSet)] = groups.iterator
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