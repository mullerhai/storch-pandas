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
import torch.DataFrame.{JoinType, KeyFunction, compare}

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object Combining {
  def join[V](left: DataFrame[V], right: DataFrame[V], how: DataFrame.JoinType, on: DataFrame.KeyFunction[V]): DataFrame[V] = {
    val leftIt = left.index.iterator
    val rightIt = right.index.iterator
    val leftMap = new mutable.LinkedHashMap[AnyRef,  Seq[V]]
    val rightMap = new mutable.LinkedHashMap[AnyRef,  Seq[V]]

    for (row <- left) {
      val name = leftIt.next
      val key = if (on == null) name
      else on.apply(row)
      if (leftMap.put(key, row) != null) throw new IllegalArgumentException("generated key is not unique: " + key)
    }

    for (row <- right) {
      val name = rightIt.next
      val key = if (on == null) name
      else on.apply(row)
      if (rightMap.put(key, row) != null) throw new IllegalArgumentException("generated key is not unique: " + key)
    }
    val columns = new  ListBuffer[AnyRef]()
    if (how ne JoinType.RIGHT) columns.addAll(left.columns)
    else  right.columns.map(col => columns.append(col))

    for (column <- if (how ne JoinType.RIGHT) right.columns else left.columns) {
      val index = columns.indexOf(column)
      if (index >= 0) if (column.isInstanceOf[ Seq[?]]) {
        @SuppressWarnings(Array("unchecked"))
        val l1 = classOf[ Seq[?]].cast(columns(index))
        l1.add(if (how ne JoinType.RIGHT) "left"
        else "right")
        @SuppressWarnings(Array("unchecked"))
        val l2 = classOf[ Seq[?]].cast(column)
        l2.add(if (how ne JoinType.RIGHT) "right"
        else "left")
      }
      else {
        columns.set(index, String.format("%s_%s", columns(index), if (how ne JoinType.RIGHT) "left"
        else "right"))
        column = String.format("%s_%s", column, if (how ne JoinType.RIGHT) "right"
        else "left")
      }
      columns.add(column)
    }
    val df = new DataFrame[V](columns)

    for (entry <- if (how ne JoinType.RIGHT) leftMap else rightMap) {
      val tmp = new  ListBuffer[V]()//entry.getValue)
      val row = if (how ne JoinType.RIGHT) rightMap.get(entry._1)
      else leftMap.get(entry._1)
      if (row != null || (how ne JoinType.INNER)) {
        tmp.addAll(if (row != null) row
        else Collections.nCopies[V](right.columns.size, null))
        df.append(entry.getKey, tmp)
      }
    }
    if (how eq JoinType.OUTER) {

      for (entry <- if (how ne JoinType.RIGHT) rightMap else leftMap) {
        val row = if (how ne JoinType.RIGHT) leftMap.get(entry._1) else rightMap.get(entry._1)
        if (row == null) {
          val tmp = new  ListBuffer[V]()
          Collections.nCopies[V](if (how ne JoinType.RIGHT) left.columns.size
          else right.columns.size, null))
          tmp.addAll(entry.getValue)
          df.append(entry.getKey, tmp)
        }
      }
    }
    df
  }

  def joinOn[V](left: DataFrame[V], right: DataFrame[V], how: DataFrame.JoinType, cols: Int*): DataFrame[V] = join(left, right, how, new DataFrame.KeyFunction[V]() {
    override def apply(value:  Seq[V]): AnyRef = {
      val key = new  ListBuffer[V]()//cols.length)
      for (col <- cols) {
        key.append(value(col))
      }
      Collections.unmodifiableList(key)
    }
  })

  def merge[V](left: DataFrame[V], right: DataFrame[V], how: DataFrame.JoinType): DataFrame[V] = {
    val intersection = new mutable.LinkedHashSet[AnyRef](left.nonnumeric.columns)
    intersection.retainAll(right.nonnumeric.columns)
    val columns = intersection.toArray(new Array[AnyRef](intersection.size))
    join(left.reindex(columns), right.reindex(columns), how, null)
  }

  @SafeVarargs def update[V](dest: DataFrame[V], overwrite: Boolean, others: DataFrame[_ <: V]*): Unit = {
    breakable {
      for (col <- 0 until dest.size) {
        for (row <- 0 until dest.length) {
          if (overwrite || dest.get(row, col) == null) for (other <- others) {
            if (col < other.size && row < other.length) {
              val value = other.get(row, col)
              if (value != null) {
                dest.set(row, col, value)
                break //todo: break is not supported
              }
            }
          }
        }
      }
    }
  }

  @SafeVarargs def concat[V](first: DataFrame[V], others: DataFrame[_ <: V]*): DataFrame[V] = {
    val dfs = new  ListBuffer[DataFrame[_ <: V]]()//others.length + 1)
    dfs.append(first)
    dfs.addAll(List(others))
    var rows = 0
    val columns = new mutable.LinkedHashSet[AnyRef]

    for (df <- dfs) {
      rows += df.length

      for (c <- df.columns) {
        columns.add(c)
      }
    }
    val newcols = new  ListBuffer[AnyRef]()//columns)
    val combined = new DataFrame[V](columns).reshape(rows, columns.size)
    var offset = 0

    for (df <- dfs) {
      val cols = new  ListBuffer[AnyRef]()//df.columns)
      for (c <- 0 until cols.size) {
        val newc = newcols.indexOf(cols(c))
        for (r <- 0 until df.length) {
          combined.set(offset + r, newc, df.get(r, c))
        }
      }
      offset += df.length
    }
    combined
  }
}