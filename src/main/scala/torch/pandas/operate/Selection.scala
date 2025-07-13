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

import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer

import torch.DataFrame
import torch.DataFrame.Predicate

object Selection {
  def select[V](
      df: DataFrame[V],
      predicate: DataFrame.Predicate[V],
  ): SparseBitSet = {
    val selected = new SparseBitSet
    val rows = df.iterator
    var r = 0
    while (rows.hasNext) {
      if (predicate.apply(rows.next)) selected.set(r)
      r += 1
    }
    selected
  }

  def select(index: Index, selected: SparseBitSet): Index = {
    val names = new ListBuffer[Any]() // index.names)
    index.names.foreach(ee => names.append(ee))
    val newidx = new Index
    var r = selected.nextSetBit(0)
    while (r >= 0) {
      val name = names(r)
      newidx.add(name, index.get(name))
      r = selected.nextSetBit(r + 1)
    }
    newidx
  }

  def select[V](
      blocks: BlockManager[V],
      selected: SparseBitSet,
  ): BlockManager[V] = {
    val data = new ListBuffer[Seq[V]]
    for (c <- 0 until blocks.size()) {
      val column = new ListBuffer[V]() // selected.cardinality)
//      selected.getCardinality
      var r = selected.nextSetBit(0)
      while (r >= 0) {
        column.append(blocks.get(c, r))
        r = selected.nextSetBit(r + 1)
      }
      data.append(column.toSeq)
    }
    new BlockManager[V](data.toSeq)
  }

  def select[V](
      blocks: BlockManager[V],
      rows: SparseBitSet,
      cols: SparseBitSet,
  ): BlockManager[V] = {
    val data = new ListBuffer[Seq[V]]
    var c = cols.nextSetBit(0)
    while (c >= 0) {
      val column = new ListBuffer[V]() // rows.cardinality)
      var r = rows.nextSetBit(0)
      while (r >= 0) {
        column.append(blocks.get(c, r))
        r = rows.nextSetBit(r + 1)
      }
      data.append(column.toSeq)
      c = cols.nextSetBit(c + 1)
    }
    new BlockManager[V](data.toSeq)
  }

  def slice[V](
      df: DataFrame[V],
      rowStart: Int,
      rowEnd: Int,
      colStart: Int,
      colEnd: Int,
  ): Array[SparseBitSet] = {
    val rows = new SparseBitSet
    val cols = new SparseBitSet
    rows.set(rowStart, rowEnd)
    cols.set(colStart, colEnd)
    Array[SparseBitSet](rows, cols)
  }

  class DropNaPredicate[V] extends DataFrame.Predicate[V] {
    override def apply(values: Seq[V]): Boolean = {

      for (value <- values) if (value == null) return false
      true
    }
  }
}
