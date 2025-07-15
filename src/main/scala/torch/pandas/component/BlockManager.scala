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

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}

class BlockManager[V] private (
    private val blocks: mutable.Buffer[mutable.Buffer[V]],
) {
  def this() = this(mutable.Buffer.empty[mutable.Buffer[V]])

  def this(data: Iterable[Iterable[V]]) = {
    this()
    for (col <- data) add(mutable.Buffer.from(col))
  }

  def reshape(cols: Int, rows: Int): Unit = {
    while (blocks.size < cols)
      add(mutable.ListBuffer.fill(rows)(null.asInstanceOf[V]))

    for (block <- blocks)
      while (block.size < rows) block.addOne(null.asInstanceOf[V])
  }

  def get(col: Int, row: Int): V = blocks(col)(row)

  def set(value: V, col: Int, row: Int): Unit = {
    blocks(col)(row) = value
  }

  def add(col: mutable.Buffer[V]): Unit = {
    val len = length()
    while (col.size < len) col.addOne(null.asInstanceOf[V])
    blocks.addOne(col)
  }

  def size(): Int = blocks.size

  def length(): Int = if (blocks.isEmpty) 0 else blocks.head.size
}
//class BlockManager[V](data: Seq[? <: Seq[? <: V]]) {
//  final private var blocks: ListBuffer[Seq[V]] = new ListBuffer[Seq[V]]()
//
//
//
//  for (col <- data) {
//    add(new  ListBuffer[V](col))
//  }
//
//
//  def this ={
//    this(Seq[ Seq[V]])
//  }
//
//  def reshape(cols: Int, rows: Int): Unit = {
//    for (c <- blocks.size until cols) {
//      add(new  ListBuffer[V](rows))
//    }
//
//    for (block <- blocks) {
//      for (r <- block.size until rows) {
//        block.append(null)
//      }
//    }
//  }
//
//  def get(col: Int, row: Int): V = blocks(col)(row)
//
//  def set(value: V, col: Int, row: Int): Unit = {
//    blocks(col).set(row, value)
//  }
//
//  def add(col:  ListBuffer[V]): Unit = {
//    val len = length
//    for (r <- col.size until len) {
//      col.append(null)
//    }
//    blocks.append(col)
//  }
//
//  def size: Int = blocks.size
//
//  def length: Int = if (blocks.isEmpty) 0
//  else blocks(0).size
//}
