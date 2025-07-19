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
    var maxRowCount = 0
    // 先计算最大行数
    for (col <- data) {
      maxRowCount = math.max(maxRowCount, col.size)
    }
    // 逐列处理数据
    for (col <- data) {
      val newCol = mutable.Buffer.empty[V]
      var rowIndex = 0
      for (value <- col) {
        newCol += value
        rowIndex += 1
      }
      // 补齐列到最大行数
      while (rowIndex < maxRowCount) {
        newCol += null.asInstanceOf[V]
        rowIndex += 1
      }
      add(newCol)
    }
//    for (col <- data) add(mutable.Buffer.from(col))
  }

  def reshape(cols: Int, rows: Int): Unit = {
    // 当当前列数少于目标列数时，添加新列
    while (blocks.size < cols) {
      add(mutable.Buffer.fill(rows)(null.asInstanceOf[V]))
    }
    // 遍历每一列，将列的长度补齐到目标行数
    blocks.foreach { block =>
      while (block.size < rows) {
        block.addOne(null.asInstanceOf[V])
      }
    }
  }


  def get(col: Int, row: Int): V = blocks(col)(row)

  def set(value: V, col: Int, row: Int): Unit = {
    blocks(col)(row) = value
  }

  def add(col: scala.collection.mutable.Buffer[V]): Unit = {
    // 获取当前 blocks 中第一列的长度（即行数）
    val len = length()
    // 若传入列的长度小于现有行数，用 null 补齐
    while (col.size < len) {
      col.addOne(null.asInstanceOf[V])
    }
    // 将补齐后的列添加到 blocks 列表中
    blocks.addOne(col)
  }

  //big bug cost most time , more data  more time !!!!
//  def add(col: mutable.Buffer[V]): Unit = {
//    val len = length()
////    while (col.size < len) col.addOne(null.asInstanceOf[V])
////    blocks.addOne(col)
//
//    if (col == null) return
//    val missingCount = len - col.size
//    if (missingCount > 0) {
//      // 一次性添加所需的 null 值
//      col.addAll(mutable.Buffer.fill(missingCount)(null.asInstanceOf[V]))
//    }
//    blocks.addOne(col)
//  }

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
