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

import java.util.Comparator
import torch.DataFrame
import torch.DataFrame.SortDirection

import scala.collection.mutable.{LinkedHashMap, ListBuffer, *}
import scala.util.control.Breaks.{breakable,break}
object Sorting {
  def sort[V](df: DataFrame[V], cols: LinkedHashMap[Int, DataFrame.SortDirection]): DataFrame[V] = {
    val comparator = new Comparator[ Seq[V]]() {
      @SuppressWarnings(Array("unchecked")) override def compare(r1:  Seq[V], r2:  Seq[V]): Int = {
        var result = 0

        breakable{
          for (col <- cols) {
            val c = col._1
            val v1 = classOf[Comparable[?]].cast(r1(c))
            val v2 = r2(c)
            result = v1.compareTo(v2)
            result *=
            if (col._2 eq SortDirection.DESCENDING) -1
            else 1
            if (result != 0) break //todo: break is not supported
          }
        }

        result
      }
    }
    sort(df, comparator)
  }

  def sort[V](df: DataFrame[V], comparator: Comparator[ Seq[V]]): DataFrame[V] = {
    val sorted = new DataFrame[V](df.columns)
    val cmp = new Comparator[Int]() {
      override def compare(r1: Int, r2: Int): Int = comparator.compare(df.row(r1), df.row(r2))
    }
    val rows = new Array[Int](df.length)
    for (r <- 0 until df.length) {
      rows(r) = r
    }
    java.util.Arrays.sort(rows, cmp)
    val labels = new  ListBuffer[AnyRef](df.index)
    for (r <- rows) {
      val label = if (r < labels.size) labels.get(r)
      else r
      sorted.append(label, df.row(r))
    }
    sorted
  }
}