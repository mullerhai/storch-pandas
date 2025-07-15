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

import torch.pandas.DataFrame
import torch.pandas.DataFrame.SortDirection

import java.util.Comparator
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.util.control.Breaks.{break, breakable}
object Sorting {
  def sort[V](
      df: DataFrame[V],
      cols: LinkedHashMap[Int, DataFrame.SortDirection],
  ): DataFrame[V] = {
    val comparator = new Comparator[Seq[V]]() {
      @SuppressWarnings(Array("unchecked"))
      override def compare(r1: Seq[V], r2: Seq[V]): Int = {
        var result = 0

        breakable(for (col <- cols) {
          val c = col._1
          val v1 = classOf[Comparable[V]].cast(r1(c))
          val v2 = r2(c)
          result = v1.compareTo(v2)
          val resultTmp = if (col._2 eq SortDirection.DESCENDING) -1 else 1
          result *= resultTmp
          if (result != 0) break // todo: break is not supported
        })

        result
      }
    }
    sort(df, comparator)
  }

  def sort[V](
      df: DataFrame[V],
      comparator: Comparator[Seq[V]],
  ): DataFrame[V] = {
    val sorted = new DataFrame[V](df.getColumns.map(_.toString)*)
    val cmp = new Comparator[AnyRef]() {
      override def compare(r1: AnyRef, r2: AnyRef): Int = comparator
        .compare(df.row(r1), df.row(r2))
    }
    val rows = new Array[AnyRef](df.length)
    for (r <- 0 until df.length) rows(r) = r.asInstanceOf[AnyRef]

    java.util.Arrays.sort(rows, cmp)
    val labels = new ListBuffer[AnyRef]() // df.getIndex)
    for (r <- rows) {
      val label =
        if (r.asInstanceOf[Int] < labels.size) labels(r.asInstanceOf[Int])
        else r
      sorted.append(label, df.row(r))
    }
    sorted
  }
}
