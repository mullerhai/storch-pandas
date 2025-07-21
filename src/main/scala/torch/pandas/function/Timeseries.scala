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
package torch.pandas.function

import java.util
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer

import torch.pandas.DataFrame
import torch.pandas.DataFrame.Function

object Timeseries {
  def rollapply[V](
      df: DataFrame[V],
      function: DataFrame.Function[Seq[V], V],
      period: Int,
  ): DataFrame[V] = {
    // can't use apply because rolling window functions are likely path dependent
    val data = new ListBuffer[Seq[V]]() // df.size)
    val f = new Timeseries.WindowFunction[V](function, period)
    for (c <- 0 until df.size) {
      val column = new ListBuffer[V]() // df.length)
      for (r <- 0 until df.length) {
        val windowFunc = f.apply(df.getFromIndex(r, c))
        column.append(windowFunc.asInstanceOf[V])
      }
      data.append(column.toSeq)
      f.reset()
    }
    new DataFrame[V](
      df.getIndex,
      df.getColumns.asInstanceOf[Seq[AnyRef]],
      data.toList,
    )
  }

  private class WindowFunction[V](
      val function: DataFrame.Function[Seq[V], V],
      val period: Int,
  ) extends DataFrame.Function[V, V] {
//    this.window = new ListBuffer[V]
    protected final var window: ListBuffer[V] = new ListBuffer[V]()

    override def apply(value: V): V = {
      while (window.size < period) window.append(null.asInstanceOf[V])
      window.append(value.asInstanceOf[V])
      val result = function.apply(window.toSeq)
//      window.remove
      window.clear()
      result
    }

    def reset(): Unit = window.clear()
  }

  private class DiscreteDifferenceFunction
      extends DataFrame.Function[Seq[Number], Number] {
    override def apply(values: Seq[Number]): Number = {
      if (values.contains(null)) return null
      values(values.size - 1).doubleValue - values(0).doubleValue
    }
  }

  private class PercentChangeFunction
      extends DataFrame.Function[Seq[Number], Number] {
    override def apply(values: Seq[Number]): Number = {
      if (values.contains(null)) return null
      val x1 = values(0).doubleValue
      val x2 = values(values.size - 1).doubleValue
      (x2 - x1) / x1
    }
  }

  def diff[V](df: DataFrame[V], period: Int): DataFrame[V] = {
    val nonnumeric = df.nonnumeric
    @SuppressWarnings(Array("unchecked"))
    val diff = df.numeric.apply(new Timeseries.WindowFunction[Number](
      new Timeseries.DiscreteDifferenceFunction,
      period,
    )).asInstanceOf[DataFrame[V]]
    if (nonnumeric.isEmpty) diff else nonnumeric.join(diff)
  }

  def percentChange[V](df: DataFrame[V], period: Int): DataFrame[V] = {
    val nonnumeric = df.nonnumeric
    @SuppressWarnings(Array("unchecked"))
    val diff = df.numeric.apply(new Timeseries.WindowFunction[Number](
      new Timeseries.PercentChangeFunction,
      period,
    )).asInstanceOf[DataFrame[V]]
    if (nonnumeric.isEmpty) diff else nonnumeric.join(diff)
  }
}
