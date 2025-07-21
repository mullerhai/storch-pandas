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

import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import torch.pandas.DataFrame
object Shaping {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def reshape[V](df: DataFrame[V], rows: Int, cols: Int): DataFrame[V] = {
    val reshaped = new DataFrame[V]
    var colIt: Iterator[AnyRef] = df.getColumns.iterator
    for (c <- 0 until cols) {

      val name = colIt.nextOption().getOrElse(c.asInstanceOf[AnyRef]) //  if (it.hasNext) it.next else c
      reshaped.add(name)
    }

    val rowIt = df.getIndex.iterator
    for (r <- 0 until rows) {

      val name = rowIt.nextOption().getOrElse(r.asInstanceOf[AnyRef]) // if (it.hasNext) it.next else r.asInstanceOf[String]
      reshaped.append(name, Seq.empty)
    }
    for (c <- 0 until cols) for (r <- 0 until rows)

      if (c < df.size && r < df.length) reshaped.set(r, c, df.getFromIndex(r, c))
    reshaped
  }

  def reshape[V](
      df: DataFrame[V],
      rows: Seq[AnyRef],
      cols: Seq[AnyRef],
  ): DataFrame[V] = {
    val reshaped = new DataFrame[V]

    for (name <- cols) reshaped.add(name)

    for (name <- rows) reshaped.append(name, Seq.empty)

    for (c <- cols)

      for (r <- rows) if (df.getColumns.contains(c) && df.getIndex.contains(r))
        reshaped
          .set(r, c, df.getFromIndex(r.asInstanceOf[Int], c.asInstanceOf[Int]))
    reshaped
  }
}
