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
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
object Shaping {
  def reshape[V](df: DataFrame[V], rows: Int, cols: Int): DataFrame[V] = {
    val reshaped = new DataFrame[V]
    var it: Iterator[AnyRef] = null
    it = df.columns.iterator
    for (c <- 0 until cols) {
      val name = if (it.hasNext) it.next
      else c
      reshaped.add(name)
    }
    it = df.index.iterator
    for (r <- 0 until rows) {
      val name = if (it.hasNext) it.next
      else r
      reshaped.append(name, Seq[V])
    }
    for (c <- 0 until cols) {
      for (r <- 0 until rows) {
        if (c < df.size && r < df.length) reshaped.set(r, c, df.get(r, c))
      }
    }
    reshaped
  }

  def reshape[V](df: DataFrame[V], rows: Seq[?], cols: Seq[?]): DataFrame[V] = {
    val reshaped = new DataFrame[V]

    for (name <- cols) {
      reshaped.add(name)
    }

    for (name <- rows) {
      reshaped.append(name, Seq[V])
    }

    for (c <- cols) {

      for (r <- rows) {
        if (df.columns.contains(c) && df.index.contains(r)) reshaped.set(r, c, df.get(r, c))
      }
    }
    reshaped
  }
}