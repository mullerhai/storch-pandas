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
import scala.collection.mutable.{LinkedHashMap,LinkedHashSet,ListBuffer}

object Comparison {
  def compare[V](df1: DataFrame[V], df2: DataFrame[V]): DataFrame[String] = {
    // algorithm
    // 1. determine union of rows and columns
    val rows = new LinkedHashSet[AnyRef]
    rows.addAll(df1.index)
    rows.addAll(df2.index)
    val cols = new LinkedHashSet[AnyRef]
    cols.addAll(df1.columns)
    cols.addAll(df2.columns)
    // 2. reshape left to contain all rows and columns
    val left = df1.reshape(rows, cols)
    // 3. reshape right to contain all rows and columns
    val right = df2.reshape(rows, cols)
    val comp = new DataFrame[String](rows, cols)
    // 4. perform comparison cell by cell
    for (c <- 0 until left.size) {
      for (r <- 0 until left.length) {
        val lval = left.get(r, c)
        val rval = right.get(r, c)
        if (lval == null && rval == null) {
          // equal but null
          comp.set(r, c, "")
        }
        else if (lval != null && lval == rval) {
          // equal
          comp.set(r, c, String.valueOf(lval))
        }
        else if (lval == null) {
          // missing from left
          comp.set(r, c, String.valueOf(rval)) // + " (added from right)");
        }
        else if (rval == null) {
          // missing from right
          comp.set(r, c, String.valueOf(lval)) // + " (added from left)");
        }
        else {
          // not equal
          comp.set(r, c, String.format("%s | %s", lval, rval))
        }
      }
    }
    comp
  }
}