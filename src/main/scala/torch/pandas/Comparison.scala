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
package torch.pandas

import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.LinkedHashSet
import scala.collection.mutable.ListBuffer

import torch.pandas.DataFrame
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object Comparison {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def compare[V](df1: DataFrame[V], df2: DataFrame[V]): DataFrame[String] = {
    // algorithm
    // 1. determine union of rows and columns
    val rows = new LinkedHashSet[AnyRef]()
    rows.addAll(df1.getIndex)
    rows.addAll(df2.getIndex)
    val cols = new LinkedHashSet[AnyRef]()
    cols.addAll(df1.getColumns)
    cols.addAll(df2.getColumns)
    // 2. reshape left to contain all rows and columns
    val rowSeq = rows.toSeq
    val colsSeq = cols.toSeq
    val left = df1.reshape(rowSeq, colsSeq)
    // 3. reshape right to contain all rows and columns
    val right = df2.reshape(rowSeq, colsSeq)
    val comp = new DataFrame[String](rowSeq, colsSeq.asInstanceOf[Seq[AnyRef]])
    // 4. perform comparison cell by cell
    for (c <- 0 until left.size) for (r <- 0 until left.length) {
      val lval = left.getFromIndex(r, c)
      val rval = right.getFromIndex(r, c)
      if (lval == null && rval == null)
        // equal but null
        comp.set(r, c, "")
      else if (lval != null && lval == rval)
        // equal
        comp.set(r, c, String.valueOf(lval))
      else if (lval == null)
        // missing from left
        comp.set(r, c, String.valueOf(rval)) // + " (added from right)");
      else if (rval == null)
        // missing from right
        comp.set(r, c, String.valueOf(lval)) // + " (added from left)");
      else
        // not equal
        comp.set(r, c, String.format("%s | %s", lval, rval))
    }
    comp
  }
}
