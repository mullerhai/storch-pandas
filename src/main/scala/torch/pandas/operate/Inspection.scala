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

import torch.DataFrame

object Inspection {
  def types(df: DataFrame[?]): Seq[Class[?]] = {
    val types = new ListBuffer[Class[?]]()
    var c = 0
    while (c < df.size && 0 < df.length) {
      val value = df.getFromIndex(0, c)
      types.append(if (value != null) value.getClass else classOf[AnyRef])
      c += 1
    }
    types.toSeq
  }

  def numeric(df: DataFrame[?]): SparseBitSet = {
    val numeric = new SparseBitSet
    val typeSeq = types(df)
    for (c <- 0 until typeSeq.size)
      if (classOf[Number].isAssignableFrom(typeSeq(c))) numeric.set(c)
    numeric
  }

  def nonnumeric(df: DataFrame[?]): SparseBitSet = {
    val nonnumeric = numeric(df)
    nonnumeric.flip(0, df.size)
    nonnumeric
  }
}
