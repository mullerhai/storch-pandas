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

import scala.collection.mutable
import scala.collection.AbstractSeq
import scala.collection.AbstractMap
import scala.collection.AbstractSet
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer
import torch.DataFrame
import torch.DataFrame.Function

object Views {

  class ListView[V](val df: DataFrame[V], val transpose: Boolean) extends AbstractSeq[List[V]] {
    override def length: Int = if (transpose) df.length else df.size

    override def get(index: Int) = new Views.SeriesListView[V](df, index, !transpose)

    //: List[V]
    override def apply(index: Int) = new SeriesListView(df, index, !transpose)
  }

  class SeriesListView[V](val df: DataFrame[V], val index: Int, val transpose: Boolean) extends AbstractSeq[V] {
    override def length: Int = if (transpose) df.length else df.size
    override def get(index: Int): V = if (transpose) df.get(index, this.index)

    override def apply(index: Int): V = if (transpose) df.get(index, this.index) else df.get(this.index, index)
  }

  class MapView[V](val df: DataFrame[V], val transpose: Boolean) extends AbstractSeq[Map[Any, V]] {
    override def length: Int = if (transpose) df.length else df.size
    override def get(index: Int) = new Views.SeriesMapView[V](df, index, !transpose)

    //: Map[Any, V]
    override def apply(index: Int) = new Views.SeriesMapView(df, index, !transpose)
  }

  class SeriesMapView[V](val df: DataFrame[V], val index: Int, val transpose: Boolean) extends AbstractMap[Any, V] {
    override def iterator: Iterator[(Any, V)] = {
      val names = if (transpose) df.index else df.columns
      val it = names.iterator
      new Iterator[(Any, V)] {
        private var valueIdx = 0

        override def hasNext: Boolean = it.hasNext

        override def next(): (Any, V) = {
          val key = it.next()
          val value = if (transpose) df.get(valueIdx, index) else df.get(index, valueIdx)
          valueIdx += 1
          (key, value)
        }
      }
    }

    override def get(key: Any): Option[V] = {
      val names = if (transpose) df.index else df.columns
      if (names.contains(key)) {
        val idx = names.toList.indexOf(key)
        Some(if (transpose) df.get(idx, index) else df.get(index, idx))
      } else {
        None
      }
    }
  }

  class TransformedView[V, U](val df: DataFrame[V], val transform: Function[V, U], val transpose: Boolean) extends AbstractSeq[List[U]] {
    override def length: Int = if (transpose) df.length else df.size
    override def get(index: Int) = new Views.TransformedSeriesView[V, U](df, transform, index, !transpose)

    override def apply(index: Int): List[U] = new TransformedSeriesView(df, transform, index, !transpose)
  }

  class TransformedSeriesView[V, U](val df: DataFrame[V], val transform: Function[V, U], val index: Int, val transpose: Boolean) extends AbstractSeq[U] {
    override def length: Int = if (transpose) df.length else df.size

    override def get(index: Int): U = {
      val value = if (transpose) df.get(index, this.index)
      else df.get(this.index, index)
      transform.apply(value)
    }
    override def apply(index: Int): U = {
      val value = if (transpose) df.get(index, this.index)
      else df.get(this.index, index)
      transform.apply(value)
    }
  }

  class FlatView[V](val df: DataFrame[V]) extends AbstractSeq[V] {
    override def length: Int = df.size * df.length
    override def size: Int = df.size * df.length

    override def get(index: Int): V = df.get(index % df.length, index / df.length)

    override def apply(index: Int): V = df.get(index % df.length, index / df.length)
  }

  class FillNaFunction[V](val fill: V) extends Function[V, V] {
    override def apply(value: V): V = if (value == null) fill else value
  }
}