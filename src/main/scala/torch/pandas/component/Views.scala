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

import java.util
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.*
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

import torch.pandas.DataFrame
import torch.pandas.DataFrame.Function

//    override def get(index: Int) =
//      new Views.SeriesListView[V](df, index, !transpose)
//    override def size: Int = if (transpose) df.length else df.size

object Views {
  private val logger = LoggerFactory.getLogger(this.getClass)
  class ListView[V](val df: DataFrame[V], val transpose: Boolean)
      extends AbstractSeq[List[V]] {

    override def apply(index: Int): List[V] =
      new Views.SeriesListView[V](df, index, !transpose).toList

    override def length: Int = if (transpose) df.length else df.size

    val viewLength = if (transpose) df.length else df.size

    override def iterator: Iterator[List[V]] = new Iterator[List[V]] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < viewLength

      override def next(): List[V] = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
  }

  // util.AbstractList[V]
  //    override def get(index: Int): V =
  //      if (transpose) {
  ////        println("Class Views : Transpose get: " + index + ", " + this.index)
  //        df.getFromIndex(index, this.index)
  //      } else {
  ////        println("Class Views : Normal get: index num " + index + ",  index map " + this.index)
  //        df.getFromIndex(this.index, index)
  //      }
  //
  //    override def size: Int = if (transpose) df.length else df.size

  class SeriesListView[V](
      val df: DataFrame[V],
      val index: Int,
      val transpose: Boolean,
  ) extends AbstractSeq[V] {

    override def apply(index: Int): V =
      if (transpose)
        //        println("Class Views : Transpose get: " + index + ", " + this.index)
        df.getFromIndex(index, this.index)
      else
        //        println("Class Views : Normal get: index num " + index + ",  index map " + this.index)
        df.getFromIndex(this.index, index)

    override def length: Int = if (transpose) df.length else df.size

    val viewLength = if (transpose) df.length else df.size
    override def iterator: Iterator[V] = new Iterator[V] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < viewLength

      override def next(): V = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
  }

  class MapView[V](val df: DataFrame[V], val transpose: Boolean)
      extends AbstractSeq[Map[AnyRef, V]] {
//    override def get(index: Int) =
//      new Views.SeriesMapView[V](df, index, !transpose)
//
//    override def size: Int = if (transpose) df.length else df.size

    override def apply(index: Int): Map[AnyRef, V] =
      new Views.SeriesMapView[V](df, index, !transpose)

    override def length: Int = if (transpose) df.length else df.size

    override def iterator: Iterator[Map[AnyRef, V]] = new Iterator[Map[AnyRef, V]] {
      private var currentIndex = 0
      private val maxLength = MapView.this.length

      override def hasNext: Boolean = currentIndex < maxLength

      override def next(): Map[AnyRef, V] = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
//    override def iterator: Iterator[Map[AnyRef, V]] = ???
  }

  class SeriesMapView[V](
      val df: DataFrame[V],
      val index: Int,
      val transpose: Boolean,
  ) extends AbstractMap[AnyRef, V] {
    override def get(key: AnyRef): Option[V] = {
      val names = if (transpose) df.getIndex else df.getColumns
      names.indexOf(key) match {
        case -1 => None
        case idx => Some(
            if (transpose) df.getFromIndex(idx, index)
            else df.getFromIndex(index, idx),
          )
      }
    }

    override def iterator: Iterator[(AnyRef, V)] = {
      val names = if (transpose) df.getIndex else df.getColumns
      val it = names.iterator
      new Iterator[(AnyRef, V)] {
        private var valueIdx = 0

        override def hasNext: Boolean = it.hasNext

        override def next(): (AnyRef, V) = {
          val key = it.next().asInstanceOf[AnyRef]
          val value =
            if (transpose) df.getFromIndex(valueIdx, index)
            else df.getFromIndex(index, valueIdx)
          valueIdx += 1
          (key, value)
        }
      }
    }

    // 移除单个键，返回一个新的 Map
    override def -(key: AnyRef): Map[AnyRef, V] = {
      val entries = this.toList.filter(_._1 != key)
      Map(entries: _*)
    }

    // 移除多个键，返回一个新的 Map
    override def -(
        key1: AnyRef,
        key2: AnyRef,
        keys: AnyRef*,
    ): Map[AnyRef, V] = {
      val allKeys = Set(key1, key2) ++ keys
      val entries = this.toList.filter { case (k, _) => !allKeys.contains(k) }
      Map(entries: _*)
    }
//    override def get(key: AnyRef): Option[V] = ???

//    override def iterator: Iterator[(AnyRef, V)] = ???

//    override def -(key: AnyRef): Map[AnyRef, V] = ???

//    override def -(key1: AnyRef, key2: AnyRef, keys: AnyRef*): Map[AnyRef, V] = ???

//    override def entrySet: util.Set[util.Map.Entry[AnyRef, V]] =
//      new util.AbstractSet[util.Map.Entry[AnyRef, V]]() {
//        override def iterator: util.Iterator[util.Map.Entry[AnyRef, V]] = {
//          val names = if (transpose) df.getIndex else df.getColumns
//          val it = names.iterator
//          new util.Iterator[util.Map.Entry[AnyRef, V]]() {
//            var value = 0
//
//            override def hasNext: Boolean = return it.hasNext
//
//            override def next: util.Map.Entry[AnyRef, V] = {
//              val key = it.next
//              val value = { this.value += 1; this.value - 1 }
//              new util.Map.Entry[AnyRef, V]() {
//                override def getKey: AnyRef = return key.asInstanceOf[AnyRef]
//
//                override def getValue: V =
//                  return if (transpose) df.getFromIndex(value, index)
//                  else df.getFromIndex(index, value)
//
//                override def setValue(value: V): V =
//                  throw new UnsupportedOperationException
//              }
//            }
//
//            override def remove(): Unit =
//              throw new UnsupportedOperationException
//          }
//        }
//
//        override def size: Int = return if (transpose) df.length else df.size
//      }

  }

  class TransformedView[V, U](
      val df: DataFrame[V],
      val transform: DataFrame.Function[V, U],
      protected val transpose: Boolean,
  ) extends AbstractSeq[List[U]] {
//    override def get(index: Int) =
//      new Views.TransformedSeriesView[V, U](df, transform, index, !transpose)
//
//    override def size: Int = if (transpose) df.length else df.size

    override def apply(index: Int): List[U] =
      new Views.TransformedSeriesView[V, U](df, transform, index, !transpose)
        .toList

    override def length: Int = if (transpose) df.length else df.size

    val viewLength = if (transpose) df.length else df.size

    override def iterator: Iterator[List[U]] = new Iterator[List[U]] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < viewLength

      override def next(): List[U] = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
  }

  class TransformedSeriesView[V, U](
      val df: DataFrame[V],
      val transform: DataFrame.Function[V, U],
      protected val index: Int,
      protected val transpose: Boolean,
  ) extends AbstractSeq[U] {
//    override def get(index: Int): U = {
//      val value =
//        if (transpose) df.getFromIndex(index, this.index) else df.getFromIndex(this.index, index)
//      transform.apply(value.asInstanceOf[V])
//    }

//    override def size: Int = if (transpose) df.length else df.size

    override def apply(index: Int): U = {
      val value =
        if (transpose) df.getFromIndex(index, this.index)
        else df.getFromIndex(this.index, index)
      transform.apply(value.asInstanceOf[V])
    }

    override def length: Int = if (transpose) df.length else df.size

    val viewLength = if (transpose) df.length else df.size

    override def iterator: Iterator[U] = new Iterator[U] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < viewLength

      override def next(): U = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
  }

  class FlatView[V](val df: DataFrame[V]) extends AbstractSeq[V] {

//    override def get(index: Int): V = df
//      .getFromIndex(index % df.length, index / df.length)
//
//    override def size: Int = df.size * df.length

    override def apply(index: Int): V = df
      .getFromIndex(index % df.length, index / df.length)

    override def length: Int = df.size * df.length

    val viewLength = df.size * df.length

    override def iterator: Iterator[V] = new Iterator[V] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < viewLength

      override def next(): V = {
        val result = apply(currentIndex)
        currentIndex += 1
        result
      }
    }
  }

  class FillNaFunction[V](val fill: V) extends DataFrame.Function[V, V] {
    override def apply(value: V): V =
      if (value == null) fill else value.asInstanceOf[V]
  }
}
//object Views {
//
//  class ListView[V](val df: DataFrame[V], val transpose: Boolean) extends AbstractSeq[List[V]] {
//    override def length: Int = if (transpose) df.length else df.size
//
//    def get(index: Int) = new Views.SeriesListView[V](df, index, !transpose)
//
//    override def apply(index: Int) = new SeriesListView(df, index, !transpose).toList
//
//    override def iterator: Iterator[List[V]] = {
//      println("hhe")
//      df.iterator
////      df.toList.map(_.toList).iterator
//    }
//  }
//
//  class SeriesListView[V](val df: DataFrame[V], val index: Int, val transpose: Boolean) extends AbstractSeq[V] {
//    override def length: Int = if (transpose) df.length else df.size
//    def get(index: Int): V = df.get(index, this.index)
//
//
//    override def apply(index: Int): V = if (transpose) df.get(index, this.index) else df.get(this.index, index)
//
//    override def iterator: Iterator[V] = {
//      df.itervalues
//    }
//  }
//
//  class MapView[V](val df: DataFrame[V], val transpose: Boolean) extends AbstractSeq[Map[Any, V]] {
//    override def length: Int = if (transpose) df.length else df.size
//    def get(index: Int) = new Views.SeriesMapView[V](df, index, !transpose)
//
//    override def iterator: Iterator[Map[Any, V]] = {
//      df.itermap
//    }
//
//    //: Map[Any, V]
//    override def apply(index: Int) = new Views.SeriesMapView(df, index, !transpose).toMap
//  }
//
//  class SeriesMapView[V](val df: DataFrame[V], val index: Int, val transpose: Boolean) extends AbstractMap[Any, V] {
//
//    override def iterator: Iterator[(Any, V)] = {
//      val names = if (transpose) df.getIndex else df.getColumns
//      val it = names.iterator
//      new Iterator[(Any, V)] {
//        private var valueIdx = 0
//
//        override def hasNext: Boolean = it.hasNext
//
//        override def next(): (Any, V) = {
//          val key = it.next()
//          val value = if (transpose) df.get(valueIdx, index) else df.get(index, valueIdx)
//          valueIdx += 1
//          (key, value)
//        }
//      }
//    }
//
//    def get(key: Any): Option[V] = {
//      val names = if (transpose) df.getIndex else df.getColumns
//      if (names.contains(key)) {
//        val idx = names.toList.indexOf(key)
//        Some(if (transpose) df.get(idx, index) else df.get(index, idx))
//      } else {
//        None
//      }
//    }
//
//    override def -(key: Any): collection.Map[Any, V] = ???
//
//    override def -(key1: Any, key2: Any, keys: Any*): collection.Map[Any, V] = ???
//  }
//
//  class TransformedView[V, U](val df: DataFrame[V], val transform: Function[V, U], val transpose: Boolean) extends AbstractSeq[List[U]] {
//    override def length: Int = if (transpose) df.length else df.size
//    def get(index: Int) = new Views.TransformedSeriesView[V, U](df, transform, index, !transpose)
//
//
//    override def apply(index: Int): List[U] = new TransformedSeriesView(df, transform, index, !transpose).toList
//
//    override def iterator: Iterator[List[U]] = ???
////      {
////
////      df.toList.map(_.toList).iterator
////    }
//  }
//
//  class TransformedSeriesView[V, U](val df: DataFrame[V], val transform: Function[V, U], val index: Int, val transpose: Boolean) extends AbstractSeq[U] {
//    override def length: Int = if (transpose) df.length else df.size
//
//    def get(index: Int): U = {
//      val value = if (transpose) df.get(index, this.index)
//      else df.get(this.index, index)
//      transform.apply(value)
//    }
//    override def apply(index: Int): U = {
//      val value = if (transpose) df.get(index, this.index)
//      else df.get(this.index, index)
//      transform.apply(value)
//    }
//
//    override def iterator: Iterator[U] = ???
////    {
////
////    }
//  }
//
//  class FlatView[V](val df: DataFrame[V]) extends AbstractSeq[V] {
//    override def length: Int = df.size * df.length
////    override def size: Int = df.size * df.length
//    def get(index: Int): V = df.get(index % df.length, index / df.length)
//
//
//    override def apply(index: Int): V = df.get(index % df.length, index / df.length)
//
//    override def iterator: Iterator[V] = df.flatten.iterator
//  }
//
//  class FillNaFunction[V](val fill: V) extends Function[V, V] {
//    override def apply(value: V): V = if (value == null) fill else value
//  }
//}
