///*
// * storch -- Data frames for Java
// * Copyright (c) 2014, 2015 IBM Corp.
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package torch.operate
//
//import scala.collection.mutable
//import torch.DataFrame
//import torch.DataFrame.RowFunction
//
//class Indexs(private val indexMap: mutable.LinkedHashMap[Any, Int] = mutable.LinkedHashMap.empty) {
//  def this(names: Iterable[Any]) = this(mutable.LinkedHashMap.from(names.zipWithIndex))
//
//  def this(names: Iterable[Any], size: Int) = {
//    this()
//    val it = names.iterator
//    for (i <- 0 until size) {
//      val name = if (it.hasNext) it.next() else i
//      add(name, i)
//    }
//  }
//
//  def add(name: Any, value: Int): Unit = {
//    if (indexMap.contains(name)) {
//      throw new IllegalArgumentException(s"duplicate name '$name' in index")
//    }
//    indexMap(name) = value
//  }
//
//  def extend(size: Int): Unit = {
//    for (i <- indexMap.size until size) {
//      add(i, i)
//    }
//  }
//
//  def set(name: Any, value: Int): Unit = {
//    indexMap(name) = value
//  }
//
//  def get(name: Any): Int = {
//    indexMap.getOrElse(name, throw new IllegalArgumentException(s"name '$name' not in index"))
//  }
//
//  def rename(names: mutable.Map[Any, Any]): Unit = {
//    val newIndex = mutable.LinkedHashMap.empty[Any, Int]
//    for ((col, value) <- indexMap) {
//      if (names.contains(col)) {
//        newIndex(names(col)) = value
//      } else {
//        newIndex(col) = value
//      }
//    }
//    indexMap.clear()
//    indexMap ++= newIndex
//  }
//
//  def names(): Set[Any] = indexMap.keySet
//
//  def indices(names: Seq[Any]): Seq[Int] = {
//    names.map(get)
//  }
//
//  def indices(names: Array[Any]): Array[Int] = {
//    indices(names.toSeq).toArray
//  }
//}
//
//object Indexs {
//  def reindex[V](df: DataFrame[V], cols: Int*): DataFrame[V] = {
//    val transformed = df.transform {
//      if (cols.length == 1) {
//        new RowFunction[V, Any] {
//          override def apply(values: List[V]): List[List[Any]] = {
//            List(List(values(cols.head)))
//          }
//        }
//      } else {
//        new RowFunction[V, Any] {
//          override def apply(values: List[V]): List[List[Any]] = {
//            val key = mutable.ListBuffer[Any]()
//            for (c <- cols) {
//              key.addOne(values(c))
//            }
//            List(List(key.toList))
//          }
//        }
//      }
//    }
//    new DataFrame[V](
//      transformed.col(0),
//      df.columns(),
//      new storch.impl.Views.ListView[V](df, false)
//    )
//  }
//
//  def reset[V](df: DataFrame[V]): DataFrame[V] = {
//    val newIndex = (0 until df.length()).toList
//    new DataFrame[V](
//      newIndex,
//      df.columns(),
//      new storch.impl.Views.ListView[V](df, false)
//    )
//  }
//}
