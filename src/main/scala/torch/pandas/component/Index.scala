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

import torch.pandas.DataFrame.RowFunction
import torch.pandas.DataFrame

import scala.collection.{mutable, Set as KeySet}
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.jdk.CollectionConverters.*
object Index {

  def reindex[V](df: DataFrame[V], cols: Int*): DataFrame[V] = {
    val transformed = df.transform {
      if (cols.length == 1) new RowFunction[V, Any] {

        def apply(values: List[V]): List[List[Any]] = List(List(values(cols.head)))

        override def apply(values: Seq[V]): Seq[Seq[Any]] =
          Seq(Seq(values(cols.head)))
      }
      else new RowFunction[V, Any] {

        def apply(values: List[V]): List[List[Any]] = {
          val key = mutable.ListBuffer[Any]()
          for (c <- cols) key.addOne(values(c))
          List(List(key.toList))
        }

        override def apply(values: Seq[V]): Seq[Seq[Any]] = {
          val key = mutable.ListBuffer[Any]()
          for (c <- cols) key.addOne(values(c))
          List(List(key.toList))
        }
      }
    }
    val viewData = new Views.ListView[V](df, false).toSeq // .asScala
      .map(_.asInstanceOf[Seq[V]]).toList
    new DataFrame[V](
      transformed.colInt(0).asInstanceOf[Seq[AnyRef]],
      df.getColumns.asInstanceOf[Seq[AnyRef]],
      viewData,
    )
  }

//  def reindex[V](df: DataFrame[V], cols: Int*) = new DataFrame[V](
//    df.transform(if (cols.length == 1) new DataFrame.RowFunction[V, AnyRef]() {
//    override def apply(values:  Seq[V]):  Seq[ Seq[AnyRef]] = return Collections.singletonList[ Seq[AnyRef]](Collections.singletonList[AnyRef](values.get(cols(0))))
//  }
//  else new DataFrame.RowFunction[V, AnyRef]() {
//    override def apply(values:  Seq[V]):  Seq[ Seq[AnyRef]] = {
//      val key = new  ListBuffer[AnyRef](cols.length)
//      for (c <- cols) {
//        key.add(values.get(c))
//      }
//      Collections.singletonList[ Seq[AnyRef]](Collections.singletonList[AnyRef](Collections.unmodifiableList(key)))
//    }
//  }).col(0), df.columns, new Views.ListView[V](df, false))
//
  def reset[V](df: DataFrame[V]): DataFrame[V] = {
    val index = (0 until df.length).toList
    // new  ListBuffer[AnyRef](df.length)
//    for (i <- 0 until df.length) {
//      index.add(i)
//    }
    val viewData = new Views.ListView[V](df, false).toSeq //asScala
      .map(_.asInstanceOf[Seq[V]]).toList
    new DataFrame[V](
      index.asInstanceOf[Seq[AnyRef]],
      df.getColumns.asInstanceOf[Seq[AnyRef]],
      viewData,
    )
  }
}

class Index(
    private val indexMap: mutable.LinkedHashMap[AnyRef, Int] =
      mutable.LinkedHashMap.empty,
) {
//  val it: Iterator[?] = names.iterator
  def this() = this(
    new mutable.LinkedHashMap[AnyRef, Int]().addAll(List.empty[AnyRef].zipWithIndex),
  )

  def this(names: Iterable[AnyRef]) =
    this(new mutable.LinkedHashMap[AnyRef, Int]().addAll(names.zipWithIndex))

  def this(names: Iterable[AnyRef], size: Int) = {
    this()
    val it = names.iterator
    for (i <- 0 until size) {
      val name = if (it.hasNext) it.next() else i
      add(name.asInstanceOf[AnyRef], i)
    }
  }

  def add(name: AnyRef, value: Int): Unit = {
    if (indexMap.contains(name))
      throw new IllegalArgumentException(s"duplicate name '$name' in index")
    indexMap(name) = value
  }

//class Index(names: Seq[?], size: Int) {
//  var indexMap = new mutable.LinkedHashMap[AnyRef, Int]
//  val it: Iterator[?] = names.iterator
//  for (i <- 0 until size) {
//    val name = if (it.hasNext) it.next
//    else i
//    add(name, i)
//  }
////  final private var index: LinkedHashMap[AnyRef, Int] = null
//
//  def this(names: Seq[?])= {
//    this(names, names.size)
//  }
//
//  def this ={
//    this(Seq[AnyRef])
//  }

//  def add(name: AnyRef, value: Int): Unit = {
//    if (indexMap.put(name, value) != null)
//      throw new IllegalArgumentException("duplicate name '" + name + "' in index")
//  }

  def extend(size: Int): Unit = for (i <- indexMap.size until size) add(i.asInstanceOf[AnyRef], i)

  def set(name: AnyRef, value: Int): Unit = indexMap.put(name, value)

  def get(name: AnyRef): Int = {
    println(s"Class Index get func  name ${name}")
    val i = indexMap.get(name) //.get.asInstanceOf[Int]
    if (i == null || !i.isDefined)
      throw new IllegalArgumentException("name '" + name + "' not in index indexMap: " + indexMap.toSeq.mkString(",") + "")
    val index = i.get
    println(s"Class Index col or row name ${name}, index ${index}")
    index
  }

  def rename(names: Map[AnyRef, AnyRef]): Unit = {
    val idx = new mutable.LinkedHashMap[AnyRef, Int]

    for ((col, value) <- indexMap)
      if (names.keySet.contains(col)) idx.put(names.get(col), value)
      else idx.put(col, value)
    // clear and add all names back to preserve insertion order
    indexMap.clear()
    indexMap ++= idx
  }

  def names = indexMap.toSeq.map(_._1).toSeq

  def indices(names: Array[AnyRef]): Array[Int] = indices(names.toSeq)

  def indices(names: Seq[AnyRef]): Array[Int] = {
    val size = names.size
    val indices = new Array[Int](size)
    for (i <- 0 until size) {
      println(s"Class Index indices name ${names(i)},")
      println(s"Class Index indices name ${names(i)}, index ${get(names(i))}")
      indices(i) = get(names(i))
    }
    indices
//    Array(3,4,5)
  }
}
