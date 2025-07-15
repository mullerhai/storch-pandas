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
package torch.pandas.operate.adapter

import java.io.IOException
import java.lang.reflect.Method
import java.util
import scala.collection.Set as KeySet
import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.LinkedHashSet
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import org.mozilla.javascript.Context
import org.mozilla.javascript.Function
import org.mozilla.javascript.NativeArray
import org.mozilla.javascript.NativeJavaObject
import org.mozilla.javascript.ScriptRuntime
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.ScriptableObject
import torch.pandas.DataFrame.Aggregate
import torch.pandas.DataFrame.JoinType
import torch.pandas.DataFrame.KeyFunction
import torch.pandas.DataFrame.PlotType
import torch.pandas.DataFrame.Predicate
import torch.pandas.DataFrame.RowFunction
import torch.pandas.DataFrame
import torch.pandas.operate.Grouping

import scala.language.postfixOps
/*
 * there are basically two options for assisting in method resolution
 * from javascript:
 *   1. wrap data frames in a subclass of NativeJavaObject and use get()
 *      to return the appropriate NativeJavaMethod object
 *      issues with this approach include:
 *      - needing to wrap both the object and the method definition
 *      - difficulty in determining the correct method from the argument types
 *   2. wrap data frames in a custom scriptable object with unambiguous
 *      methods that delegates to the underlying data frame
 *      issues with this approach include:
 *      - need to redefine every bit of data frame functionality in terms of javascript
 *      - need to keep up to data as new methods are added to make them available in js
 * after trying each, getting the correct method dynamically is not
 * worth the effort so for now the more verbose delegate class solution
 * wins out.  Java8s nashorn interpreter appears to do a better job
 * resolving methods (via dynalink) so hopefully this is short-lived
 */
@SerialVersionUID(1L)
object DataFrameAdapter {
  private val EMPTY_DF = new DataFrame[AnyRef]

  def jsConstructor(
      ctx: Context,
      args: Array[AnyRef],
      ctor: Function,
      newExpr: Boolean,
  ): Scriptable = {
    if (args.length == 3 && args(0).isInstanceOf[NativeArray]) {
      val data = new ListBuffer[Seq[AnyRef]]
      val array = classOf[NativeArray].cast(args(2))
      val ids = array.getIds
      var i = 0
      while (i < array.getLength) {
        data.append(asList(array.get(ids(i).asInstanceOf[Int], null)))
        i += 1
      }
      val x1 = asList(args(0)).asInstanceOf[Seq[Any]]
      val x2 = asList(args(1)).asInstanceOf[Seq[Any]]
      val x3: List[Seq[AnyRef]] = data.toList // map(_.asInstanceOf[Seq[_.asInstanceOf[AnyRef]]]).toList
      val df = new DataFrame[AnyRef](x1, x2, x3)
      return new DataFrameAdapter(df)
    } else if (args.length == 2 && args(0).isInstanceOf[NativeArray])
      return new DataFrameAdapter(new DataFrame[AnyRef](
        asList(args(0)).asInstanceOf[Seq[Any]],
        asList(args(1)).asInstanceOf[Seq[Any]],
      ))
    else if (args.length == 1 && args(0).isInstanceOf[NativeArray]) {
      val df = new DataFrame[AnyRef](asList(args(0)).map(_.toString)*)
      return new DataFrameAdapter(df)
    } else if (args.length > 0) {
      val columns = new Array[String](args.length)
      for (i <- 0 until args.length) columns(i) = Context.toString(args(i))
      return new DataFrameAdapter(new DataFrame[AnyRef](columns.toSeq*))
    }
    new DataFrameAdapter(new DataFrame[AnyRef])
  }

  private def cast(objects: Scriptable) = classOf[DataFrameAdapter].cast(objects)

  def jsFunction_add(
      ctx: Context,
      scriptable: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    if (args.length == 2 && args(1).isInstanceOf[NativeArray])
      return new DataFrameAdapter(
        scriptable,
        cast(scriptable).df.add(args(0), asList(args(1))),
      )
    if (args.length == 1 && args(0).isInstanceOf[NativeArray])
      return new DataFrameAdapter(
        scriptable,
        cast(scriptable).df.add(asList(args(0))),
      )
    new DataFrameAdapter(scriptable, cast(scriptable).df.add(args))
  }

  def jsFunction_drop(
      ctx: Context,
      scriptable: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ) = {
    val cdf = cast(scriptable).df
    val arg = cdf.drop(args.map(_.asInstanceOf[Int]))
    new DataFrameAdapter(scriptable, arg)
  }

  def jsFunction_retain(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ) = new DataFrameAdapter(
    scriptObject,
    cast(scriptObject).df.retain(args.toSeq.map(_.asInstanceOf[String])),
  )

  def jsFunction_reindex(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    if (args.length > 0 && args(0).isInstanceOf[NativeArray]) {
      if (args.length > 1) return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df
          .reindex(asList(args(0)).toArray, Context.toBoolean(args(1))),
      )
      return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.reindex(asList(args(0)).toArray),
      )
    }
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.reindex(args))
  }

  def jsFunction_append(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    if (args.length == 2 && args(1).isInstanceOf[NativeArray])
      return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.append(args(0), asList(args(1))),
      )
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.append(asList(args(0))),
    )
  }

  def jsFunction_join(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val other = classOf[DataFrameAdapter].cast(args(0)).df
    val types =
      if (args.length > 1 && args(1).isInstanceOf[NativeJavaObject])
        classOf[DataFrame.JoinType]
          .cast(Context.jsToJava(args(1), classOf[DataFrame.JoinType]))
      else null
    if (args.length > 1 && args(args.length - 1).isInstanceOf[Function]) {
      @SuppressWarnings(Array("unchecked"))
      val f = Context
        .jsToJava(args(args.length - 1), classOf[DataFrame.KeyFunction[?]])
        .asInstanceOf[DataFrame.KeyFunction[AnyRef]]
      if (types != null) return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.join(other, types, f),
      )
      return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.join(other, f),
      )
    }
    if (types != null) return new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.join(other, types),
    )
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.join(other))
  }

  def jsFunction_joinOn(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val other = classOf[DataFrameAdapter].cast(args(0)).df
    val types =
      if (args.length > 1 && args(1).isInstanceOf[NativeJavaObject])
        classOf[DataFrame.JoinType]
          .cast(Context.jsToJava(args(1), classOf[DataFrame.JoinType]))
      else null
    if (types != null) return new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.joinOn(
        other,
        types,
        java.util.Arrays
          .copyOfRange(args.map(_.asInstanceOf[Int]), 2, args.length).toSeq,
      ),
    )
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.joinOn(
        other,
        java.util.Arrays
          .copyOfRange(args.map(_.asInstanceOf[Int]), 1, args.length)*,
      ),
    )
  }

  def jsFunction_merge(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val other = classOf[DataFrameAdapter].cast(args(0)).df
    val types =
      if (args.length > 1 && args(1).isInstanceOf[NativeJavaObject])
        classOf[DataFrame.JoinType]
          .cast(Context.jsToJava(args(1), classOf[DataFrame.JoinType]))
      else null
    if (types != null) return new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.merge(other, types),
    )
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.merge(other))
  }

  def jsFunction_update(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val others = new Array[DataFrame[AnyRef]](args.length)
    for (i <- 0 until args.length)
      others(i) = classOf[DataFrameAdapter].cast(args(i)).df
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.update(others*))
  } // update(others: DataFrame[? <: V]*)

  def jsFunction_coalesce(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val others = new Array[DataFrame[AnyRef]](args.length)
    for (i <- 0 until args.length)
      others(i) = classOf[DataFrameAdapter].cast(args(i)).df
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.coalesce(others*))
  } // def coalesce(others: DataFrame[? <: V]*):

  def jsFunction_head(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    // ?? Option[Double] ??? limit ????????
    val limit: Option[Double] =
      if (args.length == 1)
        // Context.toNumber ?? java.lang.Double???????? Some ?
        Some(Context.toNumber(args(0)))
      else None // ??????limit ???

    // ?? limit ?????????? head ??
    val resultDf = limit match {
      case Some(limitValue) =>
        // ? limit ???????? Double??????? intValue
        // Context.toNumber ??????? Double????????? intValue ????
        cast(scriptObject).df.heads(limitValue.intValue)
      case None =>
        // ? limit ??????????? head ??
        cast(scriptObject).df.head()
    }

    // ???? DataFrame ???? DataFrameAdapter
    new DataFrameAdapter(scriptObject, resultDf)
  }
//  def jsFunction_head(ctx: Context, scriptObject: Scriptable, args: Array[AnyRef], func: Function): Scriptable = {
//    val limit = if (args.length == 1) Context.toNumber(args(0))
//    else null
//    new DataFrameAdapter(scriptObject, if (limit != null) cast(scriptObject).df.head(limit.intValue)
//    else cast(scriptObject).df.head)
//  }

  // ... existing imports ...
  // Assuming necessary imports for Context, Scriptable, Function, DataFrameAdapter, cast

  // ... existing code ...

  def jsFunction_tail(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    // ?? Option[Double] ??? limit ????????
    val limit: Option[Double] =
      if (args.length == 1)
        // Context.toNumber ?? java.lang.Double???????? Some ?
        Some(Context.toNumber(args(0)))
      else None // ??????limit ???

    // ?? limit ?????????? tail ??
    val resultDf = limit match {
      case Some(limitValue) =>
        // ? limit ???????? Double??????? intValue
        // Context.toNumber ??????? Double????????? intValue ????
        // ?? df.tail ????????? Int ?
        cast(scriptObject).df.tail(limitValue.intValue)
      case None =>
        // ? limit ??????????? tail ??
        cast(scriptObject).df.tail
    }

    // ???? DataFrame ???? DataFrameAdapter
    new DataFrameAdapter(scriptObject, resultDf)
  }

//  def jsFunction_tail(ctx: Context, scriptObject: Scriptable, args: Array[AnyRef], func: Function): Scriptable = {
//    val limit = if (args.length == 1) Context.toNumber(args(0))
//    else null
//    new DataFrameAdapter(scriptObject, if (limit != null) cast(scriptObject).df.tail(limit)//.intValue)
//    else cast(scriptObject).df.tail)
//  }

  def jsFunction_convert(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    if (args.length > 0) {
      val types = new Array[Class[?]](args.length)
      for (i <- 0 until args.length) types(i) = classOf[Class[?]]
        .cast(Context.jsToJava(args(i), classOf[Class[?]]))
      return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.convert(types*),
      )
    }
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.convert)
  }

  def jsFunction_groupBy(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    if (args.length == 1 && args(0).isInstanceOf[Function]) {
      @SuppressWarnings(Array("unchecked"))
      val f = Context.jsToJava(args(0), classOf[DataFrame.KeyFunction[?]])
        .asInstanceOf[DataFrame.KeyFunction[AnyRef]]
      return new DataFrameAdapter(scriptObject, cast(scriptObject).df.groupBy(f))
    }
    if (args.length == 1 && args(0).isInstanceOf[NativeArray])
      return new DataFrameAdapter(
        scriptObject,
        cast(scriptObject).df.groupBy(asList(args(0)).toArray),
      )
    new DataFrameAdapter(scriptObject, cast(scriptObject).df.groupBy(args))
  }

  def jsFunction_pivot(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val row = Context.jsToJava(args(0), classOf[AnyRef])
    val col = Context.jsToJava(args(0), classOf[AnyRef])
    val values = new Array[AnyRef](args.length - 2)
    for (i <- 0 until values.length)
      values(i) = Context.jsToJava(args(i + 2), classOf[AnyRef])
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.pivot(row, col, values),
    )
  }

  def jsFunction_sortBy(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ) = new DataFrameAdapter(scriptObject, cast(scriptObject).df.sortBy(args))

  def jsFunction_unique(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ) = new DataFrameAdapter(scriptObject, cast(scriptObject).df.unique(args))

  def jsFunction_diff(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val period = if (args.length == 1) Context.toNumber(args(0)) else 1
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.diff(period.intValue),
    )
  }

  def jsFunction_percentChange(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val period = if (args.length == 1) Context.toNumber(args(0)) else 1
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.percentChange(period.intValue),
    )
  }

  def jsFunction_rollapply(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    @SuppressWarnings(Array("unchecked"))
    val f = Context.jsToJava(args(0), classOf[DataFrame.Function[?, ?]])
      .asInstanceOf[DataFrame.Function[Seq[AnyRef], AnyRef]]
    val period = if (args.length == 2) Context.toNumber(args(1)) else 1
    new DataFrameAdapter(
      scriptObject,
      cast(scriptObject).df.rollapply(f, period.intValue),
    )
  }

  def jsFunction_plot(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): AnyRef = {
    if (args.length > 0) {
      val types = classOf[DataFrame.PlotType]
        .cast(Context.jsToJava(args(0), classOf[DataFrame.PlotType]))
      cast(scriptObject).df.plot(types)
      return Context.getUndefinedValue
    }
    cast(scriptObject).df.plot()
    Context.getUndefinedValue
  }

  @throws[IOException]
  def jsStaticFunction_readCsv(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val file = Context.toString(args(0))
    val df = DataFrame.readCsv(file)
    new DataFrameAdapter(
      ctx.newObject(scriptObject, df.getClass.getSimpleName),
      df,
    )
  }

  @throws[IOException]
  def jsStaticFunction_readXls(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): Scriptable = {
    val file = Context.toString(args(0))
    val df = DataFrame.readXls(file)
    new DataFrameAdapter(
      ctx.newObject(scriptObject, df.getClass.getSimpleName),
      df,
    )
  }

//  def jsFunction_toArray(ctx: Context, scriptObject: Scriptable, args: Array[AnyRef], func: Function): Scriptable = {
//    val arr  = cast(scriptObject).df.toArray
//    ctx.newArray(scriptObject, arr)
//  }

  // ... existing imports ...
  // Assuming necessary imports for Context, Scriptable, Function, DataFrameAdapter, cast

  // ... existing code ...

  def jsFunction_toString(
      ctx: Context,
      scriptObject: Scriptable,
      args: Array[AnyRef],
      func: Function,
  ): AnyRef = {
    // ?? Option[Double] ??? limit ????????
    val limit: Option[Double] =
      if (args.length == 1)
        // Context.toNumber ?? java.lang.Double???????? Some ?
        Some(Context.toNumber(args(0)))
      else None // ??????limit ???

    // ?? limit ?????????? toString ??
    val resultString: String = limit match {
      case Some(limitValue) =>
        // ? limit ???????? Double??????? intValue
        // Context.toNumber ??????? Double????????? intValue ????
        // ?? df.toString ????????? Int ?
        cast(scriptObject).df.toString(limitValue.intValue)
      case None =>
        // ? limit ??????????? toString ??
        cast(scriptObject).df.toString
    }

    // toString ???? String?String ? AnyRef ??????????
    resultString
  }

//  def jsFunction_toString(ctx: Context, scriptObject: Scriptable, args: Array[AnyRef], func: Function): AnyRef = {
//    val limit = if (args.length == 1) Context.toNumber(args(0))
//    else null
//    if (limit != null) cast(scriptObject).df.toString(limit) //limit.intValue
//    else cast(scriptObject).df.toString
//  }

  private def asList(array: AnyRef) = List(classOf[NativeArray].cast(array))

  private def asList(array: NativeArray) = {
    val list = new ListBuffer[AnyRef]() // array.getLength.toInt)
    for (id <- array.getIds) list.append(array.get(id.asInstanceOf[Int], null))
    list
  }
}

@SerialVersionUID(1L)
class DataFrameAdapter extends ScriptableObject {
  this.df = DataFrameAdapter.EMPTY_DF
  private final var df: DataFrame[AnyRef] = null

  def this(df: DataFrame[AnyRef]) = {
    this()
    this.df = df
  }

  def this(scope: Scriptable, df: DataFrame[AnyRef]) = {
    this()
    this.df = df
    setParentScope(scope.getParentScope)
    setPrototype(scope.getPrototype)
  }

  def jsFunction_resetIndex = new DataFrameAdapter(this, df.resetIndex)

  def jsFunction_rename(old: AnyRef, name: AnyRef) =
    new DataFrameAdapter(this, df.rename(old, name))

  def jsFunction_reshape(rows: Int, cols: Int) =
    new DataFrameAdapter(this, df.reshape(rows, cols))

  def jsFunction_size: Int = df.size

  def jsFunction_length: Int = df.length

  def jsFunction_isEmpty: Boolean = df.isEmpty

  def jsFunction_index: Seq[Any] = df.getIndex

  def jsFunction_columns: Seq[Any] = df.getColumns

  def jsFunction_get(row: Int, col: Int): AnyRef = df.getFromIndex(row, col)

  def jsFunction_slice(rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int) =
    new DataFrameAdapter(this, df.slice(rowStart, rowEnd, colStart, colEnd))

  def jsFunction_set(row: Int, col: Int, value: Scriptable): Unit = df
    .set(row, col, Context.jsToJava(value, classOf[AnyRef]))

  def jsFunction_col(column: Int): Seq[AnyRef] = df.colInt(column)

  def jsFunction_row(row: Int): Seq[AnyRef] = df.row_index(row).asScala.toSeq

  def jsFunction_select(predicate: Function): DataFrameAdapter = {
    @SuppressWarnings(Array("unchecked"))
    val p = Context.jsToJava(predicate, classOf[DataFrame.Predicate[?]])
      .asInstanceOf[DataFrame.Predicate[AnyRef]]
    new DataFrameAdapter(this, df.select(p))
  }

  def jsFunction_flatten: Seq[AnyRef] = df.flatten.toSeq

  def jsFunction_transpose = new DataFrameAdapter(this, df.transpose)

  def jsFunction_apply(function: Function): DataFrameAdapter = {
    @SuppressWarnings(Array("unchecked"))
    val f = Context.jsToJava(function, classOf[DataFrame.Function[?, ?]])
      .asInstanceOf[DataFrame.Function[AnyRef, AnyRef]]
    new DataFrameAdapter(this, df.apply(f))
  }

  def jsFunction_transform(function: Function): DataFrameAdapter = {
    @SuppressWarnings(Array("unchecked"))
    val f = Context.jsToJava(function, classOf[DataFrame.RowFunction[?, ?]])
      .asInstanceOf[DataFrame.RowFunction[AnyRef, AnyRef]]
    new DataFrameAdapter(this, df.transform(f))
  }

  def jsFunction_isnull =
    new DataFrameAdapter(this, df.isnull.cast(classOf[AnyRef]))

  def jsFunction_notnull =
    new DataFrameAdapter(this, df.notnull.cast(classOf[AnyRef]))

  def jsFunction_groups: Grouping[?] = df.groups

  def jsFunction_explode: LinkedHashMap[Any, DataFrame[AnyRef]] = df.explode

  def jsFunction_aggregate(function: Function): DataFrameAdapter = {
    @SuppressWarnings(Array("unchecked"))
    val f = Context.jsToJava(function, classOf[DataFrame.Aggregate[?, ?]])
      .asInstanceOf[DataFrame.Aggregate[AnyRef, AnyRef]]
    new DataFrameAdapter(this, df.aggregate(f))
  }

  def jsFunction_count = new DataFrameAdapter(this, df.count)

  def jsFunction_collapse = new DataFrameAdapter(this, df.collapse)

  def jsFunction_sum = new DataFrameAdapter(this, df.sum)

  def jsFunction_prod = new DataFrameAdapter(this, df.prod)

  def jsFunction_mean = new DataFrameAdapter(this, df.mean)

  def jsFunction_stddev = new DataFrameAdapter(this, df.stddev)

  def jsFunction_var = new DataFrameAdapter(this, df.`var`)

  def jsFunction_skew = new DataFrameAdapter(this, df.skew)

  def jsFunction_kurt = new DataFrameAdapter(this, df.kurt)

  def jsFunction_min = new DataFrameAdapter(this, df.min)

  def jsFunction_max = new DataFrameAdapter(this, df.max)

  def jsFunction_median = new DataFrameAdapter(this, df.median)

  def jsFunction_cumsum = new DataFrameAdapter(this, df.cumsum)

  def jsFunction_cumprod = new DataFrameAdapter(this, df.cumprod)

  def jsFunction_cummin = new DataFrameAdapter(this, df.cummin)

  def jsFunction_cummax = new DataFrameAdapter(this, df.cummax)

  def jsFunction_describe = new DataFrameAdapter(this, df.describe)

  def jsFunction_types: Seq[Class[?]] = df.types

  def jsFunction_numeric =
    new DataFrameAdapter(this, df.numeric.cast(classOf[AnyRef]))

  def jsFunction_nonnumeric = new DataFrameAdapter(this, df.nonnumeric)

  def jsFunction_map(key: AnyRef, value: AnyRef): LinkedHashMap[Any, Seq[Any]] =
    df.map(key, value)

  def jsFunction_show(): Unit = df.show()

  @throws[IOException]
  def jsFunction_writeCsv(file: String): Unit = df.writeCsv(file)

  @throws[IOException]
  def jsFunction_writeXls(file: String): Unit = df.writeXls(file)

  override def getDefaultValue(hint: Class[?]): Any = {
    if (hint eq ScriptRuntime.BooleanClass) return df.isEmpty
    df.toString
  }

  override def getClassName: String = df.getClass.getSimpleName

  override def getIds: Array[AnyRef] = {
    val ids = new ListBuffer[String]
    for (m <- getClass.getMethods) {
      val name = m.getName
      if (name.startsWith("js") && name.contains("_")) ids
        .append(name.substring(name.indexOf('_') + 1))
    }
    ids.toArray
  }

  override def getAllIds: Array[AnyRef] = getIds

  override def equals(o: Any): Boolean = df == o

  override def hashCode: Int = df.hashCode
}
