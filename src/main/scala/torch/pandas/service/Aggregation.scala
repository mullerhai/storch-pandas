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
package torch.pandas.service

import org.apache.commons.math3.stat.correlation.StorelessCovariance
import org.apache.commons.math3.stat.descriptive.moment.{StandardDeviation as KStandardDeviation, Variance as KVariance}
import org.apache.commons.math3.stat.descriptive.moment.{StandardDeviation, Kurtosis as KKurtosis, Mean as KMean, Skewness as KSkewness}
import org.apache.commons.math3.stat.descriptive.rank.{Max as KMax, Median as KMedian, Min as KMin, Percentile as KPercentile}
import org.apache.commons.math3.stat.descriptive.summary.{Product as KProduct, Sum as KSum}
import org.apache.commons.math3.stat.descriptive.{StatisticalSummary, StorelessUnivariateStatistic, SummaryStatistics, UnivariateStatistic}
import torch.pandas.DataFrame
import torch.pandas.DataFrame.Aggregate

import scala.collection.mutable
import scala.collection.mutable.{HashSet, LinkedHashMap, ListBuffer}
enum AggregationType:
  case Count, Unique, Collapse, Sum, Product, Mean, StdDev, Variance, Skew,
    Kurtosis, Min, Max, Median, Percentile, Describe

object Aggregation {
  class Count[V] extends DataFrame.Aggregate[V, Number] {
    override def apply(values: Seq[V]) = values.size
  }

  class Unique[V] extends DataFrame.Aggregate[V, V] {
    override def apply(values: Seq[V]): V = {
      val unique = mutable.Set.from(values) // new HashSet[V]()//values)
      if (unique.size > 1)
        throw new IllegalArgumentException("values not unique: " + unique)
      values.head
    }
  }

  class Collapse[V](private val delimiter: String = ",")
      extends DataFrame.Aggregate[V, String] {
    def this() = this(",")

    override def apply(values: Seq[V]): String = {
      val seen = new HashSet[V]
      val sb = new StringBuilder

      for (value <- values) if (!seen.contains(value)) {
        if (sb.length > 0) sb.append(delimiter)
        sb.append(String.valueOf(value))
        seen.add(value)
      }
      sb.toString
    }
  }

  abstract class AbstractStorelessStatistic[V] protected (
      protected val stat: StorelessUnivariateStatistic,
  ) extends DataFrame.Aggregate[V, Number] {
    override def apply(values: Seq[V]): Number = {
      stat.clear()

      for (value <- values) if (value != null) {
        val numValue = value match
          case b: Boolean => if b then 1 else 0
          case _ => if (value.isInstanceOf[Number]) then value.asInstanceOf[Number] else 0 // throw new IllegalArgumentException("value not a number: " + value)
//          if (value.isInstanceOf[Boolean]) value = if (classOf[Boolean].cast(value)) 1
//          else 0
        stat.increment(classOf[Number].cast(numValue).doubleValue)
      }
      stat.getResult
    }
  }

  class Sum[V] extends Aggregation.AbstractStorelessStatistic[V](new KSum) {}

  class Product[V]
      extends Aggregation.AbstractStorelessStatistic[V](new KProduct) {}

  class Mean[V] extends Aggregation.AbstractStorelessStatistic[V](new KMean) {}

  class StdDev[V]
      extends Aggregation.AbstractStorelessStatistic[V](
        new KStandardDeviation,
      ) {}

  class Variance[V]
      extends Aggregation.AbstractStorelessStatistic[V](new KVariance) {}

  class Skew[V] extends Aggregation.AbstractStorelessStatistic[V](new KSkewness) {}

  class Kurtosis[V]
      extends Aggregation.AbstractStorelessStatistic[V](new KKurtosis) {}

  class Min[V] extends Aggregation.AbstractStorelessStatistic[V](new KMin) {}

  class Max[V] extends Aggregation.AbstractStorelessStatistic[V](new KMax) {}

  abstract class AbstractStatistic[V] protected (
      protected val stat: UnivariateStatistic,
  ) extends DataFrame.Aggregate[V, Number] {
    override def apply(values: Seq[V]): Number = {
      var count = 0
      val vals = new Array[Double](values.size)
      for (i, value) <- values.zipWithIndex do
        if value != 0 then // value != null
          vals(count) = value.asInstanceOf[Number].doubleValue
          count += 1
//      for (i <- 0 until vals.length) {
//        val `val` = values.get(i)
//        if (`val` != null) vals({
//          count += 1; count - 1
//        }) = classOf[Number].cast(`val`).doubleValue
//      }
      stat.evaluate(vals, 0, count)
    }
  }

  class Median[V] extends Aggregation.AbstractStatistic[V](new KMedian) {}

  class Percentile[V](quantile: Double)
      extends Aggregation.AbstractStatistic[V](new KPercentile(quantile)) {}

  class Describe[V] extends DataFrame.Aggregate[V, StatisticalSummary] {
    private final val stat = new SummaryStatistics

    override def apply(values: Seq[V]): StatisticalSummary = {
      stat.clear()
      for value <- values do
        if value != null then
          val numValue = value match
            case b: Boolean => if b then 1.asInstanceOf[Number] else 0.asInstanceOf[Number]
            case _ => value.asInstanceOf[Number]
          stat.addValue(numValue.doubleValue())
//      for (i <- 0 until values.size) {
//        val `val` = values.get(i)
//        if (`val` != null) {
//          if (`val`.isInstanceOf[Boolean]) `val` = if (classOf[Boolean].cast(`val`)) 1
//          else 0
//          stat.addValue(classOf[Number].cast(`val`).doubleValue)
//      for (value <- values) {
//        if (value != null) {
//          if (value.isInstanceOf[Boolean]) value = if (classOf[Boolean].cast(value)) 1
//          else 0
//          stat.addValue(classOf[Number].cast(value).doubleValue)
//        }
//      }
      stat.getSummary
    }
  }

  private def name(df: DataFrame[?], row: AnyRef, stat: AnyRef) =
    // df index size > 1 only happens if the aggregate describes a grouped data frame
    if (df.getIndex.size > 1) List(row, stat) else stat

  @SuppressWarnings(Array("unchecked"))
  def describe[V](df: DataFrame[V]): DataFrame[V] = {
    val desc = new DataFrame[V]

//    println(s"describe df->>>>>>>>>>>>>>>>> ${df.getColumns.mkString(", ")}")
    for (col <- df.getColumns)

      for (row <- df.getIndex) {
//        println(s"describe row->>>>>>>>>>>>>>>>> ${row} col->>>>>>>>>>>>>>>>> ${col}")
        val value = df.get(row.asInstanceOf[AnyRef], col)
        if (value.isInstanceOf[StatisticalSummary]) {
          if (!desc.getColumns.contains(col)) {
            desc.add(col)
            if (desc.isEmpty)

              for (r <- df.getIndex)

                for (
                  stat <- List("count", "mean", "std", "var", "max", "min")
                ) {
                  val namez = name(df, r, stat)
                  desc.append(namez, Seq.empty)
                }
          }
//          val summary = value.asInstanceOf[StatisticalSummary]
          val summary = classOf[StatisticalSummary].cast(value)
          desc.set(name(df, row, "count"), col, summary.getN.asInstanceOf[V])
          desc.set(name(df, row, "mean"), col, summary.getMean.asInstanceOf[V])
          desc.set(
            name(df, row, "std"),
            col,
            summary.getStandardDeviation.asInstanceOf[V],
          )
          desc.set(name(df, row, "var"), col, summary.getVariance.asInstanceOf[V])
          desc.set(name(df, row, "max"), col, summary.getMax.asInstanceOf[V])
          desc.set(name(df, row, "min"), col, summary.getMin.asInstanceOf[V])
        }
      }
    desc
  }

  def cov[V](df: DataFrame[V]): DataFrame[Number] = {
    val num = df.numeric
    val cov = new StorelessCovariance(num.size)
    // row-wise copy to double array and increment
    val data = new Array[Double](num.size)

    for (row <- num) {
      for (i <- 0 until row.size) data(i) = row(i).doubleValue
      cov.increment(data)
    }
    // row-wise copy results into new data frame
    val result = cov.getData
    val r = new DataFrame[Number](num.columns)
    val row = new ListBuffer[Number]() // num.size)
    for (i <- 0 until result.length) {
      row.clear()
      for (j <- 0 until result(i).length) row.append(result(i)(j))
      r.append(row.toSeq)
    }
    r
  }
}
