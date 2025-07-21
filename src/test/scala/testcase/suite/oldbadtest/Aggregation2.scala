package testcase.suite.oldbadtest

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
//package torch.pandas.operate
//
//import java.util
//import scala.collection.mutable
//import org.apache.commons.math3.stat.correlation.StorelessCovariance
//import org.apache.commons.math3.stat.descriptive.StatisticalSummary
//import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic
//import org.apache.commons.math3.stat.descriptive.SummaryStatistics
//import org.apache.commons.math3.stat.descriptive.UnivariateStatistic
//import storch.DataFrame
//import storch.DataFrame.Aggregate
//
//object Aggregation:
//  enum AggregationType:
//    case Count, Unique, Collapse, Sum, Product, Mean, StdDev, Variance, Skew, Kurtosis, Min, Max, Median, Percentile, Describe
//
//  class Count[V] extends Aggregate[V, Number]:
//    override def apply(values: List[V]): Number = values.size
//
//  class Unique[V] extends Aggregate[V, V]:
//    override def apply(values: List[V]): V =
//      val uniqueSet = mutable.Set.from(values)
//      if uniqueSet.size > 1 then
//        throw IllegalArgumentException(s"values not unique: $uniqueSet")
//      values.head
//
//  class Collapse[V](private val delimiter: String = ",") extends Aggregate[V, String]:
//    override def apply(values: List[V]): String =
//      val seen = mutable.Set[V]()
//      val sb = new StringBuilder()
//      for value <- values do
//        if !seen.contains(value) then
//          if sb.nonEmpty then
//            sb.append(delimiter)
//          sb.append(value.toString)
//          seen.add(value)
//      sb.toString
//
//  abstract class AbstractStorelessStatistic[V](protected val stat: StorelessUnivariateStatistic) extends Aggregate[V, Number]:
//    override def apply(values: List[V]): Number =
//      stat.clear()
//      for value <- values do
//        if value != null then
//          val numValue = value match
//            case b: Boolean => if b then 1 else 0
//            case _ => value.asInstanceOf[Number]
//          stat.increment(numValue.doubleValue)
//      stat.getResult
//
//  class Sum[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.summary.Sum())
//  class Product[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.summary.Product())
//  class Mean[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.moment.Mean())
//  class StdDev[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.moment.StandardDeviation())
//  class Variance[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.moment.Variance())
//  class Skew[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.moment.Skewness())
//  class Kurtosis[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.moment.Kurtosis())
//  class Min[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.rank.Min())
//  class Max[V] extends AbstractStorelessStatistic[V](new org.apache.commons.math3.stat.descriptive.rank.Max())
//
//  abstract class AbstractStatistic[V](protected val stat: UnivariateStatistic) extends Aggregate[V, Number]:
//    override def apply(values: List[V]): Number =
//      var count = 0
//      val vals = new Array[Double](values.size)
//      for (i, value) <- values.zipWithIndex do
//        if value != null then
//          vals(count) = value.asInstanceOf[Number].doubleValue
//          count += 1
//      stat.evaluate(vals, 0, count)
//
//  class Median[V] extends AbstractStatistic[V](new org.apache.commons.math3.stat.descriptive.rank.Median())
//  class Percentile[V](quantile: Double) extends AbstractStatistic[V](new org.apache.commons.math3.stat.descriptive.rank.Percentile(quantile))
//
//  class Describe[V] extends Aggregate[V, StatisticalSummary]:
//    private val stat = new SummaryStatistics()
//    override def apply(values: List[V]): StatisticalSummary =
//      stat.clear()
//      for value <- values do
//        if value != null then
//          val numValue = value match
//            case b: Boolean => if b then 1 else 0
//            case _ => value.asInstanceOf[Number]
//          stat.addValue(numValue.doubleValue)
//      stat.getSummary
//
//  private def name(df: DataFrame[_], row: Any, stat: Any): Any =
//    if df.index().size > 1 then
//      List(row, stat)
//    else
//      stat
//
//  def describe[V](df: DataFrame[V]): DataFrame[V] =
//    val desc = new DataFrame[V]()
//    for col <- df.columns() do
//      for row <- df.index() do
//        val value = df.get(row, col)
//        if value.isInstanceOf[StatisticalSummary] then
//          if !desc.columns().contains(col) then
//            desc.add(col)
//            if desc.isEmpty() then
//              for r <- df.index() do
//                for stat <- List("count", "mean", "std", "var", "max", "min") do
//                  val nameVal = name(df, r, stat)
//                  desc.append(nameVal, List.empty[V])
//          val summary = value.asInstanceOf[StatisticalSummary]
//          desc.set(name(df, row, "count"), col, summary.getN.asInstanceOf[V])
//          desc.set(name(df, row, "mean"), col, summary.getMean.asInstanceOf[V])
//          desc.set(name(df, row, "std"), col, summary.getStandardDeviation.asInstanceOf[V])
//          desc.set(name(df, row, "var"), col, summary.getVariance.asInstanceOf[V])
//          desc.set(name(df, row, "max"), col, summary.getMax.asInstanceOf[V])
//          desc.set(name(df, row, "min"), col, summary.getMin.asInstanceOf[V])
//    desc
//
//  def cov[V](df: DataFrame[V]): DataFrame[Number] =
//    val num = df.numeric()
//    val cov = new StorelessCovariance(num.size())
//    val data = new Array[Double](num.size())
//    for row <- num do
//      for (i, elem) <- row.zipWithIndex do
//        data(i) = elem.doubleValue
//      cov.increment(data)
//    val result = cov.getData
//    val r = new DataFrame[Number](num.columns())
//    for row <- result do
//      r.append(row.toList)
//    r
