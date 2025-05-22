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

import torch.DataFrame.Function
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic
import torch.DataFrame
//import torch.pandas.operate.Aggregation.Sum
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic
import org.apache.commons.math3.stat.descriptive.summary.Sum
import org.apache.commons.math3.stat.descriptive.summary.Product
import org.apache.commons.math3.stat.descriptive.rank.Min
import org.apache.commons.math3.stat.descriptive.rank.Max

import scala.collection.mutable.{LinkedHashMap, ListBuffer}

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

trait CumulativeFunction[I, O] extends Function[I, O] {
  def reset(): Unit
}

private class AbstractCumulativeFunction(
                                          private val stat: StorelessUnivariateStatistic,
                                          private val initialValue: Double
                                        ) extends CumulativeFunction[Number, Number] {
  reset()

  override def apply(value: Number): Number = {
    stat.increment(value.doubleValue)
    stat.getResult
  }

  override def reset(): Unit = {
    stat.clear()
    stat.increment(initialValue.doubleValue)
  }
}

object Transforms {
  class CumulativeSum extends AbstractCumulativeFunction(new Sum(), 0)

  class CumulativeProduct extends AbstractCumulativeFunction(new Product(), 1)

  class CumulativeMin extends AbstractCumulativeFunction(new Min(), Double.MaxValue)

  class CumulativeMax extends AbstractCumulativeFunction(new Max(), Double.MinValue)
}
//
//object Transforms {
//  trait CumulativeFunction[I, O] extends DataFrame.Function[I, O] {
//    def reset(): Unit
//  }
//
//  private class AbstractCumulativeFunction[V] protected(private val stat: StorelessUnivariateStatistic, private val initialValue: Number) extends Transforms.CumulativeFunction[Number, Number] {
//    reset()
//
//    override def apply(value: Number): Number = {
//      stat.increment(value.doubleValue)
//      stat.getResult
//    }
//
//    override def reset(): Unit = {
//      stat.clear()
//      stat.increment(initialValue.doubleValue)
//    }
//  }
//
//  class CumulativeSum[V] extends Transforms.AbstractCumulativeFunction[V](new Sum, 0) {
//  }
//
//  class CumulativeProduct[V] extends Transforms.AbstractCumulativeFunction[V](new Product, 1) {
//  }
//
//  class CumulativeMin[V] extends Transforms.AbstractCumulativeFunction[V](new Min, Double.MaxValue) {
//  }
//
//  class CumulativeMax[V] extends Transforms.AbstractCumulativeFunction[V](new Max, Double.MinValue) {
//  }
//}