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

import java.io.File
import java.lang.annotation.Annotation
import java.lang.reflect.Constructor

import scala.collection.mutable.*
import scala.collection.mutable.LinkedHashMap

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.Signature
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.ConstructorSignature
import org.aspectj.lang.reflect.FieldSignature
import org.aspectj.lang.reflect.MethodSignature

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.SharedMetricRegistries
import com.codahale.metrics.Timer
import com.codahale.metrics._
import com.codahale.metrics.annotation.Timed

@Aspect
object Metrics {
  private val registry = SharedMetricRegistries.getOrCreate("storch")

  private def getAnnotation(
      signature: Signature,
      annotationClass: Class[? <: Annotation],
  ): Annotation = {
    if (signature.isInstanceOf[ConstructorSignature]) {
      val ctor = classOf[ConstructorSignature].cast(signature).getConstructor
      return ctor.getAnnotation(annotationClass)
    } else if (signature.isInstanceOf[MethodSignature])
      return classOf[MethodSignature].cast(signature).getMethod
        .getAnnotation(annotationClass)
    else if (signature.isInstanceOf[FieldSignature])
      return classOf[FieldSignature].cast(signature).getField
        .getAnnotation(annotationClass)
    throw new RuntimeException(
      "Unsupported signature type " + signature.getClass.getName,
    )
  }

  private def name(
      signature: Signature,
      name: String,
      suffix: String,
      absolute: Boolean,
  ): String = {
    var sig = signature.toString
    // trim return type
    val index = sig.indexOf(" ")
    if (index > 0) sig = sig.substring(index + 1)
    if (name.isEmpty) return MetricRegistry.name(sig, suffix)
    if (!absolute) return MetricRegistry.name(sig, name)
    name
  }

  def displayMetrics(): Unit = {
    ConsoleReporter.forRegistry(registry).build.report()
    CsvReporter.forRegistry(registry).build(new File("target/")).report()
  }
}

@Aspect
class Metrics {
  @Around("execution(@com.codahale.metrics.annotation.Timed * *(..))")
  @throws[Throwable]
  def injectTimer(point: ProceedingJoinPoint): AnyRef = {
    val signature = point.getSignature
    val annotation = Metrics.getAnnotation(signature, classOf[Timed])
    val timed = classOf[Timed].cast(annotation)
    val name = Metrics.name(signature, timed.name, "timer", timed.absolute)
    val timer = Metrics.registry.timer(name)
    val context = timer.time
    try point.proceed(point.getArgs)
    finally context.stop
  }
}
