
ThisBuild / scalaVersion := "3.6.4"

lazy val root = (project in file("."))
  .settings(
    name := "storch-pandas"
  )

import sbt.*
import Keys.*
import sbt.Def.settings

import scala.collection.Seq

//lazy val root = project
//  .enablePlugins(NoPublishPlugin)
//  .in(file("."))
////  .aggregate(core, vision, examples, docs)
//  .settings(
//    javaCppVersion := (ThisBuild / javaCppVersion).value,
////    csrCacheDirectory := file("D:\\coursier"),
//  )

ThisBuild / tlBaseVersion := "0.0" // your current series x.y
//ThisBuild / CoursierCache := file("D:\\coursier")
ThisBuild / organization := "io.github.mullerhai" //"dev.storch"
ThisBuild / organizationName := "storch.dev"
ThisBuild / startYear := Some(2024)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("mullerhai", "mullerhai")
)
ThisBuild / version := "0.1.5"

ThisBuild / scalaVersion := "3.6.4"
ThisBuild / tlSonatypeUseLegacyHost := false

import xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommandAndRemaining("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges,
)

val minimumJavaVersion = 18


fork in run := true
ThisBuild / fork := true
//ThisBuild / JavaVersion
//ThisBuild / javaOptions in run ++= {
//  val currentJavaVersion = sys.props("java.specification.version").toDouble
//  if (currentJavaVersion < minimumJavaVersion) {
//    sys.error(s"Java version $currentJavaVersion is not supported. Minimum required version is $minimumJavaVersion.")
//  }
//  Seq.empty
//}

//ThisBuild / version := "0.1.0-SNAPSHOT"


ThisBuild / tlSitePublishBranch := Some("main")

ThisBuild / apiURL := Some(new URL("https://storch.dev/api/"))
ThisBuild / tlSonatypeUseLegacyHost := false

// publish website from this branch
ThisBuild / tlSitePublishBranch := Some("main")
ThisBuild / homepage := Some(new URL("https://storch.dev/api/"))
ThisBuild / scmInfo := Some( ScmInfo( url( "https://github.com/mullerhai/storch-pandas" ), "scm:git:https://github.com/mullerhai/storch-pandas.git" ) )
//ThisBuild / scmInfo := Some(new ScmInfo("https://github.com/mullerhai/storch-k8s.git"))
//val scrImageVersion = "4.3.0" //4.0.34
libraryDependencies += "com.lihaoyi" %% "requests" % "0.9.0"
// https://mvnrepository.com/artifact/com.lihaoyi/upickle
libraryDependencies += "com.lihaoyi" %% "upickle" % "4.2.1"
// https://mvnrepository.com/artifact/com.lihaoyi/os-lib
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.11.5-M10"
// https://mvnrepository.com/artifact/com.lihaoyi/ujson
libraryDependencies += "com.lihaoyi" %% "ujson" % "4.2.1"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.18.1" % Test
libraryDependencies += "io.github.mullerhai" % "storch-pickle_3" % "0.1.4"
libraryDependencies += "io.github.mullerhai" % "storch-numpy_3" % "0.1.6"
libraryDependencies += "io.github.mullerhai" % "storch-polars_3" % "0.1.5"
libraryDependencies += "io.github.mullerhai" % "storch-safe-tensor_3" % "0.1.1"
//libraryDependencies += "io.github.mullerhai" % "storch-scikit-learn_3" % "0.1.2" % Test exclude("org.scala-lang.modules","scala-collection-compat_2.13") exclude("org.typelevel","algebra_2.13")exclude("org.typelevel","cats-kernel_2.13")
//libraryDependencies += "io.circe" %% "circe-core" % circeVersion,
// https://mvnrepository.com/artifact/com.google.doclava/doclava
libraryDependencies += "com.google.doclava" % "doclava" % "1.0.6"
// https://mvnrepository.com/artifact/org.apache.derby/derby
libraryDependencies += "org.apache.derby" % "derby" % "10.17.1.0"
// https://mvnrepository.com/artifact/org.mozilla/rhino
libraryDependencies += "org.mozilla" % "rhino" % "1.8.0"
// https://mvnrepository.com/artifact/org.jline/jline
libraryDependencies += "org.jline" % "jline" % "3.30.1"
// https://mvnrepository.com/artifact/org.aspectj/aspectjrt
libraryDependencies += "org.aspectj" % "aspectjrt" % "1.8.2" // % "runtime"
// https://mvnrepository.com/artifact/org.knowm.xchart/xchart
libraryDependencies += "org.knowm.xchart" % "xchart" % "3.8.8"
// https://mvnrepository.com/artifact/com.xeiam.xchart/xchart
libraryDependencies += "com.xeiam.xchart" % "xchart" % "2.5.1"
// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "5.4.1"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "5.4.1"
// https://mvnrepository.com/artifact/io.jhdf/jhdf
libraryDependencies += "io.jhdf" % "jhdf" % "0.9.4"
// https://mvnrepository.com/artifact/org.apache.commons/commons-math4-core
libraryDependencies += "org.apache.commons" % "commons-math4-core" % "4.0-beta1"
libraryDependencies += "org.scalanlp" %% "breeze" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
//libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
// https://mvnrepository.com/artifact/net.sf.supercsv/super-csv
libraryDependencies += "net.sf.supercsv" % "super-csv" % "2.4.0"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.1.0-alpha1"
// https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-annotation
libraryDependencies += "io.dropwizard.metrics" % "metrics-annotation" % "4.2.30"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "4.2.30"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.11"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.1.0-alpha1" % Test
libraryDependencies += "org.specs2" %% "specs2-core" % "5.6.4" % Test
libraryDependencies += "org.scalameta" %% "munit" % "1.1.1" % Test
libraryDependencies += "org.scalameta" %% "munit-scalacheck" % "1.1.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test
//// https://mvnrepository.com/artifact/com.lancedb/lance-core
//libraryDependencies += "com.lancedb" % "lance-core" % "0.31.0"
//// https://mvnrepository.com/artifact/com.lancedb/lancedb-core
//libraryDependencies += "com.lancedb" % "lancedb-core" % "0.0.3"
//// https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc
//libraryDependencies += "org.duckdb" % "duckdb_jdbc" % "1.3.2.0"
////// https://mvnrepository.com/artifact/io.milvus/milvus-sdk-java
//libraryDependencies += "io.milvus" % "milvus-sdk-java" % "2.6.1"
////// https://mvnrepository.com/artifact/io.milvus/milvus-sdk-java-bulkwriter
//libraryDependencies += "io.milvus" % "milvus-sdk-java-bulkwriter" % "2.6.1"
//// https://mvnrepository.com/artifact/org.apache.avro/avro-ipc

//// https://mvnrepository.com/artifact/org.apache.hive/hive-common
//libraryDependencies += "org.apache.hive" % "hive-common" % "4.0.1"
////// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "4.0.1"
// https://mvnrepository.com/artifact/org.apache.arrow/arrow-vector
//libraryDependencies += "org.apache.arrow" % "arrow-vector" % "18.3.0"
//// https://mvnrepository.com/artifact/org.apache.arrow/arrow-memory
//libraryDependencies += "org.apache.arrow" % "arrow-memory" % "18.3.0" pomOnly()
//// https://mvnrepository.com/artifact/org.apache.arrow/arrow-memory-core
//libraryDependencies += "org.apache.arrow" % "arrow-memory-core" % "18.3.0"
//// https://mvnrepository.com/artifact/org.apache.arrow/arrow-memory-unsafe
//libraryDependencies += "org.apache.arrow" % "arrow-memory-unsafe" % "18.3.0" % Test
//// https://mvnrepository.com/artifact/com.epam/parso
//libraryDependencies += "com.epam" % "parso" % "2.0.14"
//libraryDependencies += "com.github.luben" % "zstd-jni" % "1.5.6-4"

//// https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
//libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.50.2.0"
// required by parquet
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.4.1" % Provided exclude("org.slf4j", "slf4j-log4j12") exclude ("org.apache.hadoop.thirdparty","hadoop-shaded-protobuf")
// https://mvnrepository.com/artifact/org.apache.hadoop.thirdparty/hadoop-shaded-protobuf_3_25
//libraryDependencies += "org.apache.hadoop.thirdparty" % "hadoop-shaded-protobuf_3_25" % "1.4.0"
//excludeDependencies += "com.github.luben" % "zstd-jni" % "1.5.0-1"
//libraryDependencies += "org.apache.avro" % "avro" % "1.12.0" % Provided exclude("org.slf4j", "slf4j-log4j12")
//libraryDependencies += "org.apache.avro" % "avro-ipc" % "1.12.0"  exclude("com.github.luben", "zstd-jni")

//libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.15.2" % Provided exclude("org.slf4j", "slf4j-log4j12") exclude("com.github.luben", "zstd-jni")
ThisBuild  / assemblyMergeStrategy := {
  case v if v.contains("module-info.class")   => MergeStrategy.discard
  case v if v.contains("UnusedStub")          => MergeStrategy.first
  case v if v.contains("aopalliance")         => MergeStrategy.first
  case v if v.contains("inject")              => MergeStrategy.first
  case v if v.contains("jline")               => MergeStrategy.discard
  case v if v.contains("scala-asm")           => MergeStrategy.discard
  case v if v.contains("asm")                 => MergeStrategy.discard
  case v if v.contains("scala-compiler")      => MergeStrategy.deduplicate
  case v if v.contains("reflect-config.json") => MergeStrategy.discard
  case v if v.contains("jni-config.json")     => MergeStrategy.discard
  case v if v.contains("git.properties")      => MergeStrategy.discard
  case v if v.contains("reflect.properties")      => MergeStrategy.discard
  case v if v.contains("compiler.properties")      => MergeStrategy.discard
  case v if v.contains("scala-collection-compat.properties")      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}