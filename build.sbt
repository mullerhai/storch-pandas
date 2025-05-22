ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

lazy val root = (project in file("."))
  .settings(
    name := "storch-pandas"
  )

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
//libraryDependencies += "org.knowm.xchart" % "xchart" % "3.8.8"
// https://mvnrepository.com/artifact/com.xeiam.xchart/xchart
libraryDependencies += "com.xeiam.xchart" % "xchart" % "2.5.1"
// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "5.4.1"
// https://mvnrepository.com/artifact/org.apache.commons/commons-math4-core
libraryDependencies += "org.apache.commons" % "commons-math4-core" % "4.0-beta1"
// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
//libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
// https://mvnrepository.com/artifact/net.sf.supercsv/super-csv
libraryDependencies += "net.sf.supercsv" % "super-csv" % "2.4.0"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.1.0-alpha1"
// https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-annotation
libraryDependencies += "io.dropwizard.metrics" % "metrics-annotation" % "4.2.30"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "4.2.30"