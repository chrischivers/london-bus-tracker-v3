name := "london-bus-tracker-v3"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.8"

// available for Scala 2.11.8, 2.12.0
libraryDependencies += "co.fs2" %% "fs2-core" % "0.9.2"

// optional I/O library
libraryDependencies += "co.fs2" %% "fs2-io" % "0.9.2"
