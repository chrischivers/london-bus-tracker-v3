import org.scalatra.sbt._

val ScalatraVersion = "2.5.0"

ScalatraPlugin.scalatraSettings

name := "london-bus-tracker-v3"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.8"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.github.sstone" % "amqp-client_2.11" % "1.5"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.16"

libraryDependencies += "net.liftweb" % "lift-json_2.11" % "3.0.1"

libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest"  % ScalatraVersion %  "test",
  "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.5" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided"
)

enablePlugins(JettyPlugin)