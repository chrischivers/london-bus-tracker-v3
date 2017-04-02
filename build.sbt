import org.scalatra.sbt._

val ScalatraVersion = "2.5.0"

ScalatraPlugin.scalatraSettings

name := "london-bus-tracker-v3"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("dwhjames", "maven")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.8"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.16"

libraryDependencies += "net.liftweb" % "lift-json_2.11" % "3.0.1"

libraryDependencies += "com.internetitem" % "logback-elasticsearch-appender" % "1.4"

libraryDependencies += "com.github.dwhjames" %% "aws-wrap" % "0.8.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.10.77"

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest"  % ScalatraVersion %  "test",
  "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.5" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container;compile",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided"
)

enablePlugins(JettyPlugin)

test in assembly := {}