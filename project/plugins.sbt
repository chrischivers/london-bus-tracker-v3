logLevel := Level.Warn

resolvers += "JBoss" at "https://repository.jboss.org"

addSbtPlugin("org.scalatra.scalate" % "sbt-scalate-precompiler" % "1.8.0.1")

addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")