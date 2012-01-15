name := "sandra"

version := "0.1-SNAPSHOT"

organization := "com.github.aloiscochard"

scalaVersion := "2.9.1"

scalacOptions += "-unchecked"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "me.prettyprint" % "hector-core" % "1.0-2",
  "org.specs2" %% "specs2" % "1.7.1" % "test"
)

resolvers += "Maven Central Repository" at "http://repo2.maven.org/maven2/"
