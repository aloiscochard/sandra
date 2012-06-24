//  __              
//  (_  _.._  _|._ _.
//  __)(_|| |(_|| (_|
//                                              
//  (c) 2012, Alois Cochard                     
//                                              
//  http://aloiscochard.github.com/sandra        
//                                              

import sbt._
import Keys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Sonatype.settings ++ Seq(
    organization        := "com.github.aloiscochard",
    version             := "0.1.6-SNAPSHOT",
    scalaVersion        := "2.9.2",
    scalacOptions       := Seq("-unchecked", "-deprecation", "-Ydependent-method-types"),
    crossScalaVersions  := Seq("2.9.1", "2.9.1-1", "2.9.2"),
    resolvers ++= Seq(
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
    )
  )
}

object Dependencies {
  val testDependencies = Seq(libraryDependencies <<= (scalaVersion, libraryDependencies) { (version, dependencies) =>
    val specs2 = version match {
      case "2.9.1" => ("org.specs2" %% "specs2" % "1.9" % "test")
      case "2.9.1-1" => ("org.specs2" %% "specs2" % "1.9" % "test")
      case _ => ("org.specs2" %% "specs2" % "1.11" % "test")
    }
    dependencies :+ specs2
  })
}

object SandraBuild extends Build {
  import Dependencies._
  import BuildSettings._

  lazy val sandra = Project (
    "sandra",
    file ("."),
    settings = buildSettings ++ testDependencies ++ Sonatype.settings ++ Seq(
      parallelExecution in Test := false,
      libraryDependencies ++= Seq(
        "me.prettyprint" % "hector-core" % "1.0-5"
      )
    )
  )
}

object Sonatype extends PublishToSonatype(SandraBuild) {
  def projectUrl    = "https://github.com/aloiscochard/sandra"
  def developerId   = "alois.cochard"
  def developerName = "Alois Cochard"
  def licenseName   = "Apache 2 License"
  def licenseUrl    = "http://www.apache.org/licenses/LICENSE-2.0.html"
}
