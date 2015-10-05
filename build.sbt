import sbt.Keys._
import sbt._

val guavaVersion             = "14.0"
val hadoopVersion            = "2.3.+"
val jacksonCoreVersion       = "2.4.4"
val jacksonVersion           = "2.6.1"
val openCsvVersion           = "2.3"
val playJsonVersion          = "2.4.3"
val scalaLoggingVersion      = "2.1.2"
val scalaTestVersion         = "2.2.+"
val scalaTimeVersion         = "1.8.+"
val scoptVersion             = "3.3.+"
val slf4jVersion             = "1.7.+"
val sparkVersion             = "1.3.+"
val typesafeConfigVersion    = "1.2.1"

lazy val commonSettings = Seq(
  organization := "com.github.andr83",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
  resolvers += Resolver.sonatypeRepo("releases"),
  externalResolvers := Seq(
    "Maven Central Server" at "http://repo1.maven.org/maven2"
  )
)

lazy val parsek = project.in(file("."))
  .aggregate(core)

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    name:="parsek-core",
    libraryDependencies ++= Seq(
      "org.slf4j"                       %  "slf4j-api"            % slf4jVersion,
      "org.slf4j"                       %  "slf4j-simple"         % slf4jVersion % "test",
      "com.typesafe.scala-logging"      %% "scala-logging-slf4j"  % scalaLoggingVersion,
      "org.json4s"                      %% "json4s-native"        % "3.3.+",
      "org.json4s"                      %% "json4s-jackson"       % "3.3.+",
      "com.github.nscala-time"          %% "nscala-time"          % scalaTimeVersion,
      "org.scalatest"                   %% "scalatest"            % scalaTestVersion % "test"
    )
  )