import sbt.Keys._
import sbt._

val guavaVersion = "14.0"
val hadoopVersion = "2.3.+"
val jacksonCoreVersion = "2.4.4"
val jacksonVersion = "2.6.1"
val json4SVersion = "3.3.+"
val openCsvVersion = "3.4"
val playJsonVersion = "2.4.3"
val scalaArmVersion = "1.4"
val scalaLoggingVersion = "2.1.2"
val scalaTestVersion = "2.2.+"
val scalaTimeVersion = "1.8.+"
val scoptVersion = "3.3.+"
val slf4jVersion = "1.7.5"
val sparkVersion = "1.3.+"
val typesafeConfigVersion = "1.2.+"

lazy val commonSettings = Seq(
  organization := "com.github.andr83",
  version := "0.1.0",
  scalaVersion := "2.10.5",
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
  resolvers += Resolver.sonatypeRepo("releases"),
  externalResolvers := Seq(
    "Maven Central Server" at "http://repo1.maven.org/maven2",
    "Sonatype OSS Releases"  at "http://oss.sonatype.org/content/repositories/releases/"
  ),
  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
    case PathList("javax", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case PathList("org", "eclipse", "jetty", "orbit", xs@_*) => MergeStrategy.last
    case PathList("com", "google", xs@_*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
    case PathList("META-INF", xs@_*) => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.discard
    case "about.html" => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.last
  },
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> "shade.com.@1").inAll
  )
)

val jacksonCoreExclusion = ExclusionRule("com.fasterxml.jackson.core")
val guavaExclusion = ExclusionRule("com.google.guava", "guava")
val sparkExclusions = Seq(
  jacksonCoreExclusion,
  guavaExclusion,
  ExclusionRule("org.apache.hadoop"),
  ExclusionRule("org.apache.hbase"),
  ExclusionRule("javax.servlet"),
  ExclusionRule("org.eclipse.jetty.orbit", "servlet-api"),
  ExclusionRule("org.mortbay.jetty", "servlet-api"),
  ExclusionRule("commons-beanutils", "commons-beanutils-core"),
  ExclusionRule("commons-collections", "commons-collections"),
  ExclusionRule("com.esotericsoftware.minlog", "minlog")
)
val hadoopExclusion = Seq(
  guavaExclusion
)

val hadoopDependencies = Seq(
  "com.google.guava" % "guava" % guavaVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion
    excludeAll (hadoopExclusion: _*),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
    excludeAll (hadoopExclusion: _*)
)

lazy val parsek = project.in(file("."))
  .aggregate(core, spark)

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-core",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test",
      "com.typesafe" % "config" % typesafeConfigVersion,
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % scalaLoggingVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "org.json4s" %% "json4s-jackson" % json4SVersion,
      "com.opencsv" % "opencsv" % openCsvVersion,
      "com.jsuereth" %% "scala-arm" % scalaArmVersion,
      "com.github.nscala-time" %% "nscala-time" % scalaTimeVersion,
      "javax.servlet" % "javax.servlet-api" % "3.0.1",
      "org.xerial.snappy" % "snappy-java" % "1.1.2",
      "net.ceedubs" %% "ficus" % "1.0.1",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    ) ++ hadoopDependencies
  )

lazy val spark = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-spark",
    libraryDependencies ++= hadoopDependencies ++ Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "org.apache.spark" %% "spark-streaming" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
        excludeAll (sparkExclusions: _*)
    )
  ).dependsOn(core)
