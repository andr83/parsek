import sbt.Keys._
import sbt._

val commonsCodecVersion = "1.1"
val commonsLangVersion = "2.6"
val guavaVersion = "14.0"
val hadoopVersion = sys.props.getOrElse("hadoopVersion", default = "2.6.+")
val javaxServletVersion = "3.0.1"
val jacksonVersion = "2.4.4"
val json4SVersion = "3.2.10"
val ficusVersion = "1.0.1"
val openCsvVersion = "3.4"
val scalaArmVersion = "1.4"
val scalaLoggingVersion = "2.1.2"
val scalaTestVersion = "2.2.+"
val scalaTimeVersion = "1.8.+"
val scoptVersion = "3.3.+"
val slf4jVersion = "1.7.5"
val snappyJavaVersion = "1.1.2"
val sparkVersion = sys.props.getOrElse("sparkVersion", default = "1.5.2")
val typesafeConfigVersion = "1.2.+"
val twitterUtilVersion = "6.27.0"

lazy val commonSettings = Seq(
  organization := "com.github.andr83",
  version := "0.1.0",
  scalaVersion := "2.10.4",
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
  resolvers += Resolver.sonatypeRepo("releases"),
  externalResolvers := Seq(
    "Maven Central Server" at "http://repo1.maven.org/maven2",
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"
  )
)

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
    case PathList("javax", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case PathList("org", "eclipse", "jetty", "orbit", xs@_*) => MergeStrategy.last
    case PathList("com", "google", xs@_*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case "plugin.properties" => MergeStrategy.discard
    case "about.html" => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.last
  },
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> "shade.com.@1").inAll
  )
)

val jacksonExclusion = Seq(
  ExclusionRule("com.fasterxml.jackson.core"),
  ExclusionRule("com.fasterxml.jackson.databind"),
  ExclusionRule("com.fasterxml.jackson.module")
)

val guavaExclusion = ExclusionRule("com.google.guava", "guava")
val sparkExclusions = Seq(
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
  .aggregate(core, spark, sql, streaming, assemblyProject)

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-core",
    dependencyOverrides ++= Set(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value
    ),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-simple" % slf4jVersion % "test",
      "com.typesafe" % "config" % typesafeConfigVersion,
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % scalaLoggingVersion,
      "org.json4s" %% "json4s-native" % json4SVersion,
      "org.json4s" %% "json4s-jackson" % json4SVersion
        excludeAll(jacksonExclusion: _*),
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.opencsv" % "opencsv" % openCsvVersion,
      "com.jsuereth" %% "scala-arm" % scalaArmVersion,
      "com.github.nscala-time" %% "nscala-time" % scalaTimeVersion,
      "javax.servlet" % "javax.servlet-api" % javaxServletVersion,
      "org.xerial.snappy" % "snappy-java" % snappyJavaVersion,
      "net.ceedubs" %% "ficus" % ficusVersion,
      "commons-codec" % "commons-codec" % commonsCodecVersion,
      "commons-lang" % "commons-lang" % commonsLangVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )// ++ hadoopDependencies
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val spark = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-spark",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "com.twitter" %% "util-eval" % twitterUtilVersion
    ) ++ hadoopDependencies
  )
  .dependsOn(core)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val sql = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-sql",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "org.apache.spark" %% "spark-hive" % sparkVersion
    )
  )
  .dependsOn(spark)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val streaming = project
  .settings(commonSettings: _*)
  .settings(
    name := "parsek-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
        excludeAll (sparkExclusions: _*),
      "org.apache.spark" %% "spark-streaming-flume" % sparkVersion
        excludeAll (sparkExclusions: _*)
    )
  )
  .dependsOn(spark)
  .disablePlugins(sbtassembly.AssemblyPlugin)

val JarConfig = config("jar") extend Compile

lazy val assemblyProject = project
  .in(file("assembly"))
  .configs(JarConfig)
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)
  .settings(
    name := "parsek-assembly",
    assemblyJarName in assembly := s"parsek-assembly-${version.value}.jar"
  )
  .settings(inConfig(JarConfig)
  (Classpaths.configSettings ++ Defaults.configTasks ++ baseAssemblySettings ++ commonSettings ++ assemblySettings ++ Seq(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false),
    assemblyJarName in assembly := s"parsek-${version.value}.jar"
  )): _*)
  .dependsOn(core, spark, sql, streaming)

lazy val assemblyClusterProject = project
  .in(file("assembly-cluster"))
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)
  .settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"parsek-cluster-${version.value}.jar"
  )
  .settings(
    projectDependencies := {
      Seq(
        (projectID in core).value.excludeAll(
          ExclusionRule("org.apache.hadoop"),
          ExclusionRule("org.xerial.snappy")
        ),
        (projectID in spark).value.excludeAll(ExclusionRule("org.apache.spark")),
        (projectID in sql).value.excludeAll(ExclusionRule("org.apache.spark")),
        (projectID in streaming).value.excludeAll(ExclusionRule("org.apache.spark"))
      )
    }
  )
  .dependsOn(core, spark, sql, streaming)
