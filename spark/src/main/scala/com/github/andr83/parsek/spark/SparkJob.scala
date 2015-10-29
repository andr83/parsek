package com.github.andr83.parsek.spark

import java.net.URI
import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.{OptionDef, Read}

/**
 * @author andr83
 */
abstract class SparkJob {
  type PathPredicate = Path => Boolean

  implicit def toPath(path: String): Path = new Path(path)
  implicit def toFile(path: String): File = new File(path)

  lazy val optionParser = new scopt.OptionParser[Unit](getClass.getSimpleName) {
    override def showUsageOnError = true
  }

  def opt[A: Read](name: String): OptionDef[A, Unit] = optionParser.opt[A](name)

  def opt[A: Read](x: Char, name: String): OptionDef[A, Unit] = optionParser.opt[A](name) abbr x.toString

  def parseOptions(args: Array[String]): Boolean = optionParser.parse(args)

  lazy val sparkConfig = {
    new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster(sparkMaster)
      .set("spark.executor.memory", sparkMemory)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  lazy val sc = new SparkContext(sparkConfig)

  lazy val fs = {
    val conf = new Configuration
    conf.set("fs.hdfs.impl",
      "org.apache.hadoop.hdfs.DistributedFileSystem"
    )
    conf.set("fs.file.impl",
      "org.apache.hadoop.fs.LocalFileSystem"
    )
    if (hdfsUri.isEmpty) {
      FileSystem.get(conf)
    } else {
      FileSystem.get(new URI(hdfsUri), conf)
    }
  }

  var sparkMemory = "1G"
  var sparkMaster = "local[2]"
  var sparkLogLevel = Level.WARN
  var hdfsUri = ""

  opt[String]("sparkMemory") foreach {
    sparkMemory = _
  } text "spark.executor.memory value, default 1G "

  opt[String]("sparkMaster") foreach { value =>
    sparkLogLevel = Level.toLevel(value)
  } text "Spark master host, default \"local\" "

  opt[String]("sparkLogLevel") foreach {
    hdfsUri = _
  } text "Spark logger output level. Default WARN"

  opt[String]("hadoopUser") foreach {
    System.setProperty("HADOOP_USER_NAME", _)
  } text "Set HADOOP_USER_NAME enviroment variable"

  opt[String]("hdfsUri") foreach {
    hdfsUri = _
  } text "Hdfs file system uri"

  /**
   * List recursively all files from path
   * @param path folder to search files
   * @return
   */
  def listFilesOnly(path: String, filter: PathPredicate = p => !p.getName.startsWith("_")): Iterable[String] =
    path split "," flatMap {
      case p => listFilesOnly(new Path(p), filter)
    }

  /**
   * List recursively all files from path
   * @param path folder to search files
   * @return
   */
  def listFilesOnly(path: Path, filter: PathPredicate): Iterable[String] = {
    val it = fs.listFiles(path, true)
    var files = List.empty[String]
    while (it.hasNext) {
      val status = it.next()
      if (status.isFile && filter(status.getPath)) {
        files = status.getPath.toString :: files
      }
    }
    files
  }

  def run() = {
    beforeJob()
    job()
    afterJob()
  }

  def beforeJob() = {}

  def afterJob() = {}

  def job()

  def main(args: Array[String]) {
    if (!parseOptions(args)) {
      sys.exit(1)
    }

    Logger.getLogger("org").setLevel(sparkLogLevel)
    Logger.getLogger("akka").setLevel(sparkLogLevel)

    run()
  }
}
