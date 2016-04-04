package com.github.andr83.parsek.spark

import java.io.File

import com.github.andr83.parsek.mapToConfig
import com.github.andr83.parsek.resources.ResourceFactory
import com.github.andr83.parsek.spark.PathFilter.PathFilter
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import resource._
import scopt.{OptionDef, Read}

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
  * Base abstract class for Spark jobs.
  *
  * @author andr83
  */
abstract class SparkJob extends LazyLogging {
  implicit def toPath(path: String): Path = new Path(path)

  implicit def toFile(path: String): File = new File(path)

  lazy val optionParser = new scopt.OptionParser[Unit](getClass.getSimpleName) {
    override def showUsageOnError = true
  }

  def opt[A: Read](name: String): OptionDef[A, Unit] = optionParser.opt[A](name)

  def opt[A: Read](x: Char, name: String): OptionDef[A, Unit] = optionParser.opt[A](name) abbr x.toString

  def parseOptions(args: Array[String]): Boolean = optionParser.parse(args)

  def newSparkConfig() = {
    val sc = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster(sparkMaster)
      .set("spark.executor.memory", sparkMemory)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkCores foreach (cores=> sc.set("spark.cores.max", cores.toString))
    sc
  }

  lazy val sparkConfig = newSparkConfig()

  lazy val sc = {
    val sc = new SparkContext(sparkConfig)
    sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    sc.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    sc
  }

  lazy val fs = FileSystem.get(hadoopConfig)

  lazy val hadoopConfig = {
    val conf = new Configuration
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("spark.yarn.appMasterEnv.HADOOP_USER_NAME", hadoopUser.getOrElse(System.getProperty("user.name")))

    if (hadoopConfigDirectory.nonEmpty) {
      conf.addResource(new Path(hadoopConfigDirectory + "/core-site.xml"))
      conf.addResource(new Path(hadoopConfigDirectory + "/hdfs-site.xml"))
    }
    conf
  }

  lazy val resourceFactory = ResourceFactory()

  var sparkMemory = "1G"
  var sparkCores: Option[Int] = None
  var sparkMaster = "local[*]"
  var sparkLogLevel = Level.WARN
  var hadoopUser: Option[String] = None
  var hadoopConfigDirectory = ""
  var params = Map.empty[String, String]

  @volatile private var _config = ConfigFactory.empty()
  def config = _config

  opt[String]("sparkMemory") foreach {
    sparkMemory = _
  } text "spark.executor.memory value, default 1G "

  opt[Int]("sparkCores") foreach {cores=>
    sparkCores = Some(cores)
  } text "spark.cores.max value, default is unlimited"

  opt[String]("sparkMaster") foreach { value =>
    sparkMaster = value
  } text "Spark master host, default \"local\" "

  opt[String]("sparkLogLevel") foreach { value =>
    sparkLogLevel = Level.toLevel(value)
  } text "Spark logger output level. Default WARN"

  opt[String]("hadoopUser") foreach {user=>
    hadoopUser = Some(user)
    System.setProperty("HADOOP_USER_NAME", user)
  } text "Set HADOOP_USER_NAME enviroment variable"

  opt[String]("hadoopConfigDirectory") foreach {
    hadoopConfigDirectory = _
  } text "Path to hadoop config directory with core-site.xml and hdfs-site.xml files"

  opt[String]('c', "config") required() foreach { path =>
    _config = if (path.startsWith("hdfs://")) {
      (for (
        in <- managed(fs.open(path))
      ) yield IOUtils.toString(in)).either match {
        case Right(content) => ConfigFactory.parseString(content)
        case Left(errors) => throw errors.head
      }
    } else {
      ConfigFactory.parseFile(path)
    }
  } text "Configuration file path. Support local and hdfs"

  opt[Map[String, String]]("params") foreach {
    params = _
  } text "Configuration file params in format param1=val1,param2=val2"

  /**
    * List recursively all files from path
    * @param path folder to search files
    * @return
    */
  def listFilesOnly(path: String, filters: Seq[PathFilter]): Iterable[String] =
    path split "," flatMap {
      case p => listFilesOnly(new Path(p), filters)
    }

  /**
    * List recursively all files from path
    * @param path folder to search files
    * @return
    */
  def listFilesOnly(path: Path, filters: Seq[PathFilter]): Iterable[String] = {
    val it = fs.listFiles(path, true)
    var files = List.empty[String]
    while (it.hasNext) {
      val status = it.next()
      if (status.isFile && filters.forall(f => f(status.getPath))) {
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

  def beforeJob(): Unit = {
    config.as[Option[Config]]("resources") foreach (resources => {
      val res = resources.root().unwrapped().keySet() map (key => {
        key -> resourceFactory.read(resources.getValue(key)).value
      })
      if (res.nonEmpty) {
        val newConfig = ConfigFactory.parseMap(mapAsJavaMap(Map("resources" -> mapAsJavaMap(res.toMap))))
        _config = newConfig.withFallback(config)
      }
    })
    _config = _config.withFallback(params)
    _config = _config.resolve()
  }

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
