package com.github.andr83.parsek.spark.source

import com.github.andr83.parsek.spark.PathFilter.PathFilter
import com.github.andr83.parsek.spark.{PathFilter, SparkJob}
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

/**
  * Read a text file from HDFS, a local file system (available on all nodes), or any
  * Hadoop-supported file system URI, and return it as an RDD of Strings.
  *
  * @param path seq of strings paths. Every string can contain multiple path separated by comma.
  *             Path can be a file or directory.
  * @param filters seq of filters to filter paths
  *
  * @author andr83
  */
case class TextFileSource(path: Seq[String], filters: Seq[PathFilter]) extends Source {

  def this(config: Config) = this(
    path = config.getAnyRef("path") match {
      case list: List[_] => list.map(_.toString)
      case value => List(value.toString)
    },
    filters = config.as[Option[List[Config]]]("filters") map (filters => {
      filters.map(f => PathFilter(f))
    }) getOrElse Seq.empty[PathFilter]
  )

  // Default filter to exclude all files with underscore prefix like _SUCCESS
  val defaultHadoopFilter = (p: Path) => !p.getName.startsWith("_")


  override def apply(job: SparkJob): RDD[PValue] = {
    val files = path.flatMap(job.listFilesOnly(_, defaultHadoopFilter +: filters))
    logger.info("Input files:")
    files.foreach(f => logger.info(f))
    if (files.isEmpty) {
      job.sc.emptyRDD[PValue]
    } else {
      job.sc.textFile(files.mkString(",")).map(PString)
    }
  }
}
