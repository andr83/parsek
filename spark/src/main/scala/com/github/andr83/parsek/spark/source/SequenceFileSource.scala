package com.github.andr83.parsek.spark.source

import com.github.andr83.parsek.spark.PathFilter._
import com.github.andr83.parsek.spark.{PathFilter, SparkJob}
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

/**
  * @author andr83
  */
case class SequenceFileSource(
  path: Seq[String],
  filters: Seq[PathFilter],
  minPartitions: Int = 0
) extends Source {

  def this(config: Config) = this(
    path = config.getAnyRef("path") match {
      case list: List[_] => list.map(_.toString)
      case value => List(value.toString)
    },
    filters = config.as[Option[List[Config]]]("filters") map (filters => {
      filters.map(f => PathFilter(f))
    }) getOrElse Seq.empty[PathFilter],
    minPartitions = config.as[Option[Int]]("minPartitions") getOrElse 0
  )

  // Default filter to exclude all files with underscore prefix like _SUCCESS
  val defaultHadoopFilter = (p: Path) => !p.getName.startsWith("_")


  override def apply(job: SparkJob): RDD[PValue] = {
    val files = path.flatMap(job.listFilesOnly(_, defaultHadoopFilter +: filters))
    logger.info("Input files:")
    files.sorted.foreach(f => logger.info(f))
    if (files.isEmpty) {
      job.sc.emptyRDD[PValue]
    } else {
      val partitions = if (minPartitions > 0) minPartitions else job.sc.defaultMinPartitions
      job.sc.sequenceFile(files.mkString(","), classOf[String], classOf[Text], partitions)
        .map(kv=> PString(kv._2.toString))
    }
  }
}
