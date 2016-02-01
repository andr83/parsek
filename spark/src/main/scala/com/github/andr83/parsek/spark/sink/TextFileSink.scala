package com.github.andr83.parsek.spark.sink

import java.util.UUID

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.util.{HadoopUtils, RDDUtils}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Save RDD as a compressed text file
  *
  * @param path path to directory where to save
  * @param codec compression codec class
  * @param serializer factory function which return Serializer instance
  *
  * @author andr83
  */
case class TextFileSink(
  path: String,
  codec: Option[Class[_ <: CompressionCodec]],
  serializer: () => Serializer,
  partitions: Seq[FieldFormatter] = Seq.empty[FieldFormatter],
  fileNamePattern: Option[String] = None
) extends Sink {

  def this(config: Config) = this(
    path = config.getString("path"),
    codec = config.as[Option[String]]("codec") map HadoopUtils.getCodec,
    serializer = config.as[Option[Config]]("serializer")
      .map(serializerConf => () => SerDe(serializerConf))
      .getOrElse(StringSerializer.factory),
    partitions = if (config.hasPath("partitions"))
      FieldFormatter(config.getList("partitions"))
    else Seq.empty[FieldFormatter],
    fileNamePattern = config.as[Option[String]]("fileNamePattern")
  )

  override def sink(rdd: RDD[PValue]): Unit = {
    try {
      if (partitions.nonEmpty) {
        val partitionKeys = partitions.map(_.asField.mkString("."))
        RDDUtils
          .serializeAndPartitionBy(rdd, serializer, partitions)
          .map {
            case (keys, line) =>
              val fileName = fileNamePattern map (pattern => partitionKeys.foldLeft((0, pattern)) {
                case ((idx, p), partitionKey) => (idx + 1, p.replaceAllLiterally("${" + partitionKey + "}", keys(idx)))
              }._2) getOrElse keys.mkString("_")
              fileName -> line
          }
          .saveAsHadoopFile(
            path,
            classOf[String],
            classOf[String],
            classOf[TextFileSink.RDDMultipleTextOutputFormat],
            codec = codec
          )
      } else {
        val outRdd = RDDUtils.serialize(rdd, serializer)
        if (fileNamePattern.nonEmpty) {
          outRdd.mapPartitionsWithIndex {
            case (idx, it) =>
              val fileName = fileNamePattern map (pattern=> {
                pattern
                  .replaceAll("{randomUUID}", UUID.randomUUID().toString)
                  .replaceAll("{partitionIndex}", idx.toString)
              }) getOrElse "part-r-" + idx
              it.map(line=> fileName -> line)
          }
          .saveAsHadoopFile(
            path,
            classOf[String],
            classOf[String],
            classOf[TextFileSink.RDDMultipleTextOutputFormat],
            codec = codec
          )
        } else {
          outRdd.saveAsTextFile(path)
        }
      }
    } catch {
      case e: Exception =>
        logger.error(e.toString, e)
    }
  }
}

object TextFileSink {

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      key.asInstanceOf[String]
    }
  }

}