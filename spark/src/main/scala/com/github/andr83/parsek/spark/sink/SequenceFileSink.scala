package com.github.andr83.parsek.spark.sink

import java.util.UUID

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.util.RDDUtils.{DefaultPartitioner, FieldsPartitioner}
import com.github.andr83.parsek.spark.util.{HadoopUtils, RDDUtils}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * @author andr83
  */
case class SequenceFileSink(
  path: String,
  codec: Option[Class[_ <: CompressionCodec]],
  serializer: () => Serializer,
  partitions: Seq[FieldFormatter] = Seq.empty[FieldFormatter],
  numPartitions: Option[Int] = None,
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
    numPartitions = config.as[Option[Int]]("numPartitions"),
    fileNamePattern = config.as[Option[String]]("fileNamePattern")
  )

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    try {
      if (partitions.nonEmpty || fileNamePattern.nonEmpty) {
        val pattern = fileNamePattern map (pattern => {
          pattern
            .replaceAllLiterally("${randomUUID}", UUID.randomUUID().toString)
            .replaceAllLiterally("${timeInMs}", time.toString)
        })

        val partitioner = if (partitions.isEmpty) DefaultPartitioner(pattern)
        else FieldsPartitioner(partitions, pattern)

        RDDUtils
          .serializeAndPartitionBy(rdd, serializer, partitioner, numPartitions)
          .mapValues(new Text(_))
          .saveAsHadoopFile(
            path,
            classOf[NullWritable],
            classOf[Text],
            classOf[SequenceFileSink.RDDMultipleSequenceOutputFormat],
            codec = codec
          )
      } else {
        RDDUtils
          .serialize(rdd, serializer)
          .map(v=> NullWritable.get() -> v)
          .saveAsSequenceFile(path, codec)
      }
    } catch {
      case e: Exception =>
        logger.error(e.toString, e)
    }
  }
}


object SequenceFileSink {

  class RDDMultipleSequenceOutputFormat extends MultipleSequenceFileOutputFormat[Any, Any] {

    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      key.asInstanceOf[String]
    }
  }

}
