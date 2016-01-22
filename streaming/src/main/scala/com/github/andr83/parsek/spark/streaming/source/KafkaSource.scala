package com.github.andr83.parsek.spark.streaming.source

import java.nio.file.{Files, Paths}

import com.github.andr83.parsek.spark.streaming.StreamingJob
import com.github.andr83.parsek.util.FileUtils
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import scala.io.Source
import scala.util.Failure

/**
  * @author andr83
  */
case class KafkaSource(
  brokers: String,
  topics: Set[String],
  offsetsFile: Option[String] = None,
  reset: Option[String]
) extends StreamingSource {
  def this(config: Config) = this(
    brokers = config.as[String]("brokers"),
    topics = config.as[Set[String]]("topics"),
    offsetsFile = config.as[Option[String]]("offsetsFile"),
    reset = config.as[Option[String]]("reset")
  )

  def apply(job: StreamingJob): DStream[PValue] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers) ++
      reset.map(v=> Map("auto.offset.reset" -> v)).getOrElse(Map.empty[String, String])
    offsetsFile match {
      case Some(file) =>
        var topicsOffset = topics.map(_ ->(0, 0L)).toMap
        if (Files.exists(Paths.get(file))) {
          for (line <- Source.fromFile(file).getLines()) {
            val Array(topic: String, partition: String, offset: String) = line.split(",")
            topicsOffset += topic ->(partition.toInt, offset.toLong)
          }
        }

        val fromOffset = topicsOffset.map {
          case (topic, (partition, offset)) => TopicAndPartition(topic, partition) -> offset
        }

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

        val ds = if (reset.isEmpty) {
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            job.ssc, kafkaParams, fromOffset, messageHandler)
        } else {
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](job.ssc, kafkaParams, topics)
        }

        ds.transform { rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // Storing current offsets to file if it was set
          offsetsFile foreach { file =>
            FileUtils.writeLines(file, offsetRanges.map(o => {
              Seq(o.topic, o.partition, o.untilOffset).mkString(",")
            }), append = false) match {
              case Failure(e) => logger.error(e.getMessage, e)
              case _ =>
            }
          }
          rdd.map{case (k, v) => PString(v)}
        }
      case None =>
        val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](job.ssc, kafkaParams, topics)
        ds.map{case (k, v) => PString(v)}
    }
  }

}
