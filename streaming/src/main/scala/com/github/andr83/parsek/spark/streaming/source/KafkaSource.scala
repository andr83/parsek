package com.github.andr83.parsek.spark.streaming.source

import java.net.URI

import com.github.andr83.parsek.spark.streaming.StreamingJob
import com.github.andr83.parsek.spark.streaming.source.KafkaSource.KafkaListener
import com.github.andr83.parsek.spark.util.HadoopUtils
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
  * @author andr83
  */
case class KafkaSource(
  brokers: String,
  topics: Set[String],
  offsetsDir: Option[String] = None,
  reset: Option[String]
) extends StreamingSource {
  def this(config: Config) = this(
    brokers = config.as[String]("brokers"),
    topics = config.as[Set[String]]("topics"),
    offsetsDir = config.as[Option[String]]("offsetsDir"),
    reset = config.as[Option[String]]("reset")
  )

  def apply(job: StreamingJob): DStream[PValue] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers) ++
      reset.map(v => Map("auto.offset.reset" -> v)).getOrElse(Map.empty[String, String])

    offsetsDir match {
      case Some(dir) =>
        val fs = FileSystem.get(URI.create(dir), job.hadoopConfig)
        job.ssc.addStreamingListener(new KafkaListener(fs, dir))

        if (!fs.exists(new Path(dir))) {
          fs.mkdirs(new Path(dir))
        }

        def readOffsets(job: StreamingJob, dir: String): Map[TopicAndPartition, Long] = {
          job.listFilesOnly(dir, Seq.empty)
            .map(file => {
              val line = HadoopUtils.readString(file, fs)
              val Array(topic: String, partition: String, offset: String) = line.split(",")
              TopicAndPartition(topic, partition.toInt) -> offset.toLong
            })
            .groupBy(_._1)
            .mapValues(it => {
              it.minBy(_._2)._2
            })
            .filterKeys(tp => topics.contains(tp.topic))
        }

        val ds: InputDStream[(String, String)] = if (reset == Some("smallest")) {
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            job.ssc, kafkaParams, topics)
        } else {
          val kc = new PublicKafkaCluster(kafkaParams)

          val result = for {
            topicPartitions <- kc.getPartitions(topics).right
            leaderOffsets <- kc.getLatestLeaderOffsets(topicPartitions).right
          } yield leaderOffsets

          result match {
            //on Kafka getting offsets failed fallback to read string from now
            case Left(errs) =>
              logger.error(errs mkString "\n")
              KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](job.ssc, kafkaParams, topics)

            case Right(topicOffsets) =>
              val fromOffset = topicOffsets.mapValues (_.offset) ++ readOffsets (job, dir)
              logger.info(s"Started kafka stream from offsets: $fromOffset")

              val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
              KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
                job.ssc, kafkaParams, fromOffset, messageHandler)
          }
        }
        ds.map { case (k, v) => PString(v) }
      case None =>
        val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](job.ssc, kafkaParams, topics)
        ds.map { case (k, v) => PString(v) }
    }
  }
}

object KafkaSource {

  class KafkaListener(fs: FileSystem, offsetsDir: String) extends StreamingListener with Logging {

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      val bi = batchCompleted.batchInfo
      val offsets = bi.streamIdToInputInfo.values.flatMap(info => {
        info.metadata.get("offsets").map(x => {
          x.asInstanceOf[List[OffsetRange]]
        }) getOrElse List.empty[OffsetRange]
      })

      log.info(s"Batch completed in ${bi.processingDelay}ms processed ${bi.numRecords} records. Offsets: $offsets")

      offsets.foreach(o => {
        val offsetsFile = offsetsDir + s"/${o.topic}-${o.partition}"
        val line = Seq(o.topic, o.partition, o.untilOffset).mkString(",")
        HadoopUtils.writeString(line, offsetsFile, overwrite = true, fs) match {
          case Left(errors) => errors foreach (e => log.error(e.getMessage, e))
          case _ =>
        }
      })
    }
  }

}
