package com.github.andr83.parsek.spark.streaming.source

import java.net.URI

import com.github.andr83.parsek.spark.streaming.StreamingJob
import com.github.andr83.parsek.spark.streaming.source.KafkaSource.ZookeeperConfig
import com.github.andr83.parsek.spark.util.HadoopUtils
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import net.ceedubs.ficus.Ficus._
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{PublicKafkaCluster, _}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.Try

/**
  * @author andr83
  */
case class KafkaSource(
  brokers: String,
  topics: Set[String],
  offsetsDir: Option[String] = None,
  zookeeperConfig: Option[ZookeeperConfig] = None,
  reset: Option[String] = None

) extends StreamingSource {

  import KafkaSource._

  def this(config: Config) = this(
    brokers = config.as[String]("brokers"),
    topics = config.as[Set[String]]("topics"),
    offsetsDir = config.as[Option[String]]("offsetsDir"),
    zookeeperConfig = config.as[Option[Config]]("zookeeper").map(c => ZookeeperConfig(connect = c.getString("connect"), groupId = c.getString("groupId"))),
    reset = config.as[Option[String]]("reset")
  )

  def apply(job: StreamingJob): DStream[PValue] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers) ++
      reset.filter(Seq("smallest", "largest").contains).map(v => Map("auto.offset.reset" -> v)).getOrElse(Map.empty[String, String])

    val kc = new PublicKafkaCluster(kafkaParams)
    val partitions = kc.getPartitions(topics).fold(
      kafkaErrorHandler(),
      res => res
    )

    val partitionsFromOffsetOpt: Option[Map[TopicAndPartition, Long]] = reset.flatMap(r => Try(r.toLong).toOption).map(ts => {
      kc.getLeaderOffsets(partitions, ts).fold(
        kafkaErrorHandler(),
        res => res.mapValues(o => o.offset)
      )
    })

    val leaderPartitions: Map[TopicAndPartition, Long] =
      kc.getLatestLeaderOffsets(partitions).fold(kafkaErrorHandler(), res => res).mapValues(o => o.offset)

    val topicAndPartitions: Option[Map[TopicAndPartition, Long]] = zookeeperConfig.map(zc => {
      // Zookeeper offset commiter
      val zkClient = new ZkClient(zc.connect)
      job.ssc.addStreamingListener(new KafkaZookeeperOffsetCommiterListener(job.ssc, zkClient, zc))

      topics.flatMap(topic => {
        val topicDirs = new ZKGroupTopicDirs(zc.groupId, topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}"
        if (zkClient.exists(zkPath)) {
          zkClient.getChildren(zkPath).asScala.map(partitionPath => {
            val zkPartPath = s"$zkPath/$partitionPath"
            val offset = zkClient.readData[String](zkPartPath).toLong
            val partition = partitionPath.split("/").last.toInt
            TopicAndPartition(topic, partition) -> offset
          })
        } else {
          Seq.empty
        }
      }).toMap
    }).orElse(offsetsDir.map(dir => {
      // Directory offset commiter
      val fs = FileSystem.get(URI.create(dir), job.hadoopConfig)
      job.ssc.addStreamingListener(new KafkaDirOffsetCommiterListener(job.ssc, fs, dir))

      if (!fs.exists(new Path(dir))) {
        fs.mkdirs(new Path(dir))
      }

      def readOffsets(job: StreamingJob, dir: String): Map[TopicAndPartition, Long] = {
        leaderPartitions ++
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
      partitionsFromOffsetOpt.getOrElse(leaderPartitions ++ readOffsets(job, dir))
    }))

    val ds: InputDStream[(String, String)] = if (reset == Some("smallest")) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        job.ssc, kafkaParams, topics)
    } else {
      val fromOffset = leaderPartitions ++ topicAndPartitions.getOrElse(leaderPartitions)
      logger.info(s"Started kafka stream from offsets: $fromOffset")

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        job.ssc, kafkaParams, fromOffset, messageHandler)
    }
    ds.map { case (k, v) => PString(v) }
  }

  def kafkaErrorHandler[T](): (ArrayBuffer[Throwable] => T) = errors => {
    errors.foreach(err => {
      logger.error(err.getMessage, err)
    })
    throw errors.head
  }
}

object KafkaSource {

  val messageHandler: (MessageAndMetadata[String, String] => (String, String)) = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

  class KafkaDirOffsetCommiterListener(ssc: StreamingContext, fs: FileSystem, offsetsDir: String) extends StreamingListener with LazyLogging {

    private[this] var isFailed = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      if (isFailed) {
        return
      }

      val bi = batchCompleted.batchInfo
      val offsets = bi.streamIdToInputInfo.values.flatMap(info => {
        info.metadata.get("offsets").map(x => {
          x.asInstanceOf[List[OffsetRange]]
        }) getOrElse List.empty[OffsetRange]
      })

      val failures = bi.outputOperationInfos.values.flatMap(info => {
        info.failureReason
      })

      if (failures.nonEmpty) {
        isFailed = true
        logger.info(s"Batch ${bi.batchTime} completed in ${bi.processingDelay.getOrElse(0)} ms with failure. Offsets: $offsets")
        Future {
          if (ssc.getState() != StreamingContextState.STOPPED) {
            ssc.stop(stopSparkContext = true, stopGracefully = false)
          }
        }
        return
      }

      logger.info(s"Batch ${bi.batchTime} completed in ${bi.processingDelay.getOrElse(0)} ms processed ${bi.numRecords} records. Offsets: $offsets")

      offsets.foreach(o => {
        val offsetsFile = offsetsDir + s"/${o.topic}-${o.partition}"
        val line = Seq(o.topic, o.partition, o.untilOffset).mkString(",")
        HadoopUtils.writeString(line, offsetsFile, overwrite = true, fs) match {
          case Left(errors) => errors foreach (e => logger.error(e.getMessage, e))
          case _ =>
        }
      })
    }
  }

  case class ZookeeperConfig(connect: String, groupId: String)

  class KafkaZookeeperOffsetCommiterListener(ssc: StreamingContext, zkClient: ZkClient, zookeeperConfig: ZookeeperConfig) extends StreamingListener with LazyLogging {
    private[this] var isFailed = false
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
      if (isFailed) {
        return
      }

      val bi = batchCompleted.batchInfo
      val offsets = bi.streamIdToInputInfo.values.flatMap(info => {
        info.metadata.get("offsets").map(x => {
          x.asInstanceOf[List[OffsetRange]]
        }) getOrElse List.empty[OffsetRange]
      })

      val failures = bi.outputOperationInfos.values.flatMap(info => {
        info.failureReason
      })

      if (failures.nonEmpty) {
        isFailed = true
        logger.info(s"Batch ${bi.batchTime} completed in ${bi.processingDelay.getOrElse(0)} ms with failure. Offsets: $offsets")
        Future {
          if (ssc.getState() != StreamingContextState.STOPPED) {
            ssc.stop(stopSparkContext = true, stopGracefully = false)
          }
        }
        return
      }

      logger.info(s"Batch ${bi.batchTime} completed in ${bi.processingDelay.getOrElse(0)} ms processed ${bi.numRecords} records. Offsets: $offsets")

      offsets.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(zookeeperConfig.groupId, o.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"

        zkUtils.updatePersistentPath(zkPath, o.untilOffset.toString)
      })
    }
  }


}
