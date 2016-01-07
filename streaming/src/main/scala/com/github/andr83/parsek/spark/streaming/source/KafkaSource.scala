package com.github.andr83.parsek.spark.streaming.source

import com.github.andr83.parsek.spark.streaming.{StreamingJob, StreamingSource}
import com.github.andr83.parsek.{PString, PValue}
import com.typesafe.config.Config
import kafka.serializer.StringDecoder
import net.ceedubs.ficus.Ficus._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @author andr83
  */
case class KafkaSource(brokers: String, topics: Set[String]) extends StreamingSource {
  def this(config: Config) = this(
    brokers = config.as[String]("brokers"),
    topics = config.as[Set[String]]("topics")
  )

  def apply(job: StreamingJob): DStream[PValue] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](job.ssc, kafkaParams, topics)
    ds.map{case (k, v) => PString(v)}
  }

}
