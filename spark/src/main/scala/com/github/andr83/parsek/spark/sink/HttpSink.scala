package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek.PValue
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.util.RDDUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.http.HttpEntity
import org.apache.http.client.entity.GzipCompressingEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.rdd.RDD

/**
  * @author andr83
  */
case class HttpSink(
  url: String,
  serializer: () => Serializer,
  codec: Option[String] = None
) extends Sink {
  def this(config: Config) = this(
    url = config.as[String]("url"),
    codec = config.as[Option[String]]("codec"),
    serializer = config.as[Option[Config]]("serializer")
      .map(serializerConf => () => SerDe(serializerConf))
      .getOrElse(StringSerializer.factory)
  )

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    val serializedRdd = RDDUtils.serialize(rdd, serializer)
    serializedRdd.foreachPartition(it => {
      if (it.nonEmpty) {
        val post = new HttpPost(url)
        val bytes = it.mkString("\n").getBytes("UTF-8")
        var entity: HttpEntity = new ByteArrayEntity(bytes)
        codec match {
          case Some("gzip") =>
            entity = new GzipCompressingEntity(entity)
          case Some(other) =>
            logger.error(s"Codec $other does not support.")
          case None =>
        }
        post.setEntity(entity)
        val client = HttpClientBuilder.create.build
        logger.info(s"Sending ${bytes.size} bytes to $url")
        val response = client.execute(post)
        logger.info(s"Response code: ${response.getStatusLine.getStatusCode}")
        logger.info(scala.io.Source.fromInputStream(response.getEntity.getContent).mkString)
      }
    })
  }
}
