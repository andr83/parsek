package com.github.andr83.parsek.spark.sink

import java.io.IOException

import com.github.andr83.parsek.PValue
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.sink.HttpSink.GzipRequestInterceptor
import com.github.andr83.parsek.spark.util.RDDUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import okhttp3.Interceptor.Chain
import okhttp3._
import okio.{BufferedSink, GzipSink, Okio}
import org.apache.spark.rdd.RDD

/**
  * @author andr83
  */
case class HttpSink(
  url: String,
  serializer: () => Serializer,
  codec: Option[String] = None,
  headers: Map[String, String] = Map.empty,
  batch: Boolean = true
) extends Sink {
  def this(config: Config) = this(
    url = config.as[String]("url"),
    codec = config.as[Option[String]]("codec"),
    serializer = config.as[Option[Config]]("serializer")
      .map(serializerConf => () => SerDe(serializerConf))
      .getOrElse(StringSerializer.factory),
    headers = config.as[Option[Map[String, String]]]("headers")
      .getOrElse(Map.empty[String, String]),
    batch = config.as[Option[Boolean]]("batch").getOrElse(true)
  )

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    val serializedRdd = RDDUtils.serialize(rdd, serializer)
    serializedRdd.foreachPartition(it => {
      if (it.nonEmpty) {
        val client = new OkHttpClient
        codec match {
          case Some("gzip") =>
            client.interceptors().add(new GzipRequestInterceptor)
          case Some(other) =>
            logger.error(s"Codec $other does not support.")
          case None =>
        }

        val requestBuilder = new Request.Builder().url(url)
        headers.foreach { case (k, v) => requestBuilder.addHeader(k, v) }

        val items = it.toList
        logger.info(s"Sending ${items.size} records. Head is: ${items.head}")

        if (batch) {
          send(client, requestBuilder, items.mkString("\n"))
        } else {
          items.foreach(data => send(client, requestBuilder, data))
        }
      }
    })
  }

  def send(client: OkHttpClient, requestBuilder: Request.Builder, data: String): Unit = {
    val body = RequestBody.create(null, data)
    requestBuilder.post(body)
    val request = requestBuilder.build()

    logger.info(s"Sending ${data.getBytes("UTF-8").length} bytes to $url")
    try {
      val response = client.newCall(request).execute()
      logger.info(s"Response code: ${response.code()}")

      if (response.isSuccessful) {
        logger.info(response.body().string())
      } else {
        throw new IOException("Unexpected code " + response)
      }
    } catch {
      case ex: Throwable =>
        println(ex)
        throw ex
    }
  }
}

object HttpSink {

  /** This interceptor compresses the HTTP request body. Many webservers can't handle this! */
  class GzipRequestInterceptor extends Interceptor {
    @throws[IOException]
    def intercept(chain: Chain): Response = {
      val originalRequest = chain.request
      if (originalRequest.body == null || originalRequest.header("Content-Encoding") != null) return chain.proceed(originalRequest)
      val compressedRequest =
        originalRequest
          .newBuilder
          .header("Content-Encoding", "gzip")
          .method(originalRequest.method, gzip(originalRequest.body))
          .build
      chain.proceed(compressedRequest)
    }

    private def gzip(body: RequestBody) = new RequestBody() {
      override def contentType: MediaType = body.contentType

      override def contentLength: Long = -1L // We don't know the compressed length in advance!

      @throws[IOException]
      override def writeTo(sink: BufferedSink): Unit = {
        val gzipSink = Okio.buffer(new GzipSink(sink))
        body.writeTo(gzipSink)
        gzipSink.close()
      }
    }
  }

}
