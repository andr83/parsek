package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek._
import com.github.andr83.parsek.serde.{SerDe, Serializer}
import com.github.andr83.parsek.spark.util.HadoopUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD

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
  serializer: Option[() => Serializer] = None
) extends Sink {

  def this(config: Config) = this(
    path = config.getString("path"),
    codec = config.as[Option[String]]("codec") map HadoopUtils.getCodec,
    serializer = config.as[Option[Config]]("serializer").map(serializerConf => () => SerDe(serializerConf))
  )

  override def sink(rdd: RDD[PValue]): Unit = {
    val out = serializer map (serializerFactory => rdd.mapPartitions(it => {
      val ser = serializerFactory()
      it.map(v => new String(ser.write(v).map(_.toChar)))
    }).filter(_.trim.nonEmpty)) getOrElse rdd
    try {
      codec match {
        case Some(codecClass) => out.saveAsTextFile(path, codecClass)
        case None => out.saveAsTextFile(path)
      }
    } catch {
      case e: Exception =>
        logger.error(e.toString, e)
    }
  }
}
