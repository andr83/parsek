package com.github.andr83.parsek.spark.sink

import com.github.andr83.parsek._
import com.github.andr83.parsek.serde.SerDe
import com.github.andr83.parsek.spark.Sink
import com.github.andr83.parsek.spark.util.HadoopUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

/**
 * @author andr83
 */
class TextFileSink(config: Config) extends Sink(config) {
  val path = config.getString("path")
  val codec = config.as[Option[String]]("codec") map HadoopUtils.getCodec
  val serializer = config.as[Option[Config]]("serializer")

  override def sink(rdd: RDD[PValue]): Unit = {
    val out = serializer map (serializerConf => rdd.mapPartitions(it => {
      val s = SerDe(serializerConf)
      it.map(v => new String(s.write(v).map(_.toChar)))
    }).filter(_.trim.nonEmpty)) getOrElse rdd
    codec match {
      case Some(codecClass) => out.saveAsTextFile(path, codecClass)
      case None => out.saveAsTextFile(path)
    }
  }
}
