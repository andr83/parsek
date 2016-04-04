package com.github.andr83.parsek.spark.util

import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{Serializer, StringSerializer}
import com.github.andr83.parsek.{PMap, PValue}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
  * Common RDD functions
  *
  * @author andr83
  */
object RDDUtils extends LazyLogging {

  def serialize(
    rdd: RDD[PValue],
    serializerFactory: () => Serializer = StringSerializer.factory
  ): RDD[String] = {
    rdd.mapPartitions(it=> {
      val serializer = serializerFactory()
      it.map(v=> new String(serializer.write(v).map(_.toChar)))
    })
  }

  def serializeAndPartitionBy(
    rdd: RDD[PValue],
    serializerFactory: () => Serializer = StringSerializer.factory,
    partitioner: RDDPartitioner,
    numPartitions: Option[Int] = None
  ): RDD[(String, String)] = {
    val rddWithKey = rdd.mapPartitionsWithIndex {
      case (idx: Int, it: Iterator[PValue]) =>
        val serializer = serializerFactory()
        it.flatMap(v=> {
          val keyOpt = partitioner(idx, v) map(key=> key.replaceAllLiterally("${partitionIndex}", idx.toString))
          val res = serializer.write(v).map(_.toChar)
          for {
            key <- keyOpt
            value <- if(res.isEmpty) None else Some(new String(res))
          } yield key -> value
        })
    }

    val num = numPartitions getOrElse {
      val num = rddWithKey.keys.distinct().count().toInt
      logger.warn(s"Autodetect $num partitions. Use numPartitions option for better performance.")
      num
    }
    rddWithKey partitionBy new HashPartitioner(num)
  }

  trait RDDPartitioner {
    def apply(partitionIndex: Int, value: PValue): Option[String]
  }

  @SerialVersionUID(1L)
  case class DefaultPartitioner(pattern: Option[String]) extends RDDPartitioner with Serializable {
    def apply(partitionIndex: Int, value: PValue): Option[String] = Some(
      pattern getOrElse s"part-r-$partitionIndex"
    )
  }

  @SerialVersionUID(1L)
  case class FieldsPartitioner(formatters: Seq[FieldFormatter], pattern: Option[String]) extends RDDPartitioner with Serializable {
    val partitionKeys = formatters.map(_.asField.mkString("."))

    def apply(partitionIndex: Int, value: PValue): Option[String] = value match {
      case r: PMap =>
        val partitions = formatters.map(_.apply(r))
        val res = pattern map (pattern => partitionKeys.foldLeft((0, pattern)) {
          case ((idx, p), partitionKey) => (idx + 1, p.replaceAllLiterally("${" + partitionKey + "}", partitions(idx)))
        }._2) getOrElse partitions.mkString("_")
        Some(res)
      case r =>
        logger.warn(s"Cannot build partition $formatters from value $value")
        None
    }
  }
}
