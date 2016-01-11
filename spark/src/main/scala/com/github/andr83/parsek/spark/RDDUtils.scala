package com.github.andr83.parsek.spark

import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{Serializer, StringSerializer}
import com.github.andr83.parsek.{PMap, PValue}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

/**
  * Common RDD functions
  *
  * @author andr83
  */
object RDDUtils extends LazyLogging {
  def serializeAndPartitionBy(
    rdd: RDD[PValue],
    serializerFactory: () => Serializer = StringSerializer.factory,
    partitions: Seq[FieldFormatter] = Seq.empty[FieldFormatter],
    numPartitions: Int = 0
  ): RDD[(Seq[String], String)] = {
    rdd.mapPartitionsWithIndex {
      case (idx: Int, it: Iterator[PValue]) =>
        val serializer = serializerFactory()
        it.flatMap(v=> {
          val keyOpt = if (partitions.isEmpty) {
            Some(Seq(idx.toString))
          } else {
            v match {
              case r: PMap =>
                Some(partitions.map(_.apply(r)))
              case r =>
                logger.warn(s"Cannot build partition $partitions from value $v")
                None
            }
          }
          val res = serializer.write(v).map(_.toChar)

          for {
            key <- keyOpt
            value <- if(res.isEmpty) None else Some(new String(res))
          } yield key -> value
        })
    } partitionBy new KeyPartitioner(if (numPartitions > 0) numPartitions else rdd.partitions.length)
  }

  class KeyPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      (key.toString.hashCode & Integer.MAX_VALUE) % numPartitions // make sure lines with the same key in the same partition
    }
  }
}
