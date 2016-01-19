package com.github.andr83.parsek.spark.sink

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Path, Paths}

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.util.RDDUtils
import com.github.andr83.parsek.util.AsyncFileUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Asynchronous local file channel sync with file size rolling and partition support.
  *
  * @param path Path to output results
  * @param serializer factory function which return Serializer instance
  * @param partitions FieldFormatter's which will define partition rules
  * @param fileNamePattern pattern for output files. Default is partition values joined with underscore ('_')
  * @param rollSize file size limit to roll it after. Current file will be renamed to file.N where N is next number available.
  *
  * @author andr83
  */
case class FileChannelSink(
  path: String,
  serializer: () => Serializer = StringSerializer.factory,
  partitions: Seq[FieldFormatter] = Seq.empty[FieldFormatter],
  fileNamePattern: Option[String] = None,
  rollSize: Long = 0
) extends Sink {

  def this(config: Config) = this(
    path = config.as[String]("path"),
    serializer = config.as[Option[Config]]("serializer")
      .map(serializerConf => () => SerDe(serializerConf))
      .getOrElse(StringSerializer.factory),
    partitions = if (config.hasPath("partitions"))
      FieldFormatter(config.getList("partitions"))
    else Seq.empty[FieldFormatter],
    fileNamePattern = config.as[Option[String]]("fileNamePattern"),
    rollSize = config.as[Option[Long]]("rollSize").getOrElse(0L)
  )

  override def sink(rdd: RDD[PValue]): Unit = {
    val outRdd = RDDUtils.serializeAndPartitionBy(rdd, serializer, partitions)

    val partitionKeys = partitions.map(_.asField.mkString("."))

    if (outRdd.isEmpty()) {
      logger.info("Rdd is empty")
    } else {
      new java.io.File(path).mkdirs()

      outRdd
        .foreachPartition(it => {
          it.toList
            .groupBy(_._1)
            .foreach { case (keys, lines) =>
              val fileName = fileNamePattern map (pattern => partitionKeys.foldLeft((0, pattern)) {
                case ((idx, p), partitionKey) => (idx + 1, p.replaceAllLiterally("${" + partitionKey + "}", keys(idx)))
              }._2) getOrElse keys.mkString("_")
              val filePath = Paths.get(path, fileName)
              val file = filePath.toFile

              if (rollSize > 0) {
                var fileSize: Long = if (file.exists()) {
                  file.length()
                } else 0L

                val buffer = new ArrayBuffer[Byte]()
                for (line <- lines) {
                  val bLine = line._2.getBytes()
                  if (fileSize + buffer.size + bLine.size >= rollSize) {
                    if (fileSize + buffer.size >= rollSize) {
                      rollFile(file)
                      fileSize = 0L
                    }
                    if (buffer.nonEmpty) {
                      dump(filePath, ByteBuffer.wrap(buffer.take(buffer.length - 1).toArray))
                      fileSize += buffer.size - 1
                      buffer.clear()
                    }
                  }

                  buffer ++= bLine
                  buffer += '\n'
                }

                if (buffer.nonEmpty) {
                  if (fileSize + buffer.size >= rollSize) {
                    rollFile(file)
                  }
                  dump(filePath, ByteBuffer.wrap(buffer.take(buffer.length - 1).toArray))
                }
              } else {
                val buffer = ByteBuffer.wrap(lines.map(_._2).mkString("\n").getBytes())
                dump(filePath, buffer)
              }
            }

        })
    }
  }

  /**
    * Write buffer to file
    *
    * @param path file to output
    * @param buffer Data content
    */
  def dump(path: Path, buffer: ByteBuffer): Unit = {
    path.getParent.toFile.mkdirs()
    AsyncFileUtils
      .appendFile(buffer, path)
      .onSuccess { case size =>
        logger.info(s"Write $size bytes to $path")
      }
  }

  /**
    * Roll file to fileName.N
    *
    * @param file file path to roll
    * @param index next index
    */
  def rollFile(file: File, index: Int = 0): Unit = {
    val rolledFile = new File(file.getAbsolutePath + "." + index)
    if (rolledFile.exists()) {
      rollFile(file, index + 1)
    } else {
      file.renameTo(rolledFile)
      logger.info(s"Rename $file to $rolledFile")
    }
  }
}
