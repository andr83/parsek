package com.github.andr83.parsek.spark.util

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress._
import resource._

import scala.io.Source

/**
 * @author andr83
 */
object HadoopUtils {
  def getCodec(name: String): Class[_ <: CompressionCodec] = name match {
    case "bzip2" => classOf[BZip2Codec]
    case "default" => classOf[DefaultCodec]
    case "deflate" => classOf[DeflateCodec]
    case "gzip" => classOf[GzipCodec]
    case "lz4" => classOf[Lz4Codec]
    case "snappy" => classOf[SnappyCodec]
    case _ => throw new IllegalArgumentException(s"Unknown codec $name")
  }

  lazy val fs = {
    val conf = new Configuration
    conf.set("fs.hdfs.impl",
      "org.apache.hadoop.hdfs.DistributedFileSystem"
    )
    conf.set("fs.file.impl",
      "org.apache.hadoop.fs.LocalFileSystem"
    )
    FileSystem.get(conf)
  }

  def readString(path: String, fs: FileSystem = fs): String = {
    (for (
      in <- managed(fs.open(new Path(path)))
    ) yield IOUtils.toString(in)).either match {
      case Right(content) => content
      case Left(errors) => throw errors.head
    }
  }

  def readBytes(path: String, fs: FileSystem = fs): Array[Byte] = if (path.startsWith("hdfs://")) {
    (for (
      in <- managed(fs.open(new Path(path)))
    ) yield IOUtils.toByteArray(in)).either match {
      case Right(content) => content
      case Left(errors) => throw errors.head
    }
  } else Source.fromFile(path).map(_.toByte).toArray

  def writeString(data: String, path: String, overwrite: Boolean = true, fs: FileSystem = fs): Either[Seq[Throwable], Unit] = {
    (for (
      out <- managed(fs.create(new Path(path)))
    ) yield IOUtils.write(data, out)).either
  }
}
