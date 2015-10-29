package com.github.andr83.parsek.spark.util

import org.apache.hadoop.io.compress._

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
}
