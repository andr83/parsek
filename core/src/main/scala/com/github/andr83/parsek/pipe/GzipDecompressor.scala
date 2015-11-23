package com.github.andr83.parsek.pipe

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

import com.github.andr83.parsek._
import com.typesafe.config.Config

/**
 * @author andr83
 */
case class GzipDecompressor(config: Config) extends TransformPipe(config) {
  override def transformString(str: String)(implicit context: Context): Option[PValue] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(str.asBytes))
    val out = scala.io.Source.fromInputStream(in).getLines().mkString("\n")
    Some(PString(out))
  }
}
