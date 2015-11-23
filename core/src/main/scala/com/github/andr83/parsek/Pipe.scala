package com.github.andr83.parsek

import com.github.andr83.parsek.pipe.parser._
import com.github.andr83.parsek.pipe.{AesWithRsaKeyDecryptor, Base64Decoder, GzipDecompressor}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * @author andr83
 */
trait Pipe extends LazyLogging with Serializable {
  val config: Config

  def run(value: PValue)(implicit context: Context): Option[PValue]
}

object Pipe {
  def apply(name: String, config: Config): Pipe = if (name.contains(".")) {
    val constructor = Class.forName(name).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[Pipe]
  } else name match {
    case "parseJson" => JsonParser(config)
    case "parseRegex" => RegexParser(config)
    case "decodeBase64" => Base64Decoder(config)
    case "decryptAesWithRsa" => AesWithRsaKeyDecryptor(config)
    case "ungzip" => GzipDecompressor(config)
    case _ => throw new IllegalStateException(s"Unknown pipe $name")
  }
}
