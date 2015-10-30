package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config

import scala.io.Source

/**
 * @author andr83
 */
case class RSADecryptor(config: Config) extends TransformPipe(config) {
  val privateKey = {
    val rsaKeyBytes = Source.fromFile(config.getStringReq("privateKey")).map(_.toByte).toArray

  }
  override def transformString(raw: String): Option[PValue] = ???
}
