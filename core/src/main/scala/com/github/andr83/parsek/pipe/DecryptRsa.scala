package com.github.andr83.parsek.pipe

import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import javax.crypto.Cipher

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author Andrei Tupitcyn
 */
case class DecryptRsa(config: Config) extends TransformPipe(config) {
  val privateKey = {
    val rsaKeyBytes = config.as[String]("privateKey").asBytes

    val spec = new PKCS8EncodedKeySpec(rsaKeyBytes)
    val kf = KeyFactory.getInstance("RSA")
    kf.generatePrivate(spec)
  }

  val cipher = Cipher.getInstance(config.as[Option[String]]("algorithm").getOrElse("RSA/ECB/PKCS1Padding"))
  cipher.init(Cipher.DECRYPT_MODE, privateKey)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    Some(cipher.doFinal(str.asBytes).dropWhile(_ == 0).asStr)
  }
}
