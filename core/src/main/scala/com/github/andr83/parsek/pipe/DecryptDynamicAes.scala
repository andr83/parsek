package com.github.andr83.parsek.pipe

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author Andrei Tupitcyn
 */
case class DecryptDynamicAes(config: Config) extends TransformPipe(config) {
  val aesKeyField = config.as[String]("aesKeyField").split('.').toSeq
  val algorithm = config.as[Option[String]]("algorithm").getOrElse("AES/ECB/PKCS5Padding")

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    context.row.getValue(aesKeyField).map{
      case PString(aesKeyStr) =>
        val aesKey = aesKeyStr.asBytes
        val encipher = Cipher.getInstance(algorithm)
        algorithm match {
          case "AES/CBC/PKCS5Padding" =>
            val secretKey = new SecretKeySpec(aesKey.take(16), "AES")
            encipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(aesKey.takeRight(16)))
          case _ =>
            val secretKey = new SecretKeySpec(aesKey, "AES")
            encipher.init(Cipher.DECRYPT_MODE, secretKey)
        }
        encipher.doFinal(str.dropWhile(_ == 0).asBytes).asStr
      case value => throw new IllegalStateException(s"Aes key must be a string but $value given")
    }
  }
}
