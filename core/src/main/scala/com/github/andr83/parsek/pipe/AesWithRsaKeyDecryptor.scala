package com.github.andr83.parsek.pipe

import java.security.KeyFactory
import java.security.spec.PKCS8EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class AesWithRsaKeyDecryptor(config: Config) extends TransformPipe(config) {
  val privateKey = {
    val rsaKeyBytes = config.as[String]("privateKey").asBytes

    val spec = new PKCS8EncodedKeySpec(rsaKeyBytes)
    val kf = KeyFactory.getInstance("RSA")
    kf.generatePrivate(spec)
  }

  val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
  cipher.init(Cipher.DECRYPT_MODE, privateKey)

  val aesKeyField = config.as[String]("aesKeyField").split('.').toSeq

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    for {
      aesRsaKey <- context.row.getValue(aesKeyField)
    } yield {
      val aesKey = decryptRsa(aesRsaKey.value.toString.asBytes).dropWhile(_ == 0)
      val body = decryptAes(aesKey.take(16), aesKey.takeRight(16), str.asBytes)
      PString(body.asStr)
    }
  }

  def decryptRsa(key: Array[Byte]): Array[Byte] = {
    cipher.doFinal(key)
  }

  def decryptAes(aesKey: Array[Byte], aesParams: Array[Byte], body: Array[Byte]): Array[Byte] = {
    val secretKey = new SecretKeySpec(aesKey, "AES")
    val encipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    encipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(aesParams))
    encipher.doFinal(body)
  }
}