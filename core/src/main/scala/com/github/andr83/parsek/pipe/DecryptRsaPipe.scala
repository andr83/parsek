package com.github.andr83.parsek.pipe

import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey}
import javax.crypto.Cipher

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Decrypt field value by Rsa algorithm
  *
  * @param privateKey private Rsa key
  * @param field field path to decrypt
  * @param algorithm algorithm to decrypt, by default AES/ECB/PKCS5Padding
  * @param as path where to save result of decrypting
  *
  * @author Andrei Tupitcyn
  */
case class DecryptRsaPipe(
  privateKey: PrivateKey,
  field: FieldPath,
  algorithm: String = DecryptRsaPipe.DefaultAlgorithm,
  as: Option[FieldPath] = None
) extends TransformPipe(field, as) {

  def this(config: Config) = this(
    privateKey = DecryptRsaPipe.getPrivateKey(config.as[String]("privateKey").asBytes),
    field = config.as[String]("field").split('.').toSeq,
    algorithm = config.as[Option[String]]("algorithm").getOrElse(DecryptRsaPipe.DefaultAlgorithm),
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  val cipher = Cipher.getInstance(algorithm)
  cipher.init(Cipher.DECRYPT_MODE, privateKey)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    Some(cipher.doFinal(str.asBytes).dropWhile(_ == 0).asStr)
  }
}

object DecryptRsaPipe {
  val DefaultAlgorithm = "RSA/ECB/PKCS1Padding"

  def apply(config: Config): DecryptRsaPipe = new DecryptRsaPipe(config)

  def getPrivateKey(key: Array[Byte]): PrivateKey = {
    val spec = new PKCS8EncodedKeySpec(key)
    val kf = KeyFactory.getInstance("RSA")
    kf.generatePrivate(spec)
  }
}
