package com.github.andr83.parsek.pipe

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Decrypt field value by Aes algorithm with key from aesKeyField
  *
  * @param aesKeyField field path to AES private key
  * @param field field path to decrypt
  * @param algorithm algorithm to decrypt, by default AES/ECB/PKCS5Padding
  * @param as path where to save result of decrypting
  *
  * @author andr83
  */
case class DecryptDynamicAesPipe(
  aesKeyField: FieldPath,
  field: FieldPath,
  algorithm: String = DecryptDynamicAesPipe.DefaultAlgorithm,
  as: Option[FieldPath] = None) extends TransformPipe(field, as)
{
  def this(config: Config) = this(
    aesKeyField = config.as[String]("aesKeyField").asFieldPath,
    field = config.as[String]("field").split('.').toSeq,
    algorithm = config.as[Option[String]]("algorithm").getOrElse(DecryptDynamicAesPipe.DefaultAlgorithm),
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

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

object DecryptDynamicAesPipe {
  val DefaultAlgorithm = "AES/ECB/PKCS5Padding"

  def apply(config: Config): DecryptDynamicAesPipe = new DecryptDynamicAesPipe(config)
}
