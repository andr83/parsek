package com.github.andr83.parsek.pipe.parser

import java.security.KeyPairGenerator
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.AesWithRsaKeyDecryptor
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class AesWithRsaKeyDecryptorSpec extends FlatSpec with Matchers with Inside {
  // at first necessary convert RSA private key:
  // openssl pkcs8 -topk8 -inform PEM -outform DER -in private.pem -out private.der -nocrypt
  "The content " should " be zipped Json string encoded with Base64 + AES" in {
    val rawBody = RandomStringUtils.random(40, true, false)
    //Generating RSA private key
    val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
    keyPairGenerator.initialize(1024)
    val keyPair = keyPairGenerator.genKeyPair()

    //Generate AES key
    val aesKey = RandomStringUtils.random(16).getBytes("UTF-8").take(16)
    val aesPadding = RandomStringUtils.random(16).getBytes("UTF-8").take(16)
    val aesKeySpec = new SecretKeySpec(aesKey, "AES")
    val aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    aesCipher.init(Cipher.ENCRYPT_MODE, aesKeySpec, new IvParameterSpec(aesPadding))

    //Encrypt AES key with RSA
    val rsaCipher = Cipher.getInstance("RSA/ECB/NoPadding")
    rsaCipher.init(Cipher.ENCRYPT_MODE, keyPair.getPublic)
    val aesRsa = rsaCipher.doFinal(aesKey ++ aesPadding).asStr

    //Encrypt body message with AES
    val body = aesCipher.doFinal(rawBody.getBytes).asStr

    val config = ConfigFactory.parseMap(Map(
      "privateKey" -> keyPair.getPrivate.getEncoded.asStr,
      "field" -> "body",
      "aesKeyField" -> "aesKey"
    ))

    val decryptor = AesWithRsaKeyDecryptor(config)

    val result: Option[PMap] = decryptor.run(PMap(Map(
      "body" -> body,
      "aesKey" -> aesRsa
    ))).map(_.asInstanceOf[PMap])

    assert(result.nonEmpty)

    inside(result) {
      case Some(PMap(map)) =>
        map should contain key "body"
        val body = map.get("body").get.asInstanceOf[PString].value
        assert(body == rawBody)
    }
  }
}
