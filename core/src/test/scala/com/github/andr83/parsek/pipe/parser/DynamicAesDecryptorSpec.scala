package com.github.andr83.parsek.pipe.parser

import java.security.KeyPairGenerator
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class DynamicAesDecryptorSpec extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  "The content " should " be a string encoded with dynamic AES key with parameters" in {
    val rawBody = RandomStringUtils.random(40, true, false)

    //Generate AES key
    val aesKey = RandomStringUtils.random(16).getBytes("UTF-8").take(16)
    val aesPadding = RandomStringUtils.random(16).getBytes("UTF-8").take(16)
    val aesKeySpec = new SecretKeySpec(aesKey, "AES")
    val aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    aesCipher.init(Cipher.ENCRYPT_MODE, aesKeySpec, new IvParameterSpec(aesPadding))

    //Encrypt body message with AES
    val body = aesCipher.doFinal(rawBody.getBytes).asStr

    val config = ConfigFactory.parseMap(Map(
      "field" -> "body",
      "aesKeyField" -> "aesKey",
      "algorithm" -> "AES/CBC/PKCS5Padding"
    ))

    val decryptor = DynamicAesDecryptor(config)

    val result: Option[PMap] = decryptor.run(PMap(Map(
      "body" -> body,
      "aesKey" -> (aesKey ++ aesPadding).asStr
    ))).map(_.asInstanceOf[PMap])

    assert(result.nonEmpty)

    inside(result) {
      case Some(PMap(map)) =>
        map should contain key "body"
        val body = map.get("body").get.asInstanceOf[PString].value
        assert(body == rawBody)
    }
  }

  "The content " should " be a string encoded with dynamic AES key without parameters" in {
    val rawBody = RandomStringUtils.random(40, true, false)

    //Generate AES key
    val aesKey = RandomStringUtils.random(16).getBytes("UTF-8").take(16)
    val aesKeySpec = new SecretKeySpec(aesKey, "AES")
    val aesCipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    aesCipher.init(Cipher.ENCRYPT_MODE, aesKeySpec)

    //Encrypt body message with AES
    val body = aesCipher.doFinal(rawBody.getBytes).asStr

    val config = ConfigFactory.parseMap(Map(
      "field" -> "body",
      "aesKeyField" -> "aesKey",
      "algorithm" -> "AES/ECB/PKCS5Padding"
    ))

    val decryptor = DynamicAesDecryptor(config)

    val result: Option[PMap] = decryptor.run(PMap(Map(
      "body" -> body,
      "aesKey" -> aesKey.asStr
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
