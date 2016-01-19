package com.github.andr83.parsek.pipe

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.github.andr83.parsek._
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{FlatSpec, Inside, Matchers}

/**
 * @author andr83
 */
class DecryptDynamicAesSpec extends FlatSpec with Matchers with Inside {
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

    val decryptor = DecryptDynamicAesPipe(
      aesKeyField = "aesKey".asFieldPath,
      field = "body".asFieldPath,
      algorithm = "AES/CBC/PKCS5Padding"
    )

    val result: Option[PMap] = decryptor.run(PMap(
      "body" -> body,
      "aesKey" -> (aesKey ++ aesPadding).asStr
    )).map(_.asInstanceOf[PMap])

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

    val decryptor = DecryptDynamicAesPipe(
      aesKeyField = "aesKey".asFieldPath,
      field = "body".asFieldPath,
      algorithm = "AES/ECB/PKCS5Padding"
    )

    val result: Option[PMap] = decryptor.run(PMap(
      "body" -> body,
      "aesKey" -> aesKey.asStr
    )).map(_.asInstanceOf[PMap])

    assert(result.nonEmpty)

    inside(result) {
      case Some(PMap(map)) =>
        map should contain key "body"
        val body = map.get("body").get.asInstanceOf[PString].value
        assert(body == rawBody)
    }
  }
}
