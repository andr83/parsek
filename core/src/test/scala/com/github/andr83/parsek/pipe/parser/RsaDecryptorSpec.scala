package com.github.andr83.parsek.pipe.parser

import java.security.KeyPairGenerator
import javax.crypto.Cipher

import com.github.andr83.parsek._
import com.github.andr83.parsek.pipe.DecryptRsa
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.collection.JavaConversions._

/**
 * @author Andrei Tupitcyn
 */
class RsaDecryptorSpec extends FlatSpec with Matchers with Inside {
  implicit val context = new PipeContext()

  // at first necessary convert RSA private key:
  // openssl pkcs8 -topk8 -inform PEM -outform DER -in private.pem -out private.der -nocrypt
  "The content " should " be a string encoded with RSA" in {
    val rawBody = RandomStringUtils.random(40, true, false)
    //Generating RSA private key
    val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
    keyPairGenerator.initialize(1024)
    val keyPair = keyPairGenerator.genKeyPair()

    //Encrypt body with RSA
    val rsaCipher = Cipher.getInstance("RSA/ECB/NoPadding")
    rsaCipher.init(Cipher.ENCRYPT_MODE, keyPair.getPublic)
    val body = rsaCipher.doFinal(rawBody.asBytes).asStr

    val config = ConfigFactory.parseMap(Map(
      "privateKey" -> keyPair.getPrivate.getEncoded.asStr,
      "algorithm" -> "RSA/ECB/NoPadding",
      "field" -> "body"
    ))

    val decryptor = DecryptRsa(config)

    val result: Option[PMap] = decryptor.run(PMap(Map(
      "body" -> body
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
