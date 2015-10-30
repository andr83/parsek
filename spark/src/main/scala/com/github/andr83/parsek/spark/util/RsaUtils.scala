package com.github.andr83.parsek.spark.util

import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey}

/**
 * @author andr83
 */
object RsaUtils {
  def privateKeyFactory(privateKey: Array[Byte]): PrivateKey = {
    val spec = new PKCS8EncodedKeySpec(privateKey)
    val kf = KeyFactory.getInstance("RSA")
    kf.generatePrivate(spec)
  }
}
