package com.github.andr83.parsek.fs

import scala.io.Source

/**
 * @author andr83
 */
class LocalFileHelper extends FileHelper {
  override def readString(path: String): String = Source.fromFile(path).mkString

  override def readBytes(path: String): Array[Byte] = Source.fromFile(path, "ISO-8859-1").map(_.toByte).toArray
}
