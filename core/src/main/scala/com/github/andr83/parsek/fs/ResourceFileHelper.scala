package com.github.andr83.parsek.fs


/**
 * @author andr83
 */
class ResourceFileHelper extends FileHelper {
  override def readString(path: String): String = {
    val stream = getClass.getResourceAsStream(if (path.startsWith("resource://")) path.substring(11) else path)
    new String(Stream.continually(stream.read()).takeWhile(_ != -1).map(_.toChar).toArray)
  }

  override def readBytes(path: String): Array[Byte] = {
    val stream = getClass.getResourceAsStream(if (path.startsWith("resource://")) path.substring(11) else path)
    Stream.continually(stream.read()).takeWhile(_ != -1).map(_.toByte).toArray
  }
}
