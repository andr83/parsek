package com.github.andr83.parsek.fs

/**
 * @author andr83
 */
abstract class FileHelper {
  def readString(path: String): String
  def readBytes(path: String): Array[Byte]
}
