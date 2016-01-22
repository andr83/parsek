package com.github.andr83.parsek.util

import java.io.FileWriter

import scala.util.{Failure, Success, Try}

/**
  * @author andr83
  */
object FileUtils {
  def writeLines(path: String, lines: Iterable[String], append: Boolean): Try[Unit] =
    writeLines(path, lines.iterator, append)

  def writeLines(path: String, lines: Iterator[String], append: Boolean = false): Try[Unit] = {
    val writer = new FileWriter(path, append)
    try {
      lines.foreach(line => writer.write(line + "\n"))
      Success(Unit)
    } catch {
      case e: Exception => Failure(e)
    } finally {
      writer.close()
    }
  }
}
