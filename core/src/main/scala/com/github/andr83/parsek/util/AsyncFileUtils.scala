package com.github.andr83.parsek.util

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}
import java.nio.file.{Path, StandardOpenOption}

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.concurrent.{Future, Promise}
import scala.util.Success

/**
  * Async file utils
  *
  * @author andr83
  */
object AsyncFileUtils extends LazyLogging {

  def appendFile(buffer: ByteBuffer, path: Path): Future[Int] =
    appendFile(buffer, path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

  def appendFile(buffer: ByteBuffer, path: Path, options: StandardOpenOption*): Future[Int] = {
    val channel = AsynchronousFileChannel.open(path, options: _*)
    val promise = Promise[Int]()

    val writeCompletionHandler = new CompletionHandler[Integer, Promise[Int]] {
      def completed(result: Integer, promise: Promise[Int]) {
        channel.close()
        promise.complete(Success(result))
      }

      def failed(exc: Throwable, promise: Promise[Int]) {
        channel.close()
        logger.error(exc.toString)
        promise.failure(exc)
      }
    }

    channel.write(buffer, channel.size(), promise, writeCompletionHandler)
    promise.future
  }
}
