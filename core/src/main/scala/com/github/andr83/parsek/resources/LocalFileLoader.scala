package com.github.andr83.parsek.resources

import java.net.URI

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.io.Source

/**
 * @author andr83
 */
object LocalFileLoader extends ResourceLoader {
  override def read(config: Config): PValue = {
    val uri = new URI(config.as[String]("path"))
    config.as[Option[String]]("encoding") match {
      case Some(encoding) => Source.fromFile(uri, encoding).mkString
      case None => Source.fromFile(uri).mkString
    }
  }
}
