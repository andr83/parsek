package com.github.andr83.parsek.resources

import java.net.URI

import com.github.andr83.parsek.PValue
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
object JarResourceLoader extends ResourceLoader {
  override def read(config: Config): PValue = {
    val path = new URI(config.as[String]("path")).getPath
    val stream = getClass.getResourceAsStream(path)
    new String(Stream.continually(stream.read()).takeWhile(_ != -1).map(_.toChar).toArray)
  }
}
