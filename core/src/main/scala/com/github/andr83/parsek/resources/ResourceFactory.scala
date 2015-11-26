package com.github.andr83.parsek.resources

import java.net.URI

import com.github.andr83.parsek.PValue
import com.typesafe.config.ConfigValueType._
import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}
import net.ceedubs.ficus.Ficus._

import scala.collection.JavaConversions._

/**
 * @author andr83
 */
class ResourceFactory(protected var loaders: Map[String, ResourceLoader]) {
  def +(loader: (String, ResourceLoader)): Unit = {
    loaders += loader
  }

  def ++(loader: Seq[(String, ResourceLoader)]): Unit = {
    loaders ++= loader
  }

  def getLoader(resourceType: String): ResourceLoader =
    loaders.getOrElse(resourceType, throw new IllegalStateException(s"Unknown resource type $resourceType"))

  def read(config: ConfigValue): PValue = config.valueType() match {
    case STRING =>
      val path = config.unwrapped().toString
      val pathUri = new URI(path)
      val loaderConfig = ConfigFactory.parseMap(Map("path" -> path))
      getLoader(pathUri.getScheme).read(loaderConfig)
    case OBJECT if config.asInstanceOf[ConfigObject].containsKey("type") =>
      val loaderConfig = config.asInstanceOf[ConfigObject].toConfig
      val resourceType = loaderConfig.as[String]("type")
      getLoader(resourceType).read(loaderConfig)
    case _ => throw new IllegalStateException(s"Invalid resource configuration: $config")
  }
}

object ResourceFactory {
  def apply(): ResourceFactory = new ResourceFactory(Map(
    "jar" -> JarResourceLoader,
    "file" -> LocalFileLoader
  ))
}
