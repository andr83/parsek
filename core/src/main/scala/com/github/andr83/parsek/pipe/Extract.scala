package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Extract values from one PMap to other PMap.
  * @constructor create a new pipe to extract values from path to path
  * @param from from path
  * @param to to path
  *
  * @author andr83
 */
case class Extract(from: Seq[String], to: Seq[String]) extends Pipe {
  def this(config: Config) = this(
    config.as[String]("from").split('.'),
    config.as[Option[String]]("to").getOrElse("").split('.')
  )

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = value match {
    case PMap(root) =>
      Some(pop(value, from) match {
        case Some((v: PMap,m)) =>
          val res = if (m.nonEmpty) {
            root + (from.head -> PMap(m))
          } else root - from.head

          if (to.isEmpty) {
            res ++ v.value
          } else {
            res.updateValue(to, v)
          }
        case _ => root
      })
    case _ => throw new IllegalStateException(s"Can not extract values from $value")
  }

  /**
    * Return PMap values in path and a new branch after removing values
    * @param value current value in branch to extract, should be PMap only
    * @param path path in value to extract
    * @return
    */
  def pop(value: PValue, path: Seq[String]): Option[(PMap, Map[String, PValue])] = value match {
    case PMap(map) if path.length > 1 => map.get(path.head).flatMap(pop(_, path.tail)).map {
      case (v, child) if child.isEmpty => v -> (map - path.head)
      case (v, child) => v -> (map + (path.head -> child))
    }
    case PMap(map) =>  map.get(path.head) map {
      case v: PMap => v -> (map - path.head)
      case v =>  throw new IllegalStateException(s"Can not extract values from list in path ${path.mkString(".")}")
    }
    case PList(_) => throw new IllegalStateException(s"Can not extract values from list in path ${path.mkString(".")}")
    case _ => throw new IllegalStateException(s"Can not extract values in path ${path.mkString(".")}")
  }
}

object Extract {
  /**
    * Create a new extract pipe
    * @param from path string
    * @param to path string
    */
  def apply(from: String, to: String): Extract = Extract(from.split('.'), to.split('.'))

  /**
    * Create a new pipe to extract values from path to root
    * @param from path string
    */
  def apply(from: String): Extract = Extract(from.split('.'), Seq.empty[String])

  /**
    * Create a new extract pipe
    * @param config configuration object with from,to fields
    */
  def apply(config: Config): Extract = new Extract(config)
}
