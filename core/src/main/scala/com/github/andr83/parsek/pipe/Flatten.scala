package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Flatten embedded list values
  *
  * @param field embedded field to be flatten. Must contain a PList value.
  * @param recursively if true flat recursively all fields in the path
  *
  * @author andr83
  */
case class Flatten(field: FieldPath, recursively: Boolean = true) extends Pipe {

  def this(config: Config) = this(
    field = config.as[String]("field").asFieldPath,
    recursively = config.as[Option[Boolean]]("recursively").getOrElse(true)
  )

  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = if (recursively) {
    val res = flatten(field, Map.empty[String, PValue], value)
    if (res.isEmpty) None else Some(PList(res))
  } else Some(flattenInner(field, value))

  /**
    * Flatten recursively fields in current path
    *
    * @param fields path
    * @param current value from path to be merged
    * @param value current level value
    * @return
    */
  def flatten(fields: FieldPath, current: Map[String, PValue], value: PValue): List[PValue] = if (fields.nonEmpty) {
    value match {
      case PList(list) => list flatMap (flatten(fields, current, _))
      case pm@PMap(map) =>
        val f = fields.head
        map.get(f) map (v => {
          flatten(fields.tail, current ++ (map - f), v)
        }) getOrElse List(pm)
      case v if fields.length == 1 => List(PMap(current + (fields.head -> v)))
      case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
    }
  } else {
    value match {
      case PList(list) => list flatMap {
        case PMap(map) => List(PMap(current ++ map))
        case innerList: PList => flatten(fields, current, innerList)
        case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
      }
      case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
    }
  }

  /**
    * Flatten only last field in the path
    *
    * @param fields path
    * @param value current value
    * @return
    */
  def flattenInner(fields: FieldPath, value: PValue): PValue = if (fields.nonEmpty) {
    value match {
      case pm@PMap(map) => map.get(fields.head) match {
        case Some(v) =>
          val res = map + (fields.head -> flattenInner(fields.tail, v))
          PMap(res)
        case None => pm
      }
      case PList(list) => PList(list.map(flattenInner(fields.tail, _)))
      case _ => throw new IllegalStateException(s"Can not flatten value $value in path ${field.mkString(".")}")
    }
  } else flatten(fields, Map.empty[String, PValue], value)
}

object Flatten {
  def apply(config: Config): Flatten = new Flatten(config)
}
