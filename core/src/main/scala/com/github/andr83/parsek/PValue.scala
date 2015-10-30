package com.github.andr83.parsek

import com.github.nscala_time.time.Imports._

/**
 * @author andr83
 */
sealed abstract class PValue {
  type valueType

  def value: valueType
}

final case class PString(value: String) extends PValue {
  type valueType = String
}

final case class PInt(value: Int) extends PValue {
  override type valueType = Int
}

final case class PLong(value: Long) extends PValue {
  override type valueType = Long
}

final case class PDouble(value: Double) extends PValue {
  override type valueType = Double
}

final case class PBool(value: Boolean) extends PValue {
  override type valueType = Boolean
}

final case class PTime(value: DateTime) extends PValue {
  override type valueType = DateTime
}

object PTime {
  def apply(time: Long): PTime = PTime(new DateTime(time))
}

final case class PMap(value: Map[String, PValue]) extends PValue {
  type valueType = Map[String, PValue]

  def getValue(path: String): Option[PValue] = getValue(path.split('.'))

  def getValue(path: Seq[String]): Option[PValue] = {
    val head = path.head
    val tail = path.tail
    if (tail.isEmpty) {
      value.value.get(head)
    } else value.value.get(head) flatMap {
      case map: PMap => map.getValue(tail)
      case _ => throw new IllegalStateException(s"Failed extracting value from path $path in $value")
    }
  }

  def updateValue(path: Seq[String], newValue: PValue): PMap = PMap.updateValue(this, path, newValue)
}

final case class PList(value: List[PValue]) extends PValue {
  type valueType = List[PValue]
}

object PValue {
  def apply(value: Any): PValue = value match {
    case lv: List[_] => PList(lv.map(apply))
    case mv: Map[_, _] => PMap(mv.filterKeys(_.isInstanceOf[String]).asInstanceOf[Map[String, Any]].mapValues(apply))
    case v: String => PString(v)
    case v: Int => PInt(v)
    case v: Long => PLong(v)
    case v: Float => PDouble(v)
    case v: Double => PDouble(v)
    case v: DateTime => PTime(v)
    case v: PValue => v
    case v => throw new IllegalArgumentException("Unsupported argument type: " + v.getClass.getName)
  }
}

object PList {
  def empty = PList(List.empty[PValue])
}

object PMap {
  def empty = PMap(Map.empty[String, PValue])

  def updateValue(map: PMap, path: Seq[String], newValue: PValue): PMap = {
    val head = path.head
    val tail = path.tail
    if (tail.isEmpty) {
      PMap(map.value + (head -> newValue))
    } else map.value.get(head) match {
      case Some(v:PMap) => updateValue(v, tail, newValue)
      case None => updateValue(PMap(map.value + (head -> PMap.empty)), tail, newValue)
      case _ => throw new IllegalStateException(s"Can not update value in path $path in $map")
    }
  }
}