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

final case class PDate(value: DateTime) extends PValue {
  override type valueType = DateTime
}

object PDate {
  def apply(time: Long): PDate = PDate(new DateTime(time))
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
  def removeValue(path: Seq[String]): PMap = PMap.removeValue(this, path)
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
    case v: DateTime => PDate(v)
    case v: PValue => v
    case v => throw new IllegalArgumentException("Unsupported argument type: " + v.getClass.getName)
  }
}

object PList {
  def empty = PList(List.empty[PValue])
  def create(values: PValue*): PList = new PList(values.toList)
}

object PMap {
  def empty = PMap(Map.empty[String, PValue])

  def apply(values: (String, PValue)*): PMap = new PMap(Map(values:_*))

  def updateValue(map: PMap, path: Seq[String], newValue: PValue): PMap = {
    val head = path.head
    val tail = path.tail
    if (tail.isEmpty) {
      PMap(map.value + (head -> newValue))
    } else map.value.get(head) match {
      case Some(v: PMap) => PMap(map.value + (head -> updateValue(v, tail, newValue)))
      case None => PMap(map.value + (head -> updateValue(PMap.empty, tail, newValue)))
      case _ => throw new IllegalStateException(s"Can not update value in path $path in $map")
    }
  }

  def removeValue(map: PMap, path: Seq[String]): PMap = {
    val head = path.head
    val tail = path.tail
    if (tail.isEmpty) {
      PMap(map.value - head)
    } else map.value.get(head) match {
      case Some(v: PMap) =>
        val newValue = removeValue(v, tail)
        if (newValue.value.isEmpty) {
          PMap(map.value - head)
        } else {
          PMap(map.value + (head -> newValue))
        }
      case None => map
      case _ => throw new IllegalStateException(s"Can not remove value in path $path in $map")
    }
  }
}