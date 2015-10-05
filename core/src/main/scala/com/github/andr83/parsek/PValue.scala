package com.github.andr83.parsek

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

final case class PMap(value: Map[String, PValue]) extends PValue {
  type valueType = Map[String, PValue]
}

final case class PList(value: List[PValue]) extends PValue {
  type valueType = List[PValue]
}

object PValue {
  def apply(value: Any): PValue = value match {
    case lv: List[_] => PList(lv.map(apply))
    case mv: Map[_,_] => PMap(mv.filterKeys(_.isInstanceOf[String]).asInstanceOf[Map[String, Any]].mapValues(apply))
    case v: String  => PString(v)
    case v: Int  => PInt(v)
    case v: Long  => PLong(v)
    case v: Float  => PDouble(v)
    case v: Double  => PDouble(v)
    case v: PValue  => v
    case v => throw new IllegalArgumentException("Unsupported argument type: " + v.getClass.getName)
  }
}

object PList {
  def empty = PList(List.empty[PValue])
}

object PMap {
  def empty = PMap(Map.empty[String, PValue])
}

object PValueImplicits {
  implicit def strToPValue(value: String): PString = PString(value)
  implicit def intToPValue(value: Int): PInt = PInt(value)
  implicit def longToPValue(value: Long): PLong = PLong(value)
  implicit def floatToPValue(value: Float): PDouble = PDouble(value)
  implicit def doubleToPValue(value: Double): PDouble = PDouble(value)
  implicit def booleanToPValue(value: Boolean): PBool = PBool(value)
  implicit def mapToPValue(value: Map[String, PValue]): PMap = PMap(value)
  implicit def listToPValue(value: List[PValue]): PList = PList(value)
}