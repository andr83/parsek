package com.github.andr83.parsek.formatter

import com.github.andr83.parsek._
import com.typesafe.config.{ConfigException, _}
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime

/**
  * PValue formatter to string instance. Can be used for partitions as example.
  * @author andr83
  */
trait ValueFormatter extends Serializable {
  def apply(value: PValue): String
}

/**
  * Simple string formatter
  */
object StringFormatter extends ValueFormatter {
  override def apply(value: PValue): String = value.value.toString
}

/**
  * Format PDate or PLong (as timestamp) by timeFormat spec
  *
  * @param timeFormat DateFormatter specification
  */
case class TimeFormatter(timeFormat: Option[String]) extends ValueFormatter {
  def this(config: Config) = this(
    timeFormat = config.as[Option[String]]("timeFormat")
  )

  lazy val timeFormatter = DateFormatter(timeFormat)

  override def apply(value: PValue): String = value match {
    case v: PDate => timeFormatter.format(v.value).value.toString
    case v: PLong => timeFormatter.format(new DateTime(v.value)).value.toString
    case _ => throw new IllegalStateException(s"Invalid date partition value $value")
  }
}

object ValueFormatter {
  def apply(config: Config): ValueFormatter = {
    val partitionType = config.as[Option[String]]("type")
      .getOrElse(throw new IllegalStateException("Partition config should have type property"))
    val className = if (partitionType.contains(".")) partitionType
    else
      "com.github.andr83.parsek.formatter." + partitionType.head.toUpper + partitionType.substring(1) + "Formatter"
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(config).asInstanceOf[ValueFormatter]
  }
}

case class FieldFormatter(field: FieldPath, formatter: ValueFormatter, as: Option[FieldPath] = None) {
  def asField: FieldPath = as.getOrElse(field)
  def apply(record: PMap): String =  record.getValue(field).map(formatter.apply).getOrElse("null")
}

object FieldFormatter {
  def apply(configs: Seq[ConfigValue]): Seq[FieldFormatter] = configs map (configValue=> configValue.valueType() match {
    case ConfigValueType.STRING => FieldFormatter(configValue.unwrapped().asInstanceOf[String].split('.'), StringFormatter)
    case ConfigValueType.OBJECT =>
      val config = configValue.asInstanceOf[ConfigObject].toConfig
      val field = config.as[String]("field").split('.')
      val as = config.as[Option[String]]("as").map(_.asFieldPath)
      FieldFormatter(field, ValueFormatter(config), as)
    case v => throw new ConfigException.Parse(configValue.origin(), s"Can not instantiate FieldFormatter from $v config")
  })
}