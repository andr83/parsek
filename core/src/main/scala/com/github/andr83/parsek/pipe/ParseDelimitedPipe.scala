package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.DateFormatter
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.serde.DelimitedSerDe
import com.github.andr83.parsek.serde.DelimitedSerDeTrait._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class ParseDelimitedPipe(
  field: FieldPath,
  fields: Seq[FieldType],
  as: Option[FieldPath],
  enclosure: Char = DefaultEnclosure,
  escape: Char = DefaultEscape,
  delimiter: Char = DefaultDelimiter,
  listDelimiter: Char = DefaultListDelimiter,
  mapFieldDelimiter: Char = DefaultMapFieldDelimiter,
  nullValue: String = DefaultNullValue,
  multiLine: Boolean = DefaultMultiLine,
  timeFormatter: DateFormatter = DateFormatter(DefaultTimeFormat)
) extends TransformPipe(field=field, as=as) {

  def this(config: Config) = this(
    field = config.as[String]("field").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath),
    fields = try {
      config.as[List[String]]("fields") map (f => StringField(f))
    } catch {
      case _: Throwable => config.as[List[FieldType]]("fields")
    },
    enclosure = config.as[Option[Char]]("enclosure").getOrElse(DefaultEnclosure),
    escape = config.as[Option[Char]]("escape").getOrElse(DefaultEscape),
    delimiter = config.as[Option[Char]]("delimiter").getOrElse(DefaultDelimiter),
    listDelimiter = config.as[Option[Char]]("listDelimiter").getOrElse(DefaultListDelimiter),
    mapFieldDelimiter = config.as[Option[Char]]("mapFieldDelimiter").getOrElse(DefaultMapFieldDelimiter),
    nullValue = config.as[Option[String]]("nullValue").getOrElse(DefaultNullValue),
    multiLine = config.as[Option[Boolean]]("multiLine").getOrElse(DefaultMultiLine),
    timeFormatter = DateFormatter(config.as[Option[String]]("timeFormat"))
  )

  val root: RecordField = RecordField("root", fields = fields)

  val parser = new DelimitedSerDe(
    fields = fields,
    enclosure = enclosure,
    escape = escape,
    delimiter = delimiter,
    listDelimiter = listDelimiter,
    mapFieldDelimiter = mapFieldDelimiter,
    nullValue = nullValue,
    multiLine = multiLine,
    timeFormatter = timeFormatter
  )
  val validatePipe = ValidatePipe(root)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    val res = parser.read(str)
    validatePipe.run(res)
  }
}
