package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.opencsv.CSVParser
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * @author andr83
 */
case class ParseDelimited(config: Config) extends TransformPipe(config) {

  val root: RecordField = {
    val fields = try {
      config.as[List[String]]("fields") map (f => StringField(f))
    } catch {
      case _: Throwable => config.as[List[FieldType]]("fields")
    }
    RecordField("root", fields = fields)
  }

  val validatePipe = Validate(root)

  val delimiter: Char = config.as[Option[Char]]("delimiter").getOrElse(',')
  val enclosure: Char = config.as[Option[Char]]("enclosure").getOrElse('"')
  val escape: Char = config.as[Option[Char]]("escape").getOrElse('\\')
  val multiLine: Boolean = config.as[Option[Boolean]]("multiLine").getOrElse(false)
  val nullValue: String = config.as[Option[String]]("nullValue").getOrElse("")
  val listDelimiter = config.as[Option[String]]("listDelimiter").getOrElse("\\|")
  val mapFieldDelimiter = config.as[Option[String]]("mapFieldDelimiter").getOrElse(":")

  val parser = new CSVParser(delimiter, enclosure, escape, CSVParser.DEFAULT_STRICT_QUOTES,
    CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE, CSVParser.DEFAULT_IGNORE_QUOTATIONS)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    val csvValues = if (multiLine) parser.parseLineMulti(str) else parser.parseLine(str)
    var i = -1
    val res = root.fields flatMap (f=> {
      i += 1
      parse(csvValues(i), f) map (v=> f.name -> v)
    })
    validatePipe.run(res.toMap)
  }

  def parse(str: String, field: FieldType, level: Int = 2): Option[PValue] = field match {
    case f: RecordField =>
      val map = str.split(getDelimiter(level)).flatMap(kv=> {
        val parts = kv.split(getDelimiter(level + 1))
        if (parts.size != 2) {
          logger.warn(s"Unexpected delimited map content. Expected [key,value] but get: $kv")
          None
        } else {
          Some(parts(0) -> parts(1))
        }
      }).toMap
      val res = for {
        cf <- f.fields
        cv <- map.get(cf.name)
        pv <- parse(cv, cf,  level + 2)
      } yield cf.name -> pv
      if (res.isEmpty) None else Some(res.toMap)
    case f: MapField =>
      val map = str.split(getDelimiter(level)).flatMap(kv=> {
        val parts = kv.split(getDelimiter(level + 1))
        if (parts.size != 2) {
          logger.warn(s"Unexpected delimited map content. Expected [key,value] but get: $kv")
          None
        } else {
          Some(parts(0) -> parts(1))
        }
      }).toMap
      if (map.isEmpty || f.field.isEmpty) None else {
        val res = map.mapValues(parse(_, f.field.get, level + 2)) collect {
          case (k, Some(v)) => k -> v
        }
        if (res.isEmpty) None else Some(res)
      }
    case f: ListField =>
      val parts = str.split(getDelimiter(level))
      if (parts.isEmpty) None else f.field match {
        case Some(listField) =>
          val res = parts.flatMap(parse(_, listField, level + 1))
          if (res.isEmpty) None else Some(res.toList)
        case _ => Some(parts.map(PValue.apply).toList)
      }
    case v => Some(PString(str))
  }

  def getDelimiter(level: Int): String = if (level == 1) {
    delimiter.toString
  } else if (level == 2) {
    listDelimiter
  } else if (level == 3) {
    mapFieldDelimiter
  } else if (level < 25) {
    level.toChar.toString
  } else throw new IllegalStateException("Exceed the maximum level 24 of nesting for csv serializer")
}
