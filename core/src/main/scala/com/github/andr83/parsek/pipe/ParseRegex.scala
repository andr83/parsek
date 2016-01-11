package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.github.andr83.parsek.util.RegexUtils
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.matching.Regex

/**
  * Parse string by regular expression and map pattern groups to PMap
  *
  * @param pattern regex with groups
  * @param field path to parse regex
  * @param as path to store as
  *
  * @author andr83
  */
case class ParseRegex(
  pattern: Regex,
  field: FieldPath = Seq.empty[String],
  as: Option[FieldPath] = None
) extends TransformPipe(field, as) {

  def this(config: Config) = this(
    pattern = config.as[String]("pattern").r,
    field = config.as[Option[String]]("field").getOrElse("").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  val namedGroups = RegexUtils.getNamedGroups(pattern)

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = for (
    m <- pattern.findFirstMatchIn(str)
    if m.groupCount > 0
  ) yield {
    val map = for (
      (name, idx) <- namedGroups
    ) yield name -> PString(m.group(idx))
    PMap(map.toMap)
  }
}

object ParseRegex {

  def apply(config: Config): ParseRegex = new ParseRegex(config)
}