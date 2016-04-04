package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.util.matching.Regex

/**
  * @author andr83
  */
case class SplitPipe(
  regex: Regex,
  appendToBeginning: String = "",
  appendToEnd: String = "",
  field: FieldPath = Seq.empty[String],
  as: Option[FieldPath] = None
) extends TransformPipe(field, as) {

  def this(config: Config) = this(
    regex = config.as[String]("regex").r,
    appendToBeginning = config.as[Option[String]]("appendToBeginning").getOrElse(""),
    appendToEnd = config.as[Option[String]]("appendToEnd").getOrElse(""),
    field = config.as[Option[String]]("field").getOrElse("").asFieldPath,
    as = config.as[Option[String]]("as").map(_.asFieldPath)
  )

  override def transformString(str: String)(implicit context: PipeContext): Option[PValue] = {
    val splitted = regex.split(str)

    val list = if (appendToBeginning.nonEmpty || appendToEnd.nonEmpty) {
      var i = 0L
      val lastIndex = splitted.length - 1
      splitted map(r=> {
        var nr = r
        if (appendToBeginning.nonEmpty && i > 0) nr = appendToBeginning + nr
        if (appendToEnd.nonEmpty && i < lastIndex) nr = nr + appendToEnd
        i += 1
        PString(nr)
      })
    } else splitted.map(PString.apply)
    if (list.isEmpty) None else Some(PList(list.toList))
  }
}
