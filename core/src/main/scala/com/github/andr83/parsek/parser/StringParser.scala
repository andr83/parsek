package com.github.andr83.parsek.parser

import com.github.andr83.parsek.ParsePipe.Parser
import com.github.andr83.parsek._

/**
 * @author andr83
 */
class StringParser (implicit val context: PipeContext) extends Parser {
  override def parse(source: PValue): PValue = source match {
    case PString(raw) => parseString(raw)
    case _ => throw new IllegalArgumentException(
      s"Json parser accept only string input but ${source.getClass} given. Message: $source"
    )
  }

  def parseString(source: String): PValue = PString(source)
}

object StringParser {
  def apply()(implicit context: PipeContext): StringParser = new StringParser()
}
