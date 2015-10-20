package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config

/**
 * @author andr83
 */
abstract class ParserPipe(config: Config) extends Pipe {
  override def run(value: PValue): Option[PValue] = value match {
    case PString(raw) => parse(raw)
    case _ => throw new IllegalArgumentException(
      s"Parser pipe accept only string input but ${value.getClass} given. Value: $value"
    )
  }

  def parse(raw: String): Option[PValue]
}