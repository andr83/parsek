package com.github.andr83.parsek.serde

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.DateFormatter

/**
  * @author andr83
  */
case class StringSerializer(timeFormatter: DateFormatter) extends Serializer {

  lazy val jsonSerDe = JsonSerDe(fields=None, timeFormatter)

  override def write(value: PValue): Array[Byte] = value match {
    case _: PMap => jsonSerDe.write(value)
    case _: PList => jsonSerDe.write(value)
    case _ => value.value.toString.asBytes
  }
}

object StringSerializer {
  lazy val factory = () => StringSerializer(DateFormatter(None))
}
