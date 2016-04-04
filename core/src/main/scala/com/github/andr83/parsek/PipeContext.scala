package com.github.andr83.parsek

import com.github.andr83.parsek.meta.RecordField

/**
 * @author nik
 */
@SerialVersionUID(1L)
class PipeContext extends Serializable {
  protected var counters = Map.empty[(String, String), LongCounter]
  var schema: Option[RecordField] = None
  var row = PMap.empty
  var path = Seq.empty[String]

  def getCounters: Map[(String, String), Long] = counters.mapValues(_.count)

  def getCounter(groupName: String, name: String): LongCounter = {
    val key = (groupName, name)
    if (!counters.contains(key)) {
      counters += key -> new LongCounter()
    }
    counters.get(key).get
  }
}

object PipeContext {
  val ErrorGroup = "ERRORS"
  val WarnGroup = "WARNS"
  val InfoGroup = "INFO"
  val InputRowsGroup = "INPUT_ROWS"
  val OutputRowsGroup = "OUTPUT_ROWS"
  val EmptyField = "EMPTY_FIELD"
}