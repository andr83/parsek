package com.github.andr83.parsek

import com.github.andr83.parsek.meta.MapField

/**
 * @author nik
 */
class PipeContext extends Serializable {
  protected var counters = Map.empty[(String, String), IntCounter]
  var schema: Option[MapField] = None
  var row = PMap.empty
  var path = Seq.empty[String]

  def getCounters: Map[(String, String), Int] = counters.mapValues(_.count)

  def getCounter(groupName: String, name: String): IntCounter = {
    val key = (groupName, name)
    if (!counters.contains(key)) {
      counters += key -> new IntCounter()
    }
    counters.get(key).get
  }
}

object PipeContext {
  val ErrorGroup = "ERRORS"
  val WarnGroup = "WARNS"
  val InfoGroup = "INFO"
  val EmptyField = "EMPTY_FIELD"
}