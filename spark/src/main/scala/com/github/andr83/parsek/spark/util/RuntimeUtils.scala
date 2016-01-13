package com.github.andr83.parsek.spark.util

import com.github.andr83.parsek.PValue
import com.twitter.util.Eval

/**
  * @author andr83
  */
object RuntimeUtils {

  def compileFilterFn(filterCode: String): PValue => Boolean = {
    val code =
      s"""
         |import com.github.andr83.parsek._
         |
         |(v: PValue) => {
         |$filterCode
         |}
      """.stripMargin
    new Eval().apply[PValue => Boolean](code)
  }
}
