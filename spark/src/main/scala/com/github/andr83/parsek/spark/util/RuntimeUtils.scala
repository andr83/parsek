package com.github.andr83.parsek.spark.util

import com.github.andr83.parsek.PValue
import com.twitter.util.Eval

/**
  * @author andr83
  */
object RuntimeUtils {

  def compileFilterFn(filterCode: String): PValue => Boolean = compileTransformFn(filterCode)

  def compileTransformFn[T](filterCode: String): T = {
    val code =
      s"""
         |import com.github.andr83.parsek._
         |
         |(v: PValue) => {
         |$filterCode
         |}
      """.stripMargin
    new Eval().apply[T](code)
  }
}
