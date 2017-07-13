package com.github.andr83.parsek.util

import com.github.andr83.parsek.PValue
import com.twitter.util.Eval

import scala.reflect.ClassTag

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

  def compileTransformFromTo[From: ClassTag, To](mapCode: String): Function1[From, To] = {
    val clazz = implicitly[ClassTag[From]].runtimeClass
    val additionalImport = {
      if (!clazz.isPrimitive && !clazz.isArray) {
        s"import ${clazz.getName}"
      } else ""
    }
    val code =
      s"""
         |import com.github.andr83.parsek._
         |$additionalImport
         |
         |(v: ${clazz.getName}) => {
         |$mapCode
         |}
      """.stripMargin

    new Eval().apply[From => To](code)
  }

  def compileTransformStringFn(filterCode: String): String => PValue = {
    val code =
      s"""
         |import com.github.andr83.parsek._
         |
         |(v: String) => {
         |$filterCode
         |}
      """.stripMargin
    new Eval().apply[String => PValue](code)
  }
}
