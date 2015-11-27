package com.github.andr83.parsek

/**
 * @author andr83
 */
package object meta {
  type FieldType = Field[_ <: PValue]
  type FieldError = (FieldType, Throwable)
}
