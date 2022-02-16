package com.thatdot.quine.persistor

import scala.annotation.implicitNotFound
import scala.util.Try

/** Provides binary serialization and deserialization for type T */
@implicitNotFound(msg = "Cannot find BinaryFormat type class for ${T}")
abstract class BinaryFormat[T] {
  def read(bytes: Array[Byte]): Try[T]
  def write(obj: T): Array[Byte]
}
