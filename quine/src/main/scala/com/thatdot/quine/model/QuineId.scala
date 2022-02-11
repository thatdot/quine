package com.thatdot.quine.model

import java.util.Arrays

import scala.util.hashing.MurmurHash3

import com.thatdot.quine.util.HexConversions
import com.thatdot.quine.util.TypeclassInstances.ByteArrOrdering

/** The internal ID type used by Quine for identifying nodes.
  *
  * @note the underlying array should not be mutated (maybe it should be a read-only bytebuffer)
  * @param array bytes that constitute the ID
  */
final case class QuineId(array: Array[Byte]) extends Ordered[QuineId] {
  override def hashCode: Int = MurmurHash3.bytesHash(array, MurmurHash3.seqSeed)

  override def equals(that: Any): Boolean = that match {
    case QuineId(other) => Arrays.equals(array, other)
    case _ => false
  }

  override def compare(other: QuineId): Int =
    ByteArrOrdering.compare(array, other.array)

  /** Print a developer-facing representation of an ID */
  def debug(implicit idProvider: QuineIdProvider): String =
    idProvider.customIdStringFromQid(this).getOrElse(toString)

  /** Print a user-facing representation of an ID (which is invertible via `qidFromPrettyString`) */
  def pretty(implicit idProvider: QuineIdProvider): String =
    idProvider.qidToPrettyString(this)

  /** The internal unambiguous string representation of the ID.
    *
    * This is always either the literal string "empty" or else a non-empty even-length string
    * containing only numbers and uppercase A-F. The choice of using "empty" instead of an empty
    * string is because we use this in places where an empty string is problematic (eg. naming
    * Akka actors).
    *
    * @see [[QuineId.fromInternalString]]
    */
  def toInternalString: String =
    if (array.isEmpty) "empty"
    else HexConversions.formatHexBinary(array)

  override def toString: String = s"QuineId(${HexConversions.formatHexBinary(array)})"
}
object QuineId {

  /** Recover an ID from a string produced by [[toInternalString]].
    *
    * @see [[QuineId.toInternalString]]
    */
  @throws[IllegalArgumentException]("if the input string is not a valid internal ID")
  def fromInternalString(str: String): QuineId =
    if (str == "empty") new QuineId(Array.emptyByteArray)
    else QuineId(HexConversions.parseHexBinary(str))
}
