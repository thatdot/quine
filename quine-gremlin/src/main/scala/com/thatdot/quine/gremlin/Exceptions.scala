package com.thatdot.quine.gremlin

import scala.util.parsing.input.Position

import com.thatdot.quine.util.HexConversions

/** The sort of exceptions that can be thrown when interpretting Gremlin queries.
  *
  * This does not mean these are the only exceptions with which queries can fail,
  * since other exceptions could be percolating up from Quine. However, these are
  * the only exceptions that the Gremlin adapter to Quine throws.
  */
sealed abstract class QuineGremlinException extends Exception {

  /** Location of the error in the initial query string */
  val position: Option[Position]

  def pretty: String

  protected def errorString(errName: String, errMessage: String): String = {
    val positionStr = position.fold("")(" at " + _)
    val caretLocation = position.fold("")(p => s"\n${p.longString}")
    errName + positionStr + ": " + errMessage + "\n" + caretLocation
  }
}

/** Since Gremlin queries are untyped, type errors occur during execution (for example
  * when a number is handed to a predicate that only knows how to handle strings).
  *
  * @param expected the type that was expected
  * @param actual the type that was received instead
  * @param offender the thing that had the wrong type
  * @param explanation justification for the type requirement
  * @param position from where in the query did the error originate
  */
final case class TypeMismatchError(
  expected: Class[_],
  actual: Class[_],
  offender: Any,
  explanation: String,
  position: Option[Position]
) extends QuineGremlinException {
  def pretty: String = errorString(
    "TypeMismatchError",
    s"$explanation\n" +
    s"  expected $expected\n" +
    s"  but got  $actual"
  )
}

/** Errors that occur during the lexing stage
  *
  * @param position where in the query did lexing fail
  */
final case class LexicalError(
  pos: Position
) extends QuineGremlinException {
  val position: Some[Position] = Some(pos)
  def pretty: String = errorString(
    "LexicalError",
    "syntax error"
  )
}

/** Errors that occur during the parsing stage
  *
  * @param message description of what was unexpected
  * @param position where in the query did parsing fail
  */
final case class ParseError(
  message: String,
  position: Option[Position]
) extends QuineGremlinException {
  def pretty: String = errorString(
    "ParseError",
    message
  )
}

/** Errors that come from trying to evaluate a variable which has not yet defined. This includes
  * both query-level variables (example: `x1 = 7; g.V(x2)`) and traversal-level variables (example:
  * `g.V().as("x1").out().select("x2")`)
  *
  * @param variableName variable that was not in scope
  * @param position where in the query was this variable accessed
  */
final case class UnboundVariableError(
  variable: Symbol,
  position: Option[Position]
) extends QuineGremlinException {
  def pretty: String = errorString(
    "UnboundVariableError",
    s"${variable.name} is unbound"
  )
}

/** Errors that occur during deserialization of properties.
  *
  * @param property Gremlin name of the property whose value could not be deserialized
  * @param rawBytes raw bytes of the value, which could not be deserialized
  * @param position where in the query did deserialization fail
  */
final case class FailedDeserializationError(
  property: String,
  rawBytes: Array[Byte],
  position: Option[Position]
) extends QuineGremlinException {
  def pretty: String =
    errorString(
      "FailedDeserializationError",
      s"property `$property` could not be unpickled.\n" +
      s"  Raw bytes: ${HexConversions.formatHexBinary(rawBytes)}"
    )
}
