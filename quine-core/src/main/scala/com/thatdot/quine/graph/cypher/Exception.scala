package com.thatdot.quine.graph.cypher

import scala.jdk.CollectionConverters._

/** Generic trait for all sorts of Cypher-related exceptions */
sealed abstract class CypherException extends Exception with Product {
  val position: Option[Position]

  def positionStr: String = position
    .fold("")(pos => s"at ${pos.row}.${pos.column} ")

  /** Construct a pretty error message, which include the type of error, the
    * position where it occurred (if we have that), the error message, then
    * a the line which contains the error position (if we have that)
    */
  def pretty: String = {
    val caretMessageOpt = position.fold("") { pos =>
      val lines = "[^\\n]*(\\n|$)".r.findAllIn(pos.source.text)
      var line = lines.next()
      var sourceIdx = line.length
      while (sourceIdx < pos.offset && lines.hasNext) {
        line = lines.next()
        sourceIdx += line.length
      }

      "\n\n" + line.stripSuffix("\n") + "\n" + (" " * (pos.column - 1)) + "^"
    }
    productPrefix + "Error " + positionStr + getMessage() + caretMessageOpt
  }
}

final case class SourceText(text: String)

// TODO: get a position on all exceptions
final case class Position(
  row: Int,
  column: Int,
  offset: Int,
  source: SourceText
)

object Position {
  def apply(row: Int, column: Int, offset: Int)(implicit
    source: SourceText,
    dummy: DummyImplicit
  ): Position =
    Position(row, column, offset, source)
}

object CypherException {

  /** Runtime mismatch in expected types compared to actual type
    *
    * @param expected all of the types that would have been valid
    * @param actualValue the value that was instead passed
    * @param context human summary of where this happened
    * @param position cursor to the error in the query text
    */
  final case class TypeMismatch(
    expected: Seq[Type],
    actualValue: Value,
    context: String,
    position: Option[Position] = None
  ) extends CypherException {

    /** Actual type */
    def actual: Type = actualValue.typ

    override def getMessage(): String = {
      val expectedTys = expected.map(_.toString).mkString(", ")
      val actualVal = actualValue.pretty
      val actualTy = actual.toString
      s"Expected type(s) $expectedTys but got value $actualVal of type $actualTy in $context"
    }
  }

  final case class NoSuchField(
    desiredField: String,
    presentFields: Set[String],
    context: String,
    position: Option[Position] = None
  ) extends CypherException {
    override def getMessage: String =
      s"Field $desiredField not present in ${presentFields.mkString("[", ", ", "]")} in $context"
  }

  /** An index is invalid, because it exceeds the bounds of a 32-bit Java index
    *
    * @param index overflowing/underflowing index value
    * @param position cursor to the error in the query text
    */
  final case class InvalidIndex(
    index: Value,
    position: Option[Position] = None
  ) extends CypherException {
    override def getMessage(): String =
      s"${index.pretty} cannot be used as an index"
  }

  /** Some low level arithmetic operation failed
    *
    * These include: overflows, underflows, and division by zero.
    *
    * @param wrapping the message from the underlying Java error
    * @param lhs the left operand
    * @param rhs the right operand
    * @param position cursor to the error in the query text
    */
  final case class Arithmetic(
    wrapping: String,
    operands: Seq[Expr.Number],
    position: Option[Position] = None
  ) extends CypherException {
    override def getMessage(): String = {
      val operandsString = operands.map(_.string).mkString(", ")
      s"Arithmetic $wrapping with operands $operandsString"
    }
  }

  /** Generic runtime exception for errors which don't fit in the other more
    * specific exception classes.
    *
    * @param message error message
    * @param position cursor to the error in the query text
    */
  final case class Runtime(
    message: String,
    position: Option[Position] = None
  ) extends CypherException {
    override def getMessage(): String = message
  }

  /** Some run-time constraint was violated
    *
    * @param message description of the constraint and why it was violated
    * @param position cursor to the error in the query text
    */
  final case class ConstraintViolation(
    message: String,
    position: Option[Position] = None
  ) extends CypherException {
    override def getMessage(): String = message
  }

  /** Compile-time error
    *
    * These mostly just come straight from `openCypher`
    *
    * @param wrapping the message from the underlying error
    * @param position cursor to the error in the query text
    */
  final case class Compile(
    wrapping: String,
    position: Option[Position]
  ) extends CypherException {
    override def getMessage(): String = wrapping
  }

  /** Compile-time syntax exception
    *
    * Comes from `openCypher`. The string message comes with the position in it,
    * which is not what we want. As a workaround, we use a regex to get rid of
    * the position in the string message.
    *
    * @param wrapping the message from the underlying error
    * @param position cursor to the error in the query text
    */
  final case class Syntax(
    wrapping: String,
    position: Option[Position]
  ) extends CypherException {
    private val stripPos = raw" \(line \d+, column \d+ \(offset: \d+\)\)$$".r
    override def getMessage(): String = stripPos.replaceAllIn(wrapping, "")
  }

  /** Function or procedure got called with the wrong arguments
    *
    * @param expectedSignature expected signature of the function or procedure
    * @param actualArguments actual arguments received
    * @param position cursor to the error in the query text
    */
  final case class WrongSignature(
    expectedSignature: String,
    actualArguments: Seq[Value],
    position: Option[Position]
  ) extends CypherException {
    override def getMessage(): String = {
      val actual = actualArguments.map(_.pretty).mkString(", ")
      s"Expected signature `$expectedSignature` but got arguments $actual"
    }
  }
  object WrongSignature {

    /** Function or procedure got called with the wrong arguments
      *
      * @param calledName name of the function or procedure
      * @param expectedArguments expected types of arguments
      * @param actualArguments actual arguments received
      * @param position cursor to the error in the query text
      */
    def apply(
      calledName: String,
      expectedArguments: Seq[Type],
      actualArguments: Seq[Value]
    ): WrongSignature =
      WrongSignature(s"$calledName(${expectedArguments.mkString(", ")})", actualArguments, None)
  }

  /** Java API: function or procedure is called with the wrong arguments
    *
    * @see [[WrongSignature]]
    */
  def wrongSignature(
    calledName: String,
    expectedArguments: java.lang.Iterable[Type],
    actualArguments: java.lang.Iterable[Value]
  ): WrongSignature =
    WrongSignature(
      calledName,
      expectedArguments.asScala.toSeq,
      actualArguments.asScala.toSeq
    )
}
