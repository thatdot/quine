package com.thatdot.quine.graph.cypher

import scala.jdk.CollectionConverters._

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.model.QuineIdProvider

/** Cypher user defined functions (UDF) must extend this class
  *
  * @note instances of [[UserDefinedFunction]] may be re-used for multiple
  * (possibly concurrent) function calls
  */
abstract class UserDefinedFunction {

  /** What is the name of the UDF */
  def name: String

  /** Category of functionality this function falls under.
    * Implementation note: this should be one of the values of [[org.opencypher.v9_0.expressions.functions.Category$]]
    */
  def category: String

  /** Is this a pure function? A pure function satisfies all of:
    *
    * - Returns a value that is fully computed from the function parameter
    *   (therefore the same arguments always produce the same result)
    *
    * - Does not read or write any non-local state
    *
    * - Does not cause side effects
    */
  def isPure: Boolean

  /** How to call the UDF
    *
    * Implementation note: the canonical way to reject an execution due to a conflict between [[signatures]]
    * and `arguments` is `throw wrongSignature(arguments)`
    *
    * @param arguments arguments passed into the UDF (after they've been evaluated)
    * @param idProvider ID provider
    * @return output value of the UDF
    */
  @throws[CypherException]
  def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Value

  /** Signature of the function
    *
    * These only get used when compiling the UDF in a query to make sure it isn't
    * called with an obviously incorrect number of arguments or type of argument.
    */
  def signatures: Seq[UserDefinedFunctionSignature]

  /** Construct a wrong signature error based on the first signature in [[signatures]]
    *
    * @param actualArguments actual arguments received
    * @return exception representing the mismatch
    */
  final protected def wrongSignature(actualArguments: Seq[Value]): CypherException.WrongSignature =
    CypherException.WrongSignature(signatures.head.pretty(name), actualArguments, None)
}

/** Java API: Cypher user defined functions (UDF) must extend this class
  *
  * @see UserDefinedFunction
  */
abstract class JavaUserDefinedFunction(
  override val name: String,
  udfSignatures: java.lang.Iterable[UserDefinedFunctionSignature],
) extends UserDefinedFunction {

  /** Java API: How to call the UDF
    *
    * @param arguments arguments passed into the UDF (after they've been evaluated)
    * @param idProvider ID provider
    * @return output value of the UDF
    */
  @throws[CypherException]
  def call(arguments: java.util.List[Value], idProvider: QuineIdProvider): Value

  final override def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Value =
    call(arguments.asJava, idProvider)

  final override def signatures = udfSignatures.asScala.toVector
}

/** Representation of a valid type for the function
  *
  * @param arguments name of the arguments and their types
  * @param output output type
  * @param description explanation of what this overload of the UDF does
  */
final case class UserDefinedFunctionSignature(
  arguments: Seq[(String, Type)],
  output: Type,
  description: String,
) {

  /** Pretty-print the signature
    *
    * @note this is defined to match the openCypher spec as much as possible
    */
  def pretty(name: String): String = {
    val inputsStr = arguments.view
      .map { case (name, typ) => s"$name :: ${typ.pretty}" }
      .mkString(", ")

    s"$name($inputsStr) :: ${output.pretty}"
  }
}
object UserDefinedFunctionSignature {

  /** Java API: make a function signature
    *
    * @param arguments name of the argument and its type
    * @param output output type
    * @param description explanation of what this overload of the UDF does
    */
  def create(
    arguments: java.lang.Iterable[Argument],
    output: Type,
    description: String,
  ): UserDefinedFunctionSignature =
    apply(arguments.asScala.map(a => (a.name, a.input)).toSeq, output, description)
}

/** Input argument to a UDF
  *
  * @param name what is the argument called
  * @param input what is its input type
  */
final case class Argument(
  name: String,
  input: Type,
)
