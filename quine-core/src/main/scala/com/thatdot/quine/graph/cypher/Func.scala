package com.thatdot.quine.graph.cypher

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import scala.collection.concurrent
import scala.jdk.CollectionConverters._

import com.thatdot.quine.model.QuineIdProvider

/** Scalar Cypher function
  *
  * TODO: thread in type signatures and error messages to all of these
  */
sealed abstract class Func {

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

  /** Name that uniquely identifies the function */
  def name: String

  /** Call the function
    *
    * The function should expect arguments matching the specified type signature
    *
    * @param args arguments to the function
    * @return the output
    */
  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value
}

/** Built-in Cypher function (aka. functions that are hardwired into the language)
  *
  * @param name name of the function
  * @param isPure return computed from parameters, and function does not access other state, or cause side-effects
  * @param description explanation of what the function does
  * @param signature type of the function
  */
sealed abstract class BuiltinFunc(
  val name: String,
  val isPure: Boolean,
  val description: String,
  val signature: String
) extends Func {

  /** Construct a wrong signature error based on the first signature in [[signatures]]
    *
    * @param actualArguments actual arguments received
    * @return exception representing the mismatch
    */
  final protected def wrongSignature(actualArguments: Seq[Value]): CypherException.WrongSignature =
    CypherException.WrongSignature(name + signature, actualArguments, None)
}

object Func {

  /** Custom user defined functions which are registered at runtime.
    * Keys must be lowercase!
    *
    * @note this must be kept in sync across the entire logical graph
    */
  final val userDefinedFunctions: concurrent.Map[String, UserDefinedFunction] =
    new ConcurrentHashMap[String, UserDefinedFunction]().asScala

  /** Built-in functions which we can count on always being available */
  // format: off
  final val builtinFunctions: Vector[BuiltinFunc] = Vector(
    Abs, Acos, Asin, Atan, Atan2, Ceil, Coalesce, Cos, Cot, Degrees, E, Exp, Floor, Haversin, Head,
    Id, Keys, Labels, Last, Left, Length, Log, Log10, LTrim, Nodes, Pi, Properties, Rand, Range,
    Relationships, Replace, Reverse, Right, RTrim, Sign, Sin, Size, Split, Sqrt, Substring, Tail,
    Tan, Timestamp, ToBoolean, ToFloat, ToInteger, ToLower, ToUpper, ToString, Trim, Type
  )

  case object Abs extends BuiltinFunc(
    name = "abs",
    isPure = true,
    description = "absolute value of a number",
    signature = "(NUMBER?) :: NUMBER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Integer(lng)) => Expr.Integer(Math.abs(lng))
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.abs(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Acos extends BuiltinFunc(
    name = "acos",
    isPure = true,
    description = "arcosine (in radians) of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.acos(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Asin extends BuiltinFunc(
    name = "asin",
    isPure = true,
    description = "arcsine (in radians) of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.asin(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Atan extends BuiltinFunc(
    name = "atan",
    isPure = true,
    description = "arctangent (in radians) of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.atan(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Atan2 extends BuiltinFunc(
    name = "atan2",
    isPure = true,
    description = "arctangent (in radians) of the quotient of its arguments",
    signature = "(NUMBER?, NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl1), Expr.Number(dbl2)) =>
          Expr.Floating(Math.atan2(dbl1, dbl2))
        case other => throw wrongSignature(other)
      }
  }

  case object Ceil extends BuiltinFunc(
    name = "ceil",
    isPure = true,
    description = "smallest integer greater than or equal to the input",
    signature = "(NUMBER?) :: NUMBER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(int: Expr.Integer) => int
        case Vector(Expr.Floating(dbl)) => Expr.Floating(Math.ceil(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Coalesce extends BuiltinFunc(
    "coalesce",
    isPure = true,
    "returns the first non-`null` value in a list of expressions",
    "(ANY?, ..) :: ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args.find(_ != Expr.Null).getOrElse(Expr.Null)
  }

  case object Cos extends BuiltinFunc(
    name = "cos",
    isPure = true,
    description = "cosine of a number of radians",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.cos(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Cot extends BuiltinFunc(
    name = "cot",
    isPure = true,
    description = "cotangent of a number of radians",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Floating(dbl)) => Expr.Floating(1.0 / Math.tan(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Degrees extends BuiltinFunc(
    name = "degrees",
    isPure = true,
    description = "convert radians to degrees",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Floating(dbl)) => Expr.Floating(Math.toDegrees(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object E extends BuiltinFunc(
    "e",
    isPure = true,
    "mathematical constant `e`",
    "() :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector() => Expr.Floating(Math.E)
        case other => throw wrongSignature(other)
      }
  }

  case object Exp extends BuiltinFunc(
    name = "exp",
    isPure = true,
    description = "return the mathematical constant `e` raised to the power of the input",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.exp(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Floor extends BuiltinFunc(
    name = "floor",
    isPure = true,
    description = "largest integer less than or equal to the input",
    signature = "(NUMBER?) :: NUMBER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(int: Expr.Integer) => int
        case Vector(Expr.Floating(dbl)) => Expr.Floating(Math.floor(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Haversin extends BuiltinFunc(
    name = "haversin",
    isPure = true,
    description = "half the versine of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Floating(dbl)) =>
          Expr.Floating({
            val sin = Math.sin(dbl / 2.0)
            sin * sin
          })
        case other => throw wrongSignature(other)
      }
  }

  case object Head extends BuiltinFunc(
    name = "head",
    isPure = true,
    description = "extract the first element of a list",
    signature = "(LIST? OF ANY?) :: ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(Vector())) => Expr.Null
        case Vector(Expr.List(nonEmptyVec)) => nonEmptyVec.head
        case other => throw wrongSignature(other)
      }
  }

  case object Id extends BuiltinFunc(
    name = "id",
    isPure = true,
    description = "extract the ID of a node",
    signature = "(NODE?) :: ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Node(qid, _, _)) => Expr.fromQuineValue(idp.qidToValue(qid))
        case other => throw wrongSignature(other)
      }
  }

  case object Keys extends BuiltinFunc(
    name = "keys",
    isPure = true,
    description = "extract the keys from a map, node, or relationship",
    signature = "(ANY?) :: LIST? OF STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Map(map)) =>
          Expr.List(map.keys.map(k => Expr.Str(k)).toVector)
        case Vector(Expr.Node(_, _, map)) =>
          Expr.List(map.keys.map(k => Expr.Str(k.name)).toVector)
        case Vector(Expr.Relationship(_, _, map, _)) =>
          Expr.List(map.keys.map(k => Expr.Str(k.name)).toVector)
        case other => throw wrongSignature(other)
      }
  }

  case object Labels extends BuiltinFunc(
    name = "labels",
    isPure = true,
    description = "extract the labels of a node or relationship",
    signature = "(ANY?) :: LIST? OF STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Node(_, lbls, _)) =>
          Expr.List(lbls.map(lbl => Expr.Str(lbl.name)).toVector)
        case Vector(Expr.Relationship(_, lbl, _, _)) =>
          Expr.List(Vector(Expr.Str(lbl.name)))
        case other => throw wrongSignature(other)
      }
  }

  case object Last extends BuiltinFunc(
    "last",
    isPure = true,
    "extract the last element of a list",
    "(LIST? OF ANY?) :: ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(lst)) => lst.lastOption.getOrElse(Expr.Null)
        case other => throw wrongSignature(other)
      }
  }

  case object Left extends BuiltinFunc(
    name = "left",
    isPure = true,
    description = "string containing the specified number of leftmost characters of the original string",
    signature = "(STRING?, INTEGER?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str), Expr.Integer(n)) => Expr.Str(str.take(n.toInt))
        case other => throw wrongSignature(other)
      }
  }

  // Note: non-path case is deprecated
  case object Length extends BuiltinFunc(
    name = "length",
    isPure = true,
    description = "length of a path (ie. the number of relationships in it)",
    signature = "(PATH?) :: INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(lst)) => Expr.Integer(lst.length.toLong)
        case Vector(Expr.Str(str)) => Expr.Integer(str.length().toLong)
        case Vector(Expr.Path(_, t)) => Expr.Integer(t.length.toLong)
        case other => throw wrongSignature(other)
      }
  }

  case object Log extends BuiltinFunc(
    name = "log",
    isPure = true,
    description = "natural logarithm of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.log(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Log10 extends BuiltinFunc(
    "log10",
    isPure = true,
    "common logarithm (base 10) of a number",
    "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Floating(dbl)) => Expr.Floating(Math.log10(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object LTrim extends BuiltinFunc(
    name = "lTrim",
    isPure = true,
    description = "original string with leading whitespace removed",
    signature = "(STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Str(str.replaceAll("^\\s+", ""))
        case other => throw wrongSignature(other)
      }
  }

  case object Nodes extends BuiltinFunc(
    name = "nodes",
    isPure = true,
    description = "extract a list of nodes in a path",
    signature = "(PATH?) :: LIST? OF NODE?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Path(h, t)) => Expr.List(h +: t.map(_._2))
        case other => throw wrongSignature(other)
      }
  }

  case object Pi extends BuiltinFunc(
    name = "pi",
    isPure = true,
    description = "mathematical constant `π`",
    signature = "() :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector() => Expr.Floating(Math.PI)
        case other => throw wrongSignature(other)
      }
  }

  case object Properties extends BuiltinFunc(
    name = "properties",
    isPure = true,
    description = "extract the properties from a map, node, or relationship",
    signature = "(ANY?) :: MAP?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(map: Expr.Map) => map
        case Vector(Expr.Node(_, _, map)) =>
          Expr.Map(map.map(kv => kv._1.name -> kv._2))
        case Vector(Expr.Relationship(_, _, map, _)) =>
          Expr.Map(map.map(kv => kv._1.name -> kv._2))
        case other => throw wrongSignature(other)
      }
  }
  case object Rand extends BuiltinFunc(
    name = "rand",
    isPure = false, // the returned value is random
    description = "random float between 0 (inclusive) and 1 (exclusive)",
    signature = "() :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector() => Expr.Floating(scala.util.Random.nextDouble())
        case other => throw wrongSignature(other)
      }
  }

  case object Range extends BuiltinFunc(
    name = "range",
    isPure = true,
    description = "construct a list of integers representing a range",
    signature = "(start :: INTEGER?, end :: INTEGER, step :: INTEGER) :: LIST? OF INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Integer(start), Expr.Integer(end)) =>
          val range = collection.immutable.Range.inclusive(start.toInt, end.toInt)
          Expr.List(range.map((i: Int) => Expr.Integer(i.toLong)).toVector)
        case Vector(Expr.Integer(start), Expr.Integer(end), Expr.Integer(step)) =>
          val range = collection.immutable.Range.inclusive(start.toInt, end.toInt, step.toInt)
          Expr.List(range.map((i: Int) => Expr.Integer(i.toLong)).toVector)
        case other => throw wrongSignature(other)
      }
  }

  case object Relationships extends BuiltinFunc(
    name = "relationships",
    isPure = true,
    description = "extract a list of relationships in a path",
    signature = "(PATH?) :: LIST? OF RELATIONSHIP?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Path(_, t)) => Expr.List(t.map(_._1))
        case other => throw wrongSignature(other)
      }
  }

  case object Replace extends BuiltinFunc(
    name = "replace",
    isPure = true,
    description = "replace every occurrence of a target string",
    signature = "(original :: STRING?, target :: STRING?, replacement :: STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(original), Expr.Str(search), Expr.Str(replace)) =>
          Expr.Str(original.replace(search, replace))
        case other => throw wrongSignature(other)
      }
  }

  case object Reverse extends BuiltinFunc(
    name = "reverse",
    isPure = true,
    description = "reverse a string or list",
    signature = "(ANY?) :: ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(lst)) => Expr.List(lst.reverse)
        case Vector(Expr.Str(str)) => Expr.Str(str.reverse)
        case other => throw wrongSignature(other)
      }
  }

  case object Right extends BuiltinFunc(
    name = "right",
    isPure = true,
    description = "string containing the specified number of rightmost characters of the original string",
    signature = "(STRING?, INTEGER?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str), Expr.Integer(n)) => Expr.Str(str.takeRight(n.toInt))
        case other => throw wrongSignature(other)
      }
  }

  case object RTrim extends BuiltinFunc(
    name = "rTrim",
    isPure = true,
    description = "original string with trailing whitespace removed",
    signature = "(STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Str(str.replaceAll("\\s+$", ""))
        case other => throw wrongSignature(other)
      }
  }

  /* Edge cases for floating points:
   *
   *   - `sign(NaN) = sign(0.0) = sign(-0.0) = 0`
   *   - `sign(-Infinity) = sign(-3.1) = -1`
   *   - `sign(+Infinity) = sign(3.1)  = 1`
   */
  case object Sign extends BuiltinFunc(
    name = "sign",
    isPure = true,
    description = "signum of a number",
    signature = "(NUMBER?) :: INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Integer(lng)) => Expr.Integer(java.lang.Long.signum(lng).toLong)
        case Vector(Expr.Floating(dbl)) => Expr.Integer(Math.signum(dbl).toLong)
        case other => throw wrongSignature(other)
      }
  }

  case object Sin extends BuiltinFunc(
    name = "sin",
    isPure = true,
    description = "sine of a number of radians",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.sin(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Size extends BuiltinFunc(
    name = "size",
    isPure = true,
    description = "number of elements in a list or characters in a string",
    signature = "(ANY?) :: INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(lst)) => Expr.Integer(lst.length.toLong)
        case Vector(Expr.Str(str)) => Expr.Integer(str.length().toLong)
        case other => throw wrongSignature(other)
      }
  }

  case object Split extends BuiltinFunc(
    name = "split",
    isPure = true,
    description = "split a string on every instance of a delimiter",
    signature = "(input :: STRING?, delimiter :: STRING?) :: LIST? OF STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str), Expr.Str(delim)) =>
          Expr.List(str.split(Pattern.quote(delim)).view.map(Expr.Str(_)).toVector)
        case other => throw wrongSignature(other)
      }
  }

  case object Sqrt extends BuiltinFunc(
    name = "sqrt",
    isPure = true,
    description = "square root of a number",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.sqrt(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Substring extends BuiltinFunc(
    name = "substring",
    isPure = true,
    description = "substring of the original string, beginning with a 0-based index start and length",
    signature = "(original :: STRING?, start :: INTEGER? [, end :: INTEGER? ]) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str), Expr.Integer(start)) =>
          Expr.Str(str.drop(start.toInt))
        case Vector(Expr.Str(str), Expr.Integer(start), Expr.Integer(length)) =>
          Expr.Str(str.drop(start.toInt).take(length.toInt))
        case other => throw wrongSignature(other)
      }
  }

  case object Tail extends BuiltinFunc(
    name = "tail",
    isPure = true,
    description = "return the list without its first element",
    signature = "(LIST? OF ANY?) :: LIST? OF ANY?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.List(Vector())) => Expr.List(Vector())
        case Vector(Expr.List(nonEmptyVec)) => Expr.List(nonEmptyVec.tail)
        case other => throw wrongSignature(other)
      }
  }

  case object Tan extends BuiltinFunc(
    name = "tan",
    isPure = true,
    description = "tangent of a number of radians",
    signature = "(NUMBER?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Number(dbl)) => Expr.Floating(Math.tan(dbl))
        case other => throw wrongSignature(other)
      }
  }

  case object Timestamp extends BuiltinFunc(
    name = "timestamp",
    isPure = false, // reads system time
    description = "number of milliseconds elapsed since midnight, January 1, 1970 UTC",
    signature = "() :: INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector() => Expr.Integer(System.currentTimeMillis())
        case other => throw wrongSignature(other)
      }
  }

  case object ToBoolean extends BuiltinFunc(
    name = "toBoolean",
    isPure = true,
    description = "convert a string into a boolean",
    signature = "(STRING?) :: BOOLEAN?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Bool(str.trim().toBoolean)
        case Vector(bool: Expr.Bool) => bool
        case other => throw wrongSignature(other)
      }
  }

  case object ToFloat extends BuiltinFunc(
    name = "toFloat",
    isPure = true,
    description = "convert a string or integer into a float",
    signature = "(ANY?) :: FLOAT?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(flt: Expr.Floating) => flt
        case Vector(Expr.Integer(lng)) => Expr.Floating(lng.toDouble)
        case Vector(Expr.Str(str)) =>
          try Expr.Floating(java.lang.Double.parseDouble(str))
          catch {
            case _: NumberFormatException => Expr.Null
          }
        case other => throw wrongSignature(other)
      }
  }

  case object ToInteger extends BuiltinFunc(
    name = "toInteger",
    isPure = true,
    description = "convert a string or float into an integer",
    signature = "(ANY?) :: INTEGER?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(int: Expr.Integer) => int
        case Vector(Expr.Floating(d)) => Expr.Integer(d.toLong)
        case Vector(Expr.Str(str)) =>
            try {
              val longValue =
                if (str.startsWith("0x") || str.startsWith("-0x")) {
                  // hex-like: take the 0x-started portion as an unsigned int, and negate it if `-` is present
                  BigInt(str.replaceFirst("0x",""), 16).longValue
                } else {
                  // decimal-like (or engineering notation)
                  new java.math.BigDecimal(str).longValue
                }
              Expr.Integer(longValue)
            }
            catch {
              case _: NumberFormatException => Expr.Null
            }
        case other => throw wrongSignature(other)
      }
  }

  case object ToLower extends BuiltinFunc(
    name = "toLower",
    isPure = true,
    description = "convert a string to lowercase",
    signature = "(STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Str(str.toLowerCase())
        case other => throw wrongSignature(other)
      }
  }

  case object ToUpper extends BuiltinFunc(
    name = "toUpper",
    isPure = true,
    description = "convert a string to uppercase",
    signature = "(STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Str(str.toUpperCase())
        case other => throw wrongSignature(other)
      }
  }

  case object ToString extends BuiltinFunc(
    name = "toString",
    isPure = true,
    description = "convert a value to a string",
    signature = "(ANY?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(s: Expr.Str) => s
        case Vector(other) => Expr.Str(other.pretty)
        case other => throw wrongSignature(other)
      }
  }

  case object Trim extends BuiltinFunc(
    name = "trim",
    isPure = true,
    description = "removing leading and trailing whitespace from a string",
    signature = "(STRING?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Str(str)) => Expr.Str(str.trim())
        case other => throw wrongSignature(other)
      }
  }

  case object Type extends BuiltinFunc(
    name = "type",
    isPure = true,
    description = "return the name of a relationship",
    signature = "(RELATIONSHIP?) :: STRING?"
  ) {
    override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
      args match {
        case Vector(Expr.Relationship(_, lbl, _, _)) => Expr.Str(lbl.name)
        case other => throw wrongSignature(other)
      }
  }

  final case class UserDefined(name: String) extends Func {
    private lazy val underlying = userDefinedFunctions(name.toLowerCase)

    def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value = underlying.call(args)

    def isPure: Boolean = underlying.isPure
  }

}
