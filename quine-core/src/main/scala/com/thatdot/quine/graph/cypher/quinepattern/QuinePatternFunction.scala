package com.thatdot.quine.graph.cypher.quinepattern

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime => JavaZonedDateTime}
import java.util.Locale

import org.apache.commons.codec.net.PercentCodec

import com.thatdot.language.ast.Value

/** QuinePatternFunction defines built-in pattern functions available in the Quine query language engine.
  *
  * These functions provide predefined operations such as mathematical calculations, list processing,
  * and null handling. The `builtIns` set contains all predefined instances of QuinePatternFunction.
  *
  * Responsibilities:
  * - Maintains a registry of built-in Quine pattern functions.
  * - Provides a utility method `findBuiltIn` to fetch a built-in function by its name.
  */
object QuinePatternFunction {
  val builtIns: Set[QuinePatternFunction] = Set(
    AbsFunction,
    CeilFunction,
    CoalesceFunction,
    CollectionMaxFunction,
    CollectionMinFunction,
    DateTimeFunction,
    DurationFunction,
    DurationBetweenFunction,
    FloorFunction,
    IdFunction,
    LocalDateTimeFunction,
    RegexFirstMatchFunction,
    SignFunction,
    SplitFunction,
    TextUrlEncodeFunction,
    ToFloatFunction,
    ToIntegerFunction,
    ToLowerFunction,
    ToStringFunction,
  )

  def findBuiltIn(name: String): Option[QuinePatternFunction] = builtIns.find(_.name == name)
}

/** Represents the arity of a function, which can either be fixed or variable.
  *
  * `FunctionArity` is a sealed trait to model the possible configurations of function arity:
  * - `VariableArity`: Indicates a function can accept any number of arguments.
  * - `FixedArity`: Indicates a function accepts a specific number of arguments, denoted by an integer `n`.
  */
sealed trait FunctionArity

object FunctionArity {
  case object VariableArity extends FunctionArity
  case class FixedArity(n: Int) extends FunctionArity
}

/** Represents a pattern-matching function within the Quine query language engine.
  *
  * QuinePatternFunction encapsulates a function by defining its name, arity, the method to handle null values,
  * and the core logic through a Cypher-compatible partial function.
  *
  * Key responsibilities of QuinePatternFunction instances:
  * - Define the function name for identification purposes.
  * - Specify the function arity, indicating whether it supports a fixed or variable number of arguments.
  * - Determine how null values should be handled when arguments are applied.
  * - Implement the Cypher-compatible logic for evaluating the function, which supports pattern-matching semantics specific to Quine.
  *
  * The primary method `apply` is used to process input arguments and return the resulting value by validating the
  * input according to the defined arity, handling null values if required, and delegating to the `cypherFunction`
  * partial function for evaluation.
  */
sealed trait QuinePatternFunction {
  def name: String
  def handleNullsBySpec: Boolean = true
  def arity: FunctionArity;
  def cypherFunction: PartialFunction[List[Value], Value]

  // TODO This should have an error channel. Right now errors are thrown
  // TODO as runtime exceptions so that we get visibility into implementation
  // TODO errors in the QuinePattern alpha
  def apply(args: List[Value]): Value = {
    arity match {
      case FunctionArity.FixedArity(n) =>
        if (args.size != n)
          throw new RuntimeException(
            s"Function $name has arity of $n, but received ${args.size} arguments!",
          )
      case _ =>
        //No need to do anything in this case
        ()
    }
    if (handleNullsBySpec && args.contains(Value.Null)) {
      Value.Null
    } else {
      cypherFunction.applyOrElse(
        args,
        (_: List[Value]) =>
          throw new QuinePatternUnimplementedException(
            s"Function $name doesn't support arguments of type ${args.map(_.getClass.getSimpleName).mkString(", ")}",
          ),
      )
    }
  }
}

/** Represents the `floor` function within the Quine query language engine.
  *
  * The `floor` function takes a single numeric input argument and returns the largest
  * integer value less than or equal to the input (mathematical floor).
  *
  * Function characteristics:
  * - Name: `floor`
  * - Arity: Fixed (expects exactly one argument)
  * - Input: A single `Value.Real` representing a floating-point number
  * - Output: A `Value.Real` representing the floored value of the input
  *
  * This function is designed to be compatible with Quine's Cypher-like query language and
  * follows the standard mathematical definition of the floor function.
  */
object FloorFunction extends QuinePatternFunction {
  val name: String = "floor"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Real(d)) =>
    Value.Real(math.floor(d))
  }
}

/** The `CeilFunction` object represents a specific Quine pattern function that computes the ceiling value of a real number.
  *
  * This function is identified by the name "ceil" and operates with a fixed arity of 1 argument.
  * When called with a single real number, it returns the smallest integer greater than or equal to the input value
  * as a real number.
  *
  * Function behavior:
  * - The `cypherFunction` defines the logic to compute the ceiling using the standard `math.ceil` method.
  * - The input must be a list of one `Value.Real` type; otherwise, the function is not applicable.
  */
object CeilFunction extends QuinePatternFunction {
  val name: String = "ceil"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Real(d)) =>
    Value.Real(math.ceil(d))
  }
}

/** A custom Quine pattern function that splits a given string into a list of substrings, based on a specified delimiter.
  *
  * This function takes exactly two arguments:
  * 1. The string to be split.
  * 2. A string that represents the delimiter sequence.
  *
  * The function returns a list of substrings obtained by splitting the input string at each occurrence of the delimiter's first character.
  *
  * Function details:
  * - **Name**: "split"
  * - **Arity**: Fixed, requires exactly two arguments.
  *
  * The `cypherFunction` defines the partial function logic for executing the operation.
  * If the input arguments provided are not valid (e.g., incorrect types or mismatched arity), the function will throw an exception.
  */
object SplitFunction extends QuinePatternFunction {
  val name: String = "split"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(strToSplit), Value.Text(splitSeq)) =>
    Value.List(strToSplit.split(splitSeq.head).toList.map(Value.Text))
  }
}

/** A Quine pattern-matching function that converts a single textual input into an integer representation.
  *
  * - Name: `toInteger`
  * - Arity: Fixed, expects exactly one argument.
  * - Function logic: If the input is of type `Value.Text`, the function attempts to parse the text as a long integer
  * (`Value.Integer`).
  *
  * This function only supports text input that can be successfully parsed into an integer. If the input is of a
  * different type or cannot be parsed, an exception is thrown.
  */
object ToIntegerFunction extends QuinePatternFunction {
  val name: String = "toInteger"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(str)) =>
    try Value.Integer(str.toLong)
    catch {
      case _: Throwable => Value.Null
    }
  }
}

/** CollectionMaxFunction provides functionality to retrieve the maximum value from a collection
  * of elements in the context of Quine's query language engine. It supports processing lists
  * of string values and determines the maximum value based on lexicographical order.
  *
  * Key Details:
  * - The function is identified by the name "coll.max".
  * - It accepts a single argument, which must be a list of values.
  * - When invoked, the function iterates through the list, comparing values lexicographically
  * to determine the maximum.
  * - The function operates exclusively on string values within the list. If passed elements
  * of unsupported types, an exception is raised.
  *
  * Behavior:
  * - If the list contains mixed types or unsupported types, the function throws a
  * QuinePatternUnimplementedException.
  * - The function assumes all values in the list are of compatible types before proceeding.
  *
  * Arity:
  * - Fixed arity of one (accepts exactly one argument).
  *
  * Exceptions:
  * - Throws QuinePatternUnimplementedException for unsupported types or invalid arguments.
  */
object CollectionMaxFunction extends QuinePatternFunction {
  val name: String = "coll.max"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.List(values)) =>
    values.head match {
      case Value.Text(str) =>
        values.tail.foldLeft(Value.Text(str)) { (max, value) =>
          value match {
            case n @ Value.Text(next) =>
              val comparisonResult = max.str.compareTo(next)
              if (comparisonResult < 0) {
                n
              } else {
                max
              }
            case other =>
              throw new QuinePatternUnimplementedException(
                s"Function $name doesn't support arguments of type ${other.getClass.getSimpleName}",
              )
          }
        }
      case other =>
        throw new QuinePatternUnimplementedException(
          s"Function $name doesn't support arguments of type ${other.getClass.getSimpleName}",
        )
    }
  }
}

/** The `CollectionMinFunction` object represents a Quine query language function `coll.min`.
  *
  * This function is used to find the minimum value within a collection. It operates on a single argument,
  * which must be a list of values (`Value.List`). If the list contains text elements, it computes the
  * lexicographical minimum among the text values. If the list contains unsupported types, it throws a
  * `QuinePatternUnimplementedException`.
  *
  * Key features:
  * - The function name is `coll.min`.
  * - The arity is fixed at 1, meaning it takes exactly one argument.
  * - The core logic is implemented in the `cypherFunction` partial function, processing lists of `Value`.
  * - If the collection contains a type other than `Value.Text`, the function will throw an exception indicating
  * unsupported argument types.
  *
  * Note:
  * - The function does not currently support collections containing mixed or non-text element types.
  * - When comparing text values, lexicographical order is used.
  */
object CollectionMinFunction extends QuinePatternFunction {
  val name: String = "coll.min"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.List(values)) =>
    values.head match {
      case Value.Text(str) =>
        values.tail.foldLeft(Value.Text(str)) { (min, value) =>
          value match {
            case n @ Value.Text(next) =>
              val comparisonResult = min.str.compareTo(next)
              if (comparisonResult > 0) {
                n
              } else {
                min
              }
            case other =>
              throw new QuinePatternUnimplementedException(
                s"Function $name doesn't support arguments of type ${other.getClass.getSimpleName}",
              )
          }
        }
      case other =>
        throw new QuinePatternUnimplementedException(
          s"Function $name doesn't support arguments of type ${other.getClass.getSimpleName}",
        )
    }
  }
}

/** Represents a Cypher-compatible 'coalesce' function implemented as a Quine pattern function.
  *
  * The coalesce function evaluates a list of input values and returns the first non-null value.
  * If all input values are null, the function returns null.
  *
  * Key characteristics:
  * - Name: "coalesce"
  * - Arity: Variable, supporting an arbitrary number of arguments.
  * - Implementation: Evaluates the list of arguments in order and finds the first non-null value,
  * returning null if no non-null value is found.
  *
  * The function is defined to handle null values in a manner consistent with Cypher semantics,
  * enabling robust pattern-matching capabilities within the Quine query engine.
  */
object CoalesceFunction extends QuinePatternFunction {
  val name: String = "coalesce"
  val arity: FunctionArity = FunctionArity.VariableArity
  override val handleNullsBySpec: Boolean = false
  val cypherFunction: PartialFunction[List[Value], Value] = { case args =>
    args.find(_ != Value.Null).getOrElse(Value.Null)
  }
}

/** The `ToLowerFunction` object represents a Quine pattern function that converts a given string to lowercase.
  *
  * This function operates on a single argument of type `Value.Text` and returns a `Value.Text` containing the lowercase version
  * of the input string. If the input argument is not of the expected type or if the arity is incorrect, the function will
  * throw an exception.
  *
  * Key characteristics:
  * - Name: `toLower`
  * - Fixed arity: accepts exactly one argument
  * - Implements logic via a Cypher-compatible partial function
  */
object ToLowerFunction extends QuinePatternFunction {
  val name: String = "toLower"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(str)) =>
    Value.Text(str.toLowerCase)
  }
}

/** A Quine pattern function that converts an integer value to its string representation.
  *
  * The `ToStringFunction` is a unary function, identified by the name "toString",
  * which consumes a single integer argument and produces its textual representation as a `Text` value.
  * It is implemented as a partial function that matches input of type `Value.Integer` and
  * converts the integer to a string.
  *
  * Key parameters:
  * - `name`: The name of the function ("toString").
  * - `arity`: Specifies the function as having a fixed arity of 1.
  * - `cypherFunction`: Defines the logic of converting a `Value.Integer` to a `Value.Text` containing its string representation.
  */
object ToStringFunction extends QuinePatternFunction {
  val name: String = "toString"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Integer(n)) =>
    Value.Text(n.toString)
  }
}

/** A function used to handle date-time conversions and parsing within the Quine query language engine.
  *
  * `DateTimeFunction` provides date-time processing capabilities supporting variable-arity input arguments.
  * It allows constructing a `Value.DateTime` object from either epoch milliseconds or a text representation
  * of a date-time with an optional format string.
  *
  * Supported input cases:
  * 1. A map containing the key `epochMillis` with an associated integer value representing milliseconds
  * since the epoch in UTC. This resolves to a `Value.DateTime` object.
  * 2. Two text arguments representing a date-time string and a format string. The format string should
  * adhere to the patterns defined by `java.time.format.DateTimeFormatter`. This resolves to a
  * `Value.DateTime` object by parsing the text input.
  *
  * If the input arguments are of unexpected types or invalid, a `QuinePatternUnimplementedException` is thrown.
  */
object DateTimeFunction extends QuinePatternFunction {
  val name: String = "datetime"
  val arity: FunctionArity = FunctionArity.VariableArity
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Map(values)) =>
      values(Symbol("epochMillis")) match {
        case Value.Integer(milliseconds) =>
          Value.DateTime(
            JavaZonedDateTime.ofInstant(
              Instant.ofEpochMilli(milliseconds),
              ZoneOffset.UTC,
            ),
          )
        case other =>
          throw new QuinePatternUnimplementedException(
            s"Function $name doesn't support arguments of type ${other.getClass.getSimpleName}",
          )
      }
    case List(Value.Text(timeStr), Value.Text(timeFormat)) =>
      val formatter = DateTimeFormatter.ofPattern(timeFormat, Locale.US)
      val dateTime = JavaZonedDateTime.parse(timeStr, formatter)
      Value.DateTime(dateTime)
  }
}

/** An implementation of `QuinePatternFunction` that extracts the first regex match from a given text input.
  *
  * This function takes exactly two arguments:
  * - The first argument is the text to match against.
  * - The second argument is a string representing the regex pattern.
  *
  * If the regex has matching groups, the function returns all groups from the first match, including the full match
  * (group 0). If no match is found, an empty list is returned.
  *
  * Function Properties:
  * - `name`: The name of the function is `text.regexFirstMatch`.
  * - `arity`: This function has a fixed arity of 2, meaning it requires exactly two arguments.
  * - `cypherFunction`: The partial function implementing the core logic for extracting matches.
  */
object RegexFirstMatchFunction extends QuinePatternFunction {
  val name: String = "text.regexFirstMatch"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(toMatch), Value.Text(patternStr)) =>
    val pattern = patternStr.r
    val regexMatch = pattern.findFirstMatchIn(toMatch).toList
    Value.List(
      for {
        m <- regexMatch
        i <- 0 to m.groupCount
      } yield Value.Text(m.group(i)),
    )
  }
}

/** The `ToFloat` object is a pattern-matching function that converts a value into its floating-point representation.
  *
  * This function supports converting values of the following types:
  * - `Value.Real`: Retains the same floating-point representation.
  * - `Value.Integer`: Converts the integer value to its floating-point representation.
  * - `Value.Text`: Parses the text value as a floating-point number.
  *
  * Any other value types provided as input will result in an exception as they are not supported by the function.
  *
  * Attributes:
  * - `name`: The name of the function, which is "toFloat".
  * - `arity`: Specifies that the function requires exactly one argument.
  * - `cypherFunction`: The core logic for performing the conversion, implemented as a partial function.
  */
object ToFloatFunction extends QuinePatternFunction {
  val name: String = "toFloat"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Real(r)) => Value.Real(r)
    case List(Value.Integer(n)) => Value.Real(n.toDouble)
    case List(Value.Text(str)) =>
      try Value.Real(str.toDouble)
      catch {
        case _: Throwable => Value.Null
      }
  }
}

/** A custom Quine pattern function for performing addition operations.
  *
  * The `AddFunction` object extends `QuinePatternFunction` and implements its own
  * name, arity, and partial function logic. This function supports the following use cases:
  * - Adding two integers, resulting in an integer.
  * - Adding two real numbers, resulting in a real number.
  * - Concatenating two text strings, resulting in a single concatenated text.
  * - Concatenating a text string with an integer, resulting in a text string.
  *
  * Function Name: `"add"`
  *
  * Arity: Fixed arity of 2 arguments.
  *
  * Core Logic:
  * For two arguments, depending on their types:
  * - If both arguments are integers, it returns their sum as an integer.
  * - If both arguments are real numbers, it returns their sum as a real number.
  * - If both arguments are text strings, it concatenates them, returning a text string.
  * - If one argument is a text string and the other is an integer, it concatenates them, treating the integer as a text string.
  */
object AddFunction extends QuinePatternFunction {
  val name: String = "add"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(n1), Value.Integer(n2)) => Value.Integer(n1 + n2)
    case List(Value.Real(n1), Value.Real(n2)) => Value.Real(n1 + n2)
    case List(Value.Real(n1), Value.Integer(n2)) => Value.Real(n1 + n2)
    case List(Value.Text(lstr), Value.Text(rstr)) => Value.Text(lstr + rstr)
    case List(Value.Text(str), Value.Integer(n)) => Value.Text(str + n)
  }
}

/** MultiplyFunction is a Quine pattern function that performs multiplication on two numeric values.
  *
  * This function supports multiplication of:
  * - Two integers (producing an integer result).
  * - Two real numbers (producing a real result).
  * - An integer and a real number (producing a real result).
  *
  * - The function name is defined as "multiply".
  * - The function arity is fixed at two arguments.
  * - Null handling is determined by the inherited behavior of QuinePatternFunction.
  *
  * The core logic of the function is implemented in the `cypherFunction`, which is a partial function
  * matching valid input types to the corresponding multiplication behavior.
  */
object MultiplyFunction extends QuinePatternFunction {
  val name: String = "multiply"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(n1), Value.Integer(n2)) => Value.Integer(n1 * n2)
    case List(Value.Real(n1), Value.Real(n2)) => Value.Real(n1 * n2)
    case List(Value.Real(n), Value.Integer(n2)) => Value.Real(n * n2)
    case List(Value.Integer(a), Value.Real(b)) => Value.Real(a * b)
  }
}

/** Object representing a divide function within the Quine query language engine.
  *
  * The DivideFunction object provides a QuinePatternFunction for performing division
  * on numeric values. It accepts exactly two arguments and returns their quotient.
  *
  * This function supports integer and real number division, based on the runtime
  * types of the input values provided. If the input arguments are integers, the
  * result is an integer quotient. If the input arguments are real numbers, the
  * result is a real quotient. The function relies on pattern matching to determine
  * and apply the correct operation based on the input types.
  *
  * Key Details:
  * - Name: "divide"
  * - Arity: Fixed, requiring exactly two arguments.
  * - Cypher Function: Handles division for both integer and real numbers.
  *
  * Division by zero or unsupported argument types will result in a runtime exception.
  */
object DivideFunction extends QuinePatternFunction {
  val name: String = "divide"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(n1), Value.Integer(n2)) => Value.Integer(n1 / n2)
    case List(Value.Real(n1), Value.Real(n2)) => Value.Real(n1 / n2)
    case List(Value.Integer(n), Value.Real(n2)) => Value.Real(n / n2)
  }
}

/** Represents the equality comparison function within the Quine query language engine.
  *
  * The `CompareEqualityFunction` is a concrete implementation of `QuinePatternFunction` that
  * checks if two given values are equal. It evaluates the equality through a Cypher-compatible
  * function and returns a boolean result wrapped in a `Value` object.
  *
  * Key characteristics:
  * - Name: "equals"
  * - Fixed arity of 2, requiring exactly two input arguments.
  * - Does not handle null values by specification. If any argument is null, it will not perform custom handling.
  * - Cypher function logic: Compares the input arguments for equality and returns `Value.True` if equal,
  * otherwise `Value.False`.
  */
object CompareEqualityFunction extends QuinePatternFunction {
  val name: String = "equals"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  override def handleNullsBySpec: Boolean = false
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(left, right) =>
    if (left == right) Value.True else Value.False
  }
}

/** Implements a comparison function that checks if the first argument is greater than or equal to the second argument.
  *
  * `CompareGreaterThanEqualToFunction` is a specific implementation of `QuinePatternFunction` designed to facilitate
  * greater-than-or-equal-to comparison operations within the Quine engine for supported argument types:
  * integers, real numbers, and text values.
  *
  * This function:
  * - Operates on two arguments, as enforced by its fixed arity of 2.
  * - Returns `Value.True` if the first argument is greater than or equal to the second argument.
  * - Returns `Value.False` if the first argument is not greater than or equal to the second argument.
  * - Does not handle null values by specification (`handleNullsBySpec = false`).
  *
  * The `cypherFunction` defines the evaluation logic using pattern matching, ensuring type-specific comparisons:
  * - Integer comparison evaluates based on numeric ordering.
  * - Real number comparison evaluates based on numeric ordering.
  * - Text comparison evaluates based on lexicographical ordering.
  *
  * Invalid types or arguments not matching supported cases will result in an error during evaluation.
  */
object CompareGreaterThanEqualToFunction extends QuinePatternFunction {
  val name: String = "greaterThanEqualTo"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(a), Value.Integer(b)) => if (a >= b) Value.True else Value.False
    case List(Value.Real(a), Value.Real(b)) => if (a >= b) Value.True else Value.False
    case List(Value.Text(a), Value.Text(b)) => if (a >= b) Value.True else Value.False
  }
}

object CompareGreaterThanFunction extends QuinePatternFunction {
  val name: String = "greaterThan"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(a), Value.Integer(b)) => if (a > b) Value.True else Value.False
    case List(Value.Real(a), Value.Real(b)) => if (a > b) Value.True else Value.False
    case List(Value.Text(a), Value.Text(b)) => if (a > b) Value.True else Value.False
    case List(Value.Duration(a), Value.Duration(b)) => if (a.compareTo(b) > 0) Value.True else Value.False
  }
}

/** Represents a pattern function that evaluates whether a value is less than another value.
  *
  * `CompareLessThanFunction` is a Quine pattern function that checks if the first argument is less than
  * the second argument. The arguments can be integers, real numbers, or text strings. It returns a boolean
  * result based on the comparison.
  *
  * - **Name**: "lessThan"
  * - **Arity**: Fixed arity with exactly two arguments
  *
  * The function supports comparison of three specific data types:
  * - Integer values
  * - Real number values
  * - Text string values (compared lexicographically)
  *
  * If the arguments do not match the expected types or the comparison is invalid, the function will
  * raise an appropriate exception.
  */
object CompareLessThanFunction extends QuinePatternFunction {
  val name: String = "lessThan"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(a), Value.Integer(b)) => if (a < b) Value.True else Value.False
    case List(Value.Real(a), Value.Real(b)) => if (a < b) Value.True else Value.False
    case List(Value.Text(a), Value.Text(b)) => if (a < b) Value.True else Value.False
  }
}

/** LogicalAndFunction implements a logical "AND" operation within the Quine query language engine.
  *
  * This object defines a two-argument function named "and" that evaluates the logical conjunction
  * of two boolean values (`true` and `false`). The result adheres to standard logical "AND" semantics:
  * - Returns `true` if both inputs are `true`.
  * - Returns `false` for all other combinations of inputs.
  *
  * The function has a fixed arity of 2, requiring exactly two arguments.
  * It is defined using a partial function that evaluates input cases based on provided values.
  */
object LogicalAndFunction extends QuinePatternFunction {
  val name: String = "and"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.True, Value.True) => Value.True
    case List(Value.False, Value.False) => Value.False
    case List(Value.True, Value.False) => Value.False
    case List(Value.False, Value.True) => Value.False
  }
}

/** LogicalOrFunction defines the `or` logical operation in the Quine query language.
  *
  * This object implements the `QuinePatternFunction` trait and represents a fixed-arity
  * logical 'OR' operator that evaluates two boolean values and returns their logical disjunction.
  *
  * Name:
  * - The name of this function is "or".
  *
  * Arity:
  * - This function has a fixed arity of 2, meaning it requires exactly two arguments.
  *
  * Core Functionality:
  * - The function evaluates the logical disjunction of two boolean values.
  * - The input is a list of two `Value` instances, where each `Value` is expected to be `Value.True` or `Value.False`.
  * - The result of the evaluation is:
  *   - `Value.True` if either or both inputs are `Value.True`.
  *   - `Value.False` if both inputs are `Value.False`.
  */
object LogicalOrFunction extends QuinePatternFunction {
  val name: String = "or"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.True, Value.True) => Value.True
    case List(Value.False, Value.False) => Value.False
    case List(Value.True, Value.False) => Value.True
    case List(Value.False, Value.True) => Value.True
  }
}

/** Represents a Cypher-compatible `sign` function for pattern matching in the Quine query language
  * .
  *
  * The `sign` function evaluates a single numerical argument and returns the sign
  * of the value as an integer:
  * - `1` if the number is positive.
  * - `-1` if the number is negative.
  * - `0` if the number is zero.
  *
  * Supported input types:
  * - `Integer`: Processes integer numerical values.
  * - `Real`: Processes real (floating-point) numerical values.
  *
  * Expects exactly one argument (Fixed Arity: 1). If the argument is not a numerical
  * input of the supported types
  * or if it has more/less than one argument, an error is raised.
  */
object SignFunction extends QuinePatternFunction {
  val name: String = "sign"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(n)) => Value.Integer(if (n > 0) 1 else if (n < 0) -1 else 0)
    case List(Value.Real(n)) => Value.Integer(if (n > 0) 1 else if (n < 0) -1 else 0)
  }
}

/** Implements the absolute value function for use within the Quine query language engine.
  *
  * The `AbsFunction` object extends the `QuinePatternFunction` trait, providing an implementation
  * of a Cypher-compatible
  * function that calculates the absolute value of a numeric input. It supports both integer
  * and real number types.
  *
  * The function behavior is defined as follows:
  * - The name of the function is "abs".
  * - It has a fixed arity of 1, meaning it requires exactly one argument.
  * - The function takes a single numeric value and returns its absolute value.
  *   - For integer inputs, the result is an integer.
  *   - For real number inputs, the result is a real number.
  *
  * Input that does not match the expected types or format will result in an error.
  */
object AbsFunction extends QuinePatternFunction {
  val name: String = "abs"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Integer(n)) => Value.Integer(math.abs(n))
    case List(Value.Real(n)) => Value.Real(math.abs(n))
  }
}

object IdFunction extends QuinePatternFunction {
  val name: String = "id"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  override val handleNullsBySpec: Boolean = false
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Node(nodeId, _, _)) => Value.NodeId(nodeId)
    case List(Value.Null) => throw new IllegalArgumentException("Cannot evaluate id function on null value")
  }
}

object NotEquals extends QuinePatternFunction {
  val name: String = "notEquals"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  override val handleNullsBySpec: Boolean = false
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(v1, v2) =>
    if (v1 == v2) Value.False else Value.True
  }
}

object DurationFunction extends QuinePatternFunction {
  val name: String = "duration"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(durationString)) =>
    Value.Duration(java.time.Duration.parse(durationString))
  }
}

object DurationBetweenFunction extends QuinePatternFunction {
  val name: String = "duration.between"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.DateTime(start), Value.DateTime(end)) => Value.Duration(java.time.Duration.between(start, end))
    case List(Value.DateTimeLocal(start), Value.DateTimeLocal(end)) =>
      Value.Duration(java.time.Duration.between(start, end))
  }
}

object TextUrlEncodeFunction extends QuinePatternFunction {
  val name: String = "text.urlencode"
  val arity: FunctionArity = FunctionArity.FixedArity(1)
  val cypherFunction: PartialFunction[List[Value], Value] = { case List(Value.Text(text)) =>
    Value.Text(new String(new PercentCodec(Array.empty[Byte], false).encode(text.getBytes(StandardCharsets.UTF_8))))
  }
}

object LocalDateTimeFunction extends QuinePatternFunction {
  val name: String = "localdatetime"
  val arity: FunctionArity = FunctionArity.FixedArity(2)
  val cypherFunction: PartialFunction[List[Value], Value] = {
    case List(Value.Text(dateTimeStr), Value.Text(formatStr)) =>
      Value.DateTimeLocal(LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern(formatStr)))
  }
}
