package com.thatdot.quine.gremlin

import scala.reflect.ClassTag
import scala.util.Try
import scala.util.parsing.input.{Position, Positional}

import org.apache.commons.text.StringEscapeUtils

import com.thatdot.quine.graph.{BaseGraph, cypher, idFrom}
import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}
import com.thatdot.quine.util.Log._

/** A Gremlin vertex object. This is what gets returned from a query like `g.V().has("foo")`.
  *
  * @param id the underlying primary key of the node in Quine
  */
final case class Vertex(id: QuineId)

/** A Gremlin edge object. This is what gets returned from a query like `g.V().has("foo").outE()`.
  *
  * @param fromId the underlying primary key of the node in Quine from which the edge originates
  * @param toId   the underlying primary key of the node in Quine at which the edge ends
  */
final case class Edge(fromId: QuineId, label: Symbol, toId: QuineId)

/** An expression in a Gremlin query
  *
  * Examples: `[x,898,y]`, `x`, `"hello"`.
  */
sealed abstract class GremlinExpression extends Positional {

  /** Evaluate the Gremlin value into some JVM value.
    *
    * Arrays evaluate to [[scala.collection.immutable.Seq]]'s and predicates evaluated to
    * [[GremlinPredicate]]'s.
    *
    * @param store      variables in scope at the moment of evaluation
    * @param idProvider procedures for handling ID values
    */
  @throws(
    "The TypedValue(QuineValue.Id) provided could not be deserialized using the implicitly-provided idProvider",
  )
  @throws[UnboundVariableError](
    "The Variable provided was not bound by the implicitly-provided store",
  )
  def eval()(implicit store: VariableStore, idProvider: QuineIdProvider): Any = this match {
    case TypedValue(QuineValue.Id(id)) => idProvider.customIdFromQid(id).get
    // These traversals ensure ids in nested quinevals get decoded
    case TypedValue(QuineValue.List(vals)) =>
      vals.map(TypedValue(_).eval())
    case TypedValue(QuineValue.Map(map)) => map.view.mapValues(TypedValue(_).eval()).toMap
    case TypedValue(qv) => qv.underlyingJvmValue

    case IdFromFunc(xs) =>
      val hashed: QuineId = idFrom(xs.map(expr => cypher.Value.fromAny(expr.eval())): _*)
      idProvider.customIdFromQid(hashed).get

    case RawArr(xs) => xs.map(_.eval())

    case Variable(v) => store.get(v, pos)
  }

  /** Evaluate a Gremlin value then cast the result into a certain type.
    *
    * @param store        variables in scope at the moment of evaluation
    * @param idProvider procedures for handling ID values
    * @param errorMessage explanation for why the caller expects the type
    * @tparam T type to which the evaluated value is cast
    */
  def evalTo[T](
    errorMessage: => String,
  )(implicit ct: ClassTag[T], store: VariableStore, idProvider: QuineIdProvider): T =
    eval().castTo[T](errorMessage, Some(pos)).get

  /** Pretty-print the Gremlin value. Ideally this roundtrips parsing.
    *
    * @param graph needed for printing custom user IDs
    * @return a pretty-printed representation of the value
    */
  def pprint(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = this match {

    case TypedValue(QuineValue.Str(str)) => "\"" + StringEscapeUtils.escapeJson(str) + "\""

    // Custom IDs should be printed according to the ID provider
    case TypedValue(QuineValue.Id(qid)) =>
      idProvider.qidToPrettyString(qid)

    case TypedValue(v) => v.underlyingJvmValue.toString // TODO consider something better for lists

    case Variable(v) => v.name

    case IdFromFunc(exprs) => exprs.map(_.pprint).mkString("idFrom(", ",", ")")

    case RawArr(exprs) => exprs.map(_.pprint).mkString("[", ",", "]")
  }
}

final private[gremlin] case class TypedValue(value: QuineValue) extends GremlinExpression

final private[gremlin] case class IdFromFunc(arguments: Vector[GremlinExpression]) extends GremlinExpression

final private[gremlin] case class RawArr(elements: Vector[GremlinExpression]) extends GremlinExpression

final private[gremlin] case class Variable(variableName: Symbol) extends GremlinExpression

/** An expression in a Gremlin query which evaluates to a predicate
  *
  * Examples: `regex("[0-9a-fA-F]+")`, `eq(123)`
  */
sealed private[gremlin] trait GremlinPredicateExpression extends Positional {

  def evalPredicate()(implicit
    store: VariableStore,
    idProvider: QuineIdProvider,
  ): GremlinPredicate[_] = this match {
    case EqPred(x) =>
      val compareTo = x.eval()
      GremlinPredicate[Any]((y: Any) => compareTo == y, this)

    case NeqPred(x) =>
      val compareTo = x.eval()
      GremlinPredicate[Any]((y: Any) => compareTo != y, this)

    case WithinPred(xs) =>
      val arr = xs.evalTo[Seq[Any]]("`within(...)` expects its argument to be an array")
      GremlinPredicate[Any]((x: Any) => arr.contains(x), this)

    case RegexPred(p) =>
      val reg = p.evalTo[String]("`regex(...)` expects its argument to be a string")
      GremlinPredicate[String]((s: String) => s.matches(reg), this)
  }

  def pprint(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = this match {
    case EqPred(x) => "eq(" + x.pprint + ")"
    case NeqPred(x) => "neq(" + x.pprint + ")"
    case WithinPred(xs) => "within(" + xs.pprint + ")"
    case RegexPred(r) => "regex(" + r.pprint + ")"
  }

}

final private[gremlin] case class EqPred(compareTo: GremlinExpression) extends GremlinPredicateExpression

final private[gremlin] case class NeqPred(compareTo: GremlinExpression) extends GremlinPredicateExpression

final private[gremlin] case class WithinPred(arr: GremlinExpression) extends GremlinPredicateExpression

final private[gremlin] case class RegexPred(pattern: GremlinExpression) extends GremlinPredicateExpression

/** Represents a predicate value in the Gremlin language.
  *
  * Since values in the Gremlin language have type [[scala.Any]], it is fair to wonder why we need a
  * full blown type for representing predicates (instead of just re-using `U => Boolean`). The main
  * reason is for exception handling:
  *
  *   - detecting when the argument doesn't match the predicate's expected input type means keeping
  * a [[scala.reflect.ClassTag]] of evidence around (to work around erasure)
  *
  *   - providing helpful error messages is easier if we can see (1) what source-level
  * [[GremlinExpression]] produced the predicate and (2) where the predicate is being evaluated
  *
  * @param predicate     underlying predicate function
  * @param evaluatedFrom initial Gremlin value which evaluated to the predicate function
  *                      (used only for error reporting purposes)
  * @tparam Input        input type the predicate expects
  */
final case class GremlinPredicate[Input: ClassTag](
  predicate: Input => Boolean,
  evaluatedFrom: GremlinPredicateExpression,
) {

  /** Run the predicate against a value.
    *
    * This operation can fail, for instance if the type of the input isn't the right one
    * (example: passing a number to a regex predicate), in which case this returns a failure.
    *
    * @param value value against which the predicate is run
    * @param pos   where in the query the predicate needs to be evaluated (needed for error messages)
    * @return whether the predicate passed or not
    */
  def testAgainst(
    value: Any,
    pos: Option[Position],
  )(implicit
    graph: BaseGraph,
    logConfig: LogConfig,
  ): Try[Boolean] = {
    def printedPredicate = evaluatedFrom.pprint(graph.idProvider, logConfig)
    value
      .castTo[Input](s"the predicate `$printedPredicate` can't be used on `$value`", pos)
      .map(predicate)
  }
}

/** Mapping of bound variables to their values. */
final class VariableStore(private val mapping: Map[Symbol, Any]) {

  /** Add a new variable and its corresponding value to the store */
  def +(kv: (Symbol, Any)): VariableStore = new VariableStore(mapping + kv)

  /** Check if a variable is bound in the current store */
  def contains(variable: Symbol): Boolean = mapping.contains(variable)

  /** Get the value associated with a given variable.
    *
    * @param variable variable whose value is to be looked up
    * @param pos      where in the query this lookup is needed (needed for error messages)
    * @return the value of the variable, or throws an [[UnboundVariableError]]
    */
  def get(variable: Symbol, pos: Position): Any = mapping.getOrElse(
    variable,
    throw UnboundVariableError(variable, Some(pos)),
  )

  override def toString: String = s"VariableStore($mapping)"
}

object VariableStore {
  def empty = new VariableStore(Map.empty)
}
