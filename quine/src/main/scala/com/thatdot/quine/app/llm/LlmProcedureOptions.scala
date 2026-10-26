package com.thatdot.quine.app.llm

import io.circe.Json

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.cypher.{CypherException, Expr, Value}
import com.thatdot.quine.model.QuineIdProvider

/** Reading the `options` map the `llm.*` procedures accept, with errors a query author can act on.
  * A null option value means the option's default, as in the `history.*` procedures, so a computed
  * options map can carry null instead of being built conditionally.
  */
final class LlmProcedureOptions(procedureName: String, options: Map[String, Value], allowed: Set[String]) {

  def error(message: String): CypherException.Runtime = LlmError.queryError(procedureName, message)

  /** A `Left` message becomes a runtime Cypher error carrying the procedure name. */
  def orThrow[A](result: Either[String, A]): A = result.fold(message => throw error(message), identity)

  /** Every option key must be known, even one whose value is null; a typo silently ignored would
    * change what the call does.
    */
  def rejectUnknown(): Unit =
    options.keys.find(!allowed.contains(_)).foreach { unknown =>
      throw error(s"""unknown option "$unknown", expected one of ${allowed.toList.sorted.mkString(", ")}""")
    }

  /** The option's value, with null meaning the key was left out. */
  private def get(key: String): Option[Value] = options.get(key).filter(_ != Expr.Null)

  def string(key: String): Option[String] = get(key).map {
    case Expr.Str(s) => s
    case other => throw error(s"""option "$key" must be a string, got ${other.typ.pretty}""")
  }

  /** A string is a one-element chain; a list of strings is the chain in order. A null element is an
    * error, not skipped: a chain with a hole is a query mistake.
    */
  def stringOrList(key: String): Option[List[String]] = get(key).map {
    case Expr.Str(s) => List(s)
    case Expr.List(values) if values.isEmpty =>
      throw error(s"""option "$key" must be a string or a non-empty list of strings, got an empty list""")
    case Expr.List(values) =>
      values.toList.map {
        case Expr.Str(s) => s
        case other =>
          throw error(
            s"""option "$key" must be a string or a list of strings, got a list containing ${other.typ.pretty}""",
          )
      }
    case other => throw error(s"""option "$key" must be a string or a list of strings, got ${other.typ.pretty}""")
  }

  def boolean(key: String): Option[Boolean] = get(key).map {
    case Expr.True => true
    case Expr.False => false
    case other => throw error(s"""option "$key" must be a boolean, got ${other.typ.pretty}""")
  }

  def json(key: String)(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Option[Json] =
    get(key).map {
      case m: Expr.Map => Value.toJson(m)
      case other => throw error(s"""option "$key" must be a map, got ${other.typ.pretty}""")
    }
}
