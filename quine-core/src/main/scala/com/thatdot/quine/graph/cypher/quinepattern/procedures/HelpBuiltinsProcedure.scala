package com.thatdot.quine.graph.cypher.quinepattern.procedures

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.quine.graph.cypher.Func
import com.thatdot.quine.language.ast.Value

/** QuinePattern implementation of help.builtins procedure.
  *
  * Lists all built-in Cypher functions with their signatures and descriptions.
  *
  * Arguments: none
  *
  * Yields:
  *   name: Text - the function name
  *   signature: Text - the function signature
  *   description: Text - the function description
  */
object HelpBuiltinsProcedure extends QuinePatternProcedure {

  val name: String = "help.builtins"

  val signature: ProcedureSignature = ProcedureSignature(
    outputs = Vector(
      ProcedureOutput("name"),
      ProcedureOutput("signature"),
      ProcedureOutput("description"),
    ),
    description = "List built-in cypher functions",
  )

  def execute(
    arguments: Seq[Value],
    context: ProcedureContext,
  )(implicit ec: ExecutionContext): Future[Seq[Map[String, Value]]] = {

    if (arguments.nonEmpty) {
      throw new IllegalArgumentException(
        s"$name takes no arguments, got ${arguments.length}",
      )
    }

    // Get all builtin functions sorted by name
    val results = Func.builtinFunctions.sortBy(_.name).map { func =>
      Map(
        "name" -> Value.Text(func.name),
        "signature" -> Value.Text(func.signature),
        "description" -> Value.Text(func.description),
      )
    }

    Future.successful(results)
  }
}
