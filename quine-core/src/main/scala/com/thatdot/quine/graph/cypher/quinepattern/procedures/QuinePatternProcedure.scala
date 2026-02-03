package com.thatdot.quine.graph.cypher.quinepattern.procedures

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.{BaseGraph, LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.language.ast.Value
import com.thatdot.quine.model.Milliseconds

/** Context available to QuinePattern procedures during execution.
  *
  * This provides access to graph operations and execution parameters
  * without coupling procedures to specific Cypher infrastructure.
  *
  * @param graph Graph for accessing literalOps and other services
  * @param namespace Namespace for the query
  * @param atTime Optional historical time for the query
  * @param timeout Timeout for graph operations
  */
case class ProcedureContext(
  graph: BaseGraph with LiteralOpsGraph,
  namespace: NamespaceId,
  atTime: Option[Milliseconds],
  timeout: Timeout,
)

/** Output specification for a procedure.
  *
  * @param name Output column name
  */
case class ProcedureOutput(name: String)

/** Signature for a QuinePattern procedure.
  *
  * @param outputs The output columns produced by the procedure
  * @param description Human-readable description
  */
case class ProcedureSignature(
  outputs: Vector[ProcedureOutput],
  description: String,
)

/** Trait for procedures that work within QuinePattern's execution model.
  *
  * Unlike the old Cypher interpreter's UserDefinedProcedure (which returns
  * Pekko Streams Source), QuinePattern procedures return Future[Seq[...]]
  * to integrate cleanly with the state machine model.
  *
  * Implementations should:
  *   - Be stateless (all state comes from arguments and context)
  *   - Return results as a sequence of maps (one per result row)
  *   - Handle errors by returning failed Futures
  */
trait QuinePatternProcedure {

  /** Procedure name (e.g., "getFilteredEdges") */
  def name: String

  /** Procedure signature */
  def signature: ProcedureSignature

  /** Execute the procedure with evaluated arguments.
    *
    * @param arguments Values for the procedure arguments (already evaluated)
    * @param context Execution context with graph access
    * @param ec Execution context for async operations
    * @return Future of result rows, where each row is a map of output name -> value
    */
  def execute(
    arguments: Seq[Value],
    context: ProcedureContext,
  )(implicit ec: ExecutionContext): Future[Seq[Map[String, Value]]]
}

/** Registry of QuinePattern procedures.
  *
  * Procedures are registered at startup and looked up at runtime
  * when a Procedure plan node is executed.
  */
object QuinePatternProcedureRegistry {

  private val procedures: scala.collection.concurrent.Map[String, QuinePatternProcedure] =
    scala.collection.concurrent.TrieMap.empty

  /** Register a procedure */
  def register(procedure: QuinePatternProcedure): Unit = {
    val _ = procedures.put(procedure.name.toLowerCase, procedure)
  }

  /** Look up a procedure by name */
  def get(name: String): Option[QuinePatternProcedure] =
    procedures.get(name.toLowerCase)

  /** Get all registered procedures */
  def all: Iterable[QuinePatternProcedure] = procedures.values

  // Register built-in procedures
  register(GetFilteredEdgesProcedure)
  register(HelpBuiltinsProcedure)
}
