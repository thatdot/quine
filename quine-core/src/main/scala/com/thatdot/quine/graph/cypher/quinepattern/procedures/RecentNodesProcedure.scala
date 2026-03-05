package com.thatdot.quine.graph.cypher.quinepattern.procedures

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.util.Timeout

import cats.implicits._

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers
import com.thatdot.quine.graph.{LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.language.ast.Value
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.util.FutureHelpers.SequentialOps

/** QuinePattern implementation of recentNodes procedure.
  *
  * Gets recently touched nodes from shards.
  *
  * Arguments:
  *   count: Integer (optional, defaults to 10) - maximum number of nodes that will be yielded
  *
  * Yields:
  *   node: Node
  */
object RecentNodesProcedure extends QuinePatternProcedure {

  def name: String = "recentNodes"

  def signature: ProcedureSignature = ProcedureSignature(
    outputs = Vector(ProcedureOutput("node")),
    description = "Fetch the specified number of nodes from the in-memory cache",
  )

  def execute(
    arguments: Seq[Value],
    context: ProcedureContext,
  )(implicit ec: ExecutionContext): Future[Seq[Map[String, Value]]] = {

    val limit: Int = arguments match {
      case Seq() => 10
      case Seq(Value.Integer(n)) => n.toInt
      case Seq(other) =>
        throw new IllegalArgumentException(s"$name: count must be an integer, got ${other.getClass.getSimpleName}")
      case _ => throw new IllegalArgumentException(s"$name: requires 0 or 1 arguments, got ${arguments.length}")
    }

    val atTime = context.atTime
    val graph: LiteralOpsGraph = context.graph
    val literalOps = graph.literalOps(context.namespace)
    implicit val timeout: Timeout = context.timeout

    for {
      nodes <- graph.recentNodes(limit, context.namespace, atTime)
      interestingNodes <- nodes.toSeq.filterSequentially(literalOps.nodeIsInteresting(_, atTime))
      nodeValues <- interestingNodes.traverseSequentially(getAsNodeValue(_, context.namespace, atTime, graph))
    } yield nodeValues.map(nv => Map("node" -> nv))
  }

  /** Fetches information needed to build a [[Value.Node]] from a node and time
    *
    * @param qid node ID
    * @param atTime moment in time to query
    * @param graph graph
    */
  def getAsNodeValue(
    qid: QuineId,
    namespace: NamespaceId,
    atTime: Option[Milliseconds],
    graph: LiteralOpsGraph,
  )(implicit timeout: Timeout, ec: ExecutionContext): Future[Value.Node] =
    for {
      (props, maybeLabels) <- graph.literalOps(namespace).getPropsAndLabels(qid, atTime)
    } yield Value.Node(
      id = qid,
      labels = maybeLabels.getOrElse(Set.empty),
      props = Value.Map(
        SortedMap.from(
          props.view.mapValues(CypherAndQuineHelpers.propertyValueToPatternValue),
        ),
      ),
    )
}
