package com.thatdot.quine.graph

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.util.Timeout

import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.messaging.QuineIdAtTime
import com.thatdot.quine.model._

/** Functionality for directly modifying the runtime property graph. Always prefer using something else. */
trait LiteralOpsGraph extends BaseGraph {

  requireBehavior(classOf[LiteralOpsGraph].getSimpleName, classOf[behavior.LiteralCommandBehavior])

  // TODO: should we keep this object indirection? It serves no purpose other than namespacing...
  object literalOps {

    /** Assemble together debugging information about a node's internal state
      *
      * @note this is only meant for debugging system internals
      * @param node which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return internal node state
      */
    def logState(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout
    ): Future[NodeInternalState] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, atTime), LogInternalState).flatten
    }

    def deleteNode(node: QuineId)(implicit timeout: Timeout): Future[Unit] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), DeleteNodeCommand(true, _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    def getProps(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout
    ): Future[Map[Symbol, PropertyValue]] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, atTime), GetRawPropertiesCommand)
        .map(_.properties)(ExecutionContexts.parasitic)
    }

    /** Set a single property on a node
      *
      * @param node on which node the property should be set
      * @param key key of the property to set
      * @param value property value to set
      */
    def setProp(node: QuineId, key: String, value: QuineValue)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), SetPropertyCommand(Symbol(key), PropertyValue(value), _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    // Warning: make _sure_ the bytes you pass in here are correct. When in doubt, use [[setProp]]
    def setPropBytes(node: QuineId, key: String, value: Array[Byte])(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requiredGraphIsReady()
      val propVal = PropertyValue.fromBytes(value)
      relayAsk(QuineIdAtTime(node, None), SetPropertyCommand(Symbol(key), propVal, _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    def removeProp(node: QuineId, key: String)(implicit timeout: Timeout): Future[Unit] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), RemovePropertyCommand(Symbol(key), _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    // NB: doesn't check that the other half of the edge exists
    def getHalfEdges(
      node: QuineId,
      withType: Option[Symbol] = None,
      withDir: Option[EdgeDirection] = None,
      withId: Option[QuineId] = None,
      withLimit: Option[Int] = None,
      atTime: Option[Milliseconds] = None
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, atTime), GetHalfEdgesCommand(withType, withDir, withId, withLimit, _))
        .map(_.halfEdges)(ExecutionContexts.parasitic)
    }

    // NB: Checks that the other half of the edge exists
    def getEdges(
      node: QuineId,
      withType: Option[Symbol] = None,
      withDir: Option[EdgeDirection] = None,
      withId: Option[QuineId] = None,
      withLimit: Option[Int] = None,
      atTime: Option[Milliseconds] = None
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requiredGraphIsReady()
      for {
        halfEdges <- getHalfEdges(node, withType, withDir, withId, withLimit, atTime)
        filtered <- Future.traverse(halfEdges) { (h: HalfEdge) =>
          getHalfEdges(
            node = h.other,
            withType = Some(h.edgeType),
            withDir = Some(h.direction.reverse),
            withId = Some(node),
            withLimit = Some(1), // we just care about `nonEmpty`
            atTime = atTime
          ).map(otherSide => if (otherSide.nonEmpty) Some(h) else None)
        }
      } yield filtered.collect { case Some(completeEdges) => completeEdges }
    }

    def addEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requiredGraphIsReady()
      val edgeDir = if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected
      val one = relayAsk(
        QuineIdAtTime(from, None),
        AddHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir, to), _)
      )
      val two = relayAsk(
        QuineIdAtTime(to, None),
        AddHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir.reverse, from), _)
      )
      one.zipWith(two)((_, _) => ())
    }

    def removeEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requiredGraphIsReady()
      val edgeDir = if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected
      val one = relayAsk(
        QuineIdAtTime(from, None),
        RemoveHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir, to), _)
      )
      val two = relayAsk(
        QuineIdAtTime(to, None),
        RemoveHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir.reverse, from), _)
      )
      one.zipWith(two)((_, _) => ())
    }

    def mergeNode(fromThat: QuineId, intoThis: QuineId)(implicit timeout: Timeout): Future[QuineId] = {
      requiredGraphIsReady()
      for {
        id <- relayAsk(
          QuineIdAtTime(intoThis, None),
          GetNodeId
        ) // Get the final node ID, in the case of multiple recursive merges.
        _ <- relayAsk(QuineIdAtTime(fromThat, None), MergeIntoNodeCommand(id.qid, _))
      } yield id.qid
    }
  }
}

object LiteralOpsGraph {

  /** Check if a graph supports literal operations and refine it if possible */
  @throws[IllegalArgumentException]("if the graph does not implement LiteralOperations")
  def getOrThrow(context: => String, graph: BaseGraph): LiteralOpsGraph =
    if (graph.isInstanceOf[LiteralOpsGraph]) {
      graph.asInstanceOf[LiteralOpsGraph]
    } else {
      throw new IllegalArgumentException(s"$context requires a graph that implements LiteralOperations")
    }
}
