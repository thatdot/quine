package com.thatdot.quine.graph

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.messaging.ShardMessage.PurgeNode
import com.thatdot.quine.graph.messaging.{BaseMessage, QuineIdAtTime}
import com.thatdot.quine.model._

/** Functionality for directly modifying the runtime property graph. Always prefer using something else. */
trait LiteralOpsGraph extends BaseGraph {
  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[LiteralOpsGraph, behavior.LiteralCommandBehavior]

  // TODO: should we keep this object indirection? It serves no purpose other than namespacing...
  object literalOps {
    def purgeNode(qid: QuineId)(implicit timeout: Timeout): Future[BaseMessage.Done.type] = {
      requiredGraphIsReady()
      relayAsk(shardFromNode(qid).quineRef, PurgeNode(qid, _)).flatten
    }

    /** Assemble together debugging information about a node's internal state
      *
      * @note this is only meant for debugging system internals
      * @param node   which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return internal node state
      */
    def logState(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout
    ): Future[NodeInternalState] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, atTime), LogInternalState).flatten
    }

    def getSqResults(node: QuineId)(implicit timeout: Timeout): Future[SqStateResults] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), GetSqState)
    }

    def deleteNode(node: QuineId)(implicit timeout: Timeout): Future[Unit] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), DeleteNodeCommand(true, _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    def getProps(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout
    ): Future[Map[Symbol, PropertyValue]] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      (getPropsAndLabels(node, atTime) map { case (x, _) =>
        x // keeping only properties
      })(ExecutionContexts.parasitic)
    }

    /** Get all properties and labels of a node
      *
      * @param node   which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return map of all of the properties and set of all of the labels
      */
    def getPropsAndLabels(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout
    ): Future[(Map[Symbol, PropertyValue], Option[Set[Symbol]])] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      val futureSource = relayAsk(QuineIdAtTime(node, atTime), GetPropertiesCommand)
      Source
        .futureSource(futureSource)
        .runFold((Map.empty[Symbol, PropertyValue], Set.empty[Symbol])) {
          case ((propertiesAccumulator, labelsAccumulator), message) =>
            message match {
              case PropertyMessage(Left((key, value))) => (propertiesAccumulator + (key -> value), labelsAccumulator)
              case PropertyMessage(Right(value)) => (propertiesAccumulator, labelsAccumulator + value)
            }
        }
        .map {
          case (a, c) if c.isEmpty => (a, None)
          case (a, c) => (a, Some(c))
        }(shardDispatcherEC)
    }

    /** Set node label to multiple values
      *
      * @param node   on which node the label should be set
      * @param labels labels to set
      */
    def setLabels(node: QuineId, labels: Set[String])(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), SetLabels(labels.map(Symbol(_)), _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    /** Set node label to a single value
      *
      * @param node  on which node the label should be set
      * @param label label to set
      */
    def setLabel(node: QuineId, label: String)(implicit
      timeout: Timeout
    ): Future[Unit] = setLabels(node, Set(label))

    /** Set a single property on a node
      *
      * @param node  on which node the property should be set
      * @param key   key of the property to set
      * @param value property value to set
      */
    def setProp(node: QuineId, key: String, value: QuineValue)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      relayAsk(QuineIdAtTime(node, None), SetPropertyCommand(Symbol(key), PropertyValue(value), _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    // Warning: make _sure_ the bytes you pass in here are correct. When in doubt, use [[setProp]]
    def setPropBytes(node: QuineId, key: String, value: Array[Byte])(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      val propVal = PropertyValue.fromBytes(value)
      relayAsk(QuineIdAtTime(node, None), SetPropertyCommand(Symbol(key), propVal, _)).flatten
        .map(_ => ())(ExecutionContexts.parasitic)
    }

    def removeProp(node: QuineId, key: String)(implicit timeout: Timeout): Future[Unit] = {
      requireCompatibleNodeType()
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
      requireCompatibleNodeType()
      requiredGraphIsReady()
      val halfEdgesSource =
        relayAsk(QuineIdAtTime(node, atTime), GetHalfEdgesCommand(withType, withDir, withId, withLimit, _))
      Source.futureSource(halfEdgesSource).map(_.halfEdge).runWith(Sink.collection)
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
      requireCompatibleNodeType()
      requiredGraphIsReady()
      getHalfEdges(node, withType, withDir, withId, withLimit, atTime)
        .flatMap(halfEdges =>
          Future
            .traverse(halfEdges) { (h: HalfEdge) =>
              getHalfEdges(
                node = h.other,
                withType = Some(h.edgeType),
                withDir = Some(h.direction.reverse),
                withId = Some(node),
                withLimit = Some(1), // we just care about `nonEmpty`
                atTime = atTime
              ).map(otherSide => if (otherSide.nonEmpty) Some(h) else None)(shardDispatcherEC)
            }(implicitly, shardDispatcherEC)
        )(shardDispatcherEC)
        .map(filtered => filtered.collect { case Some(completeEdges) => completeEdges })(shardDispatcherEC)
    }

    def addEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
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
      one.zipWith(two)((_, _) => ())(shardDispatcherEC)
    }

    def removeEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
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
      one.zipWith(two)((_, _) => ())(shardDispatcherEC)
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
