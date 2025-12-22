package com.thatdot.quine.graph

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.messaging.ShardMessage.PurgeNode
import com.thatdot.quine.graph.messaging.{BaseMessage, SpaceTimeQuineId}
import com.thatdot.quine.model._

/** Functionality for directly modifying the runtime property graph. Always prefer using something else. */
trait LiteralOpsGraph extends BaseGraph {
  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[LiteralOpsGraph, behavior.LiteralCommandBehavior]

  def literalOps(namespaceId: NamespaceId): LiteralOps = LiteralOps(namespaceId)

  case class LiteralOps(namespace: NamespaceId) {
    def purgeNode(qid: QuineId)(implicit timeout: Timeout): Future[BaseMessage.Done.type] =
      relayAsk(shardFromNode(qid).quineRef, PurgeNode(namespace, qid, _)).flatten

    /** Assemble together debugging information about a node's internal state
      *
      * @note this is only meant for debugging system internals
      * @param node   which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return internal node state
      */
    def logState(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout,
    ): Future[NodeInternalState] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, atTime), LogInternalState).flatten
    }

    def getSqResults(node: QuineId)(implicit timeout: Timeout): Future[SqStateResults] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, None), GetSqState)
    }

    /** Check if a node is "interesting" (has at least one property or edge).
      * Used to filter out empty nodes from scan results.
      *
      * @param node   which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return true if the node has properties or edges
      */
    def nodeIsInteresting(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout,
    ): Future[Boolean] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, atTime), CheckNodeIsInteresting)
        .map(_.isInteresting)(shardDispatcherEC)
    }

    def deleteNode(node: QuineId)(implicit timeout: Timeout): Future[Unit] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, None), DeleteNodeCommand(deleteEdges = true, _)).flatten
        .map(_ => ())(ExecutionContext.parasitic)
    }

    def getProps(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout,
    ): Future[Map[Symbol, PropertyValue]] = {
      requireCompatibleNodeType()
      (getPropsAndLabels(node, atTime) map { case (x, _) =>
        x // keeping only properties
      })(ExecutionContext.parasitic)
    }

    /** Get all properties and labels of a node
      *
      * @param node   which node to query
      * @param atTime the historical moment to query, or None for the moving present
      * @return map of all of the properties and set of all of the labels
      */
    def getPropsAndLabels(node: QuineId, atTime: Option[Milliseconds] = None)(implicit
      timeout: Timeout,
    ): Future[(Map[Symbol, PropertyValue], Option[Set[Symbol]])] = {
      requireCompatibleNodeType()
      val futureSource = relayAsk(SpaceTimeQuineId(node, namespace, atTime), GetPropertiesCommand)
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
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, None), SetLabels(labels.map(Symbol(_)), _)).flatten
        .map(_ => ())(ExecutionContext.parasitic)
    }

    /** Set node label to a single value
      *
      * @param node  on which node the label should be set
      * @param label label to set
      */
    def setLabel(node: QuineId, label: String)(implicit
      timeout: Timeout,
    ): Future[Unit] = setLabels(node, Set(label))

    /** Set a single property on a node
      *
      * @param node  on which node the property should be set
      * @param key   key of the property to set
      * @param value property value to set
      */
    def setProp(node: QuineId, key: String, value: QuineValue)(implicit
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      relayAsk(
        SpaceTimeQuineId(node, namespace, None),
        SetPropertyCommand(Symbol(key), PropertyValue(value), _),
      ).flatten
        .map(_ => ())(ExecutionContext.parasitic)
    }

    // Warning: make _sure_ the bytes you pass in here are correct. When in doubt, use [[setProp]]
    def setPropBytes(node: QuineId, key: String, value: Array[Byte])(implicit
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      val propVal = PropertyValue.fromBytes(value)
      relayAsk(SpaceTimeQuineId(node, namespace, None), SetPropertyCommand(Symbol(key), propVal, _)).flatten
        .map(_ => ())(ExecutionContext.parasitic)
    }

    def removeProp(node: QuineId, key: String)(implicit timeout: Timeout): Future[Unit] = {
      requireCompatibleNodeType()
      relayAsk(SpaceTimeQuineId(node, namespace, None), RemovePropertyCommand(Symbol(key), _)).flatten
        .map(_ => ())(ExecutionContext.parasitic)
    }

    // NB: doesn't check that the other half of the edge exists
    def getHalfEdges(
      node: QuineId,
      withType: Option[Symbol] = None,
      withDir: Option[EdgeDirection] = None,
      withId: Option[QuineId] = None,
      withLimit: Option[Int] = None,
      atTime: Option[Milliseconds] = None,
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requireCompatibleNodeType()
      val halfEdgesSource =
        relayAsk(
          SpaceTimeQuineId(node, namespace, atTime),
          GetHalfEdgesCommand(withType, withDir, withId, withLimit, _),
        )
      Source.futureSource(halfEdgesSource).map(_.halfEdge).runWith(Sink.collection)
    }

    /** Get half edges from a node with Set-based filtering. Each Set filters edges INDEPENDENTLY. So the returned
      * collection size can be up to the size of the Cartesian Product of the cardinality of all non-empty sets.
      *
      * Note: this does not check that the other half of the edge exists
      *
      * @param node node to query
      * @param edgeTypes set of allowed edge types (empty = no filter)
      * @param directions set of allowed directions (empty = no filter)
      * @param otherIds set of allowed destination node IDs (empty = no filter)
      * @param atTime optional historical time
      * @return set of matching half edges
      */
    def getHalfEdgesFiltered(
      node: QuineId,
      edgeTypes: Set[Symbol] = Set.empty,
      directions: Set[EdgeDirection] = Set.empty,
      otherIds: Set[QuineId] = Set.empty,
      atTime: Option[Milliseconds] = None,
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requireCompatibleNodeType()
      val halfEdgesSource =
        relayAsk(
          SpaceTimeQuineId(node, namespace, atTime),
          GetHalfEdgesFilteredCommand(edgeTypes, directions, otherIds, _),
        )
      Source.futureSource(halfEdgesSource).map(_.halfEdge).runWith(Sink.collection)
    }

    /** Validate a set of expected half edges against what actually exists on a target node.
      * Returns the set of edges that are expected but DO NOT exist on the target node.
      *
      * @param targetNode the node to query for validation
      * @param expectedEdges the set of half edges we expect to find on the target node
      * @param atTime optional historical time
      * @return set of expected edges that are missing from the target node
      */
    def validateAndReturnMissingHalfEdges(
      targetNode: QuineId,
      expectedEdges: Set[HalfEdge],
      atTime: Option[Milliseconds] = None,
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requireCompatibleNodeType()
      relayAsk(
        SpaceTimeQuineId(targetNode, namespace, atTime),
        ValidateAndReturnMissingHalfEdgesCommand(expectedEdges, _),
      ).map(_.missingEdges)(shardDispatcherEC)
    }

    // NB: Checks that the other half of the edge exists
    def getEdges(
      node: QuineId,
      withType: Option[Symbol] = None,
      withDir: Option[EdgeDirection] = None,
      withId: Option[QuineId] = None,
      withLimit: Option[Int] = None,
      atTime: Option[Milliseconds] = None,
    )(implicit timeout: Timeout): Future[Set[HalfEdge]] = {
      requireCompatibleNodeType()
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
                atTime = atTime,
              ).map(otherSide => if (otherSide.nonEmpty) Some(h) else None)(shardDispatcherEC)
            }(implicitly, shardDispatcherEC),
        )(shardDispatcherEC)
        .map(filtered => filtered.collect { case Some(completeEdges) => completeEdges })(shardDispatcherEC)
    }

    def addEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      val edgeDir = if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected
      val one = relayAsk(
        SpaceTimeQuineId(from, namespace, None),
        AddHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir, to), _),
      )
      val two = relayAsk(
        SpaceTimeQuineId(to, namespace, None),
        AddHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir.reverse, from), _),
      )
      one.zipWith(two)((_, _) => ())(shardDispatcherEC)
    }

    def removeEdge(from: QuineId, to: QuineId, label: String, isDirected: Boolean = true)(implicit
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      val edgeDir = if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected
      val one = relayAsk(
        SpaceTimeQuineId(from, namespace, None),
        RemoveHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir, to), _),
      )
      val two = relayAsk(
        SpaceTimeQuineId(to, namespace, None),
        RemoveHalfEdgeCommand(HalfEdge(Symbol(label), edgeDir.reverse, from), _),
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
