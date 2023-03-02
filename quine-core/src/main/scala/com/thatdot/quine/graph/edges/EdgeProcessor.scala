package com.thatdot.quine.graph.edges

import scala.concurrent.Future

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.{BinaryHistogramCounter, CostToSleep, EdgeEvent, EventTime, MemberIdx}
import com.thatdot.quine.model._

abstract class EdgeProcessor(
  edges: EdgeCollection
) extends EdgeCollectionView {

  /** Apply edge events to a node including:
    * - derived/materialized state like edge collections and DGN bookkeeping
    * - source-of-truth state like persisted journals
    * - signalling relevant standing queries to update
    * - updating the node's cost to sleep and metrics related to edge state
    *
    * @param events a list of edgeevents to apply
    *               INV: no more than 1 event in `events` refers to the same edge
    * @param atTime a (possibly side-effecting) generator for unique EventTime timestamps within the same message
    *               boundary
    * @return       a Future that completes when all requested updates have been applied to state owned by this
    *               node, both derived (eg standing queries, edge collection, etc) and source-of-truth (eg journals)
    */
  def processEdgeEvents(
    events: List[EdgeEvent],
    atTime: () => EventTime
  ): Future[Unit]

  /** Apply a single edge event to the edge collection without causing any other side effects (SQs, metrics
    * upkeep, etc).
    */
  def updateEdgeCollection(event: EdgeEvent): Unit

  def size: MemberIdx = edges.size

  def all: Iterator[HalfEdge] = edges.all

  def toSet: Set[HalfEdge] = edges.toSet

  protected[graph] def toSerialize: Iterable[HalfEdge] = edges.toSerialize

  def nonEmpty: Boolean = edges.nonEmpty

  def matching(edgeType: Symbol): Iterator[HalfEdge] = edges.matching(edgeType)

  def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] = edges.matching(edgeType, direction)

  def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] = edges.matching(edgeType, id)

  def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] =
    edges.matching(edgeType, direction, id)

  def matching(direction: EdgeDirection): Iterator[HalfEdge] = edges.matching(direction)

  def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = edges.matching(direction, id)

  def matching(id: QuineId): Iterator[HalfEdge] = edges.matching(id)

  def matching(genEdge: GenericEdge): Iterator[HalfEdge] = edges.matching(genEdge)

  def contains(edge: HalfEdge): Boolean = edges.contains(edge)

  def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean =
    edges.hasUniqueGenEdges(requiredEdges, thisQid)
}

abstract class SynchronousEdgeProcessor(
  edges: EdgeCollection,
  qid: QuineId,
  costToSleep: CostToSleep,
  nodeEdgesCounter: BinaryHistogramCounter
)(implicit idProvider: QuineIdProvider)
    extends EdgeProcessor(edges)
    with LazyLogging {

  /** Fast check for if a number is a power of 2 */
  private def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  private[this] def edgeEventHasEffect(event: EdgeEvent): Boolean = event match {
    case EdgeAdded(edge) => !edges.contains(edge)
    case EdgeRemoved(edge) => edges.contains(edge)
  }

  protected def journalAndApplyEffects(
    effectingEvents: NonEmptyList[EdgeEvent],
    produceTimestamp: () => EventTime
  ): Future[Unit]

  def processEdgeEvents(events: List[EdgeEvent], atTime: () => EventTime): Future[Unit] =
    NonEmptyList.fromList(events.filter(edgeEventHasEffect)) match {
      case Some(effectingEvents) => journalAndApplyEffects(effectingEvents, atTime)
      case None => Future.unit
    }

  def updateEdgeCollection(event: EdgeEvent): Unit = event match {
    case EdgeEvent.EdgeAdded(edge) =>
      edges.addEdgeSync(edge)
    case EdgeEvent.EdgeRemoved(edge) =>
      edges.removeEdgeSync(edge)
  }

  /** Apply all effects (see [[processEdgeEvents]]) of a single edge event
    */
  protected[this] def applyEdgeEffect(event: EdgeEvent): Unit = {
    val oldSize = edges.size
    updateEdgeCollection(event)
    event match {
      case EdgeEvent.EdgeAdded(_) =>
        if (oldSize > 7 && isPowerOfTwo(oldSize)) costToSleep.incrementAndGet()

        val edgeCollectionSizeWarningInterval = 10000
        if ((oldSize + 1) % edgeCollectionSizeWarningInterval == 0)
          logger.warn(s"Node ${qid.pretty} has: ${oldSize + 1} edges")
        nodeEdgesCounter.increment(previousCount = oldSize)
      case EdgeEvent.EdgeRemoved(_) =>
        nodeEdgesCounter.decrement(previousCount = oldSize)
    }
  }

}
