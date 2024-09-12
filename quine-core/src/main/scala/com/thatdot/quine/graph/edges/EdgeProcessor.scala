package com.thatdot.quine.graph.edges

import scala.concurrent.Future

import cats.data.NonEmptyList

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.metrics.BinaryHistogramCounter
import com.thatdot.quine.graph.{CostToSleep, EdgeEvent, EventTime}
import com.thatdot.quine.model._
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

//abstract class DontCareWrapper(edges: AbstractEdgeCollectionView[F forSome { type F[_] }, S forSome { type S[_] }])
//    extends EdgeProcessor(edges)

/** A processor for edge events that can be applied to a node.
  * Responsible for maintaining the node's edge collection and derived metadata such as metrics and cost to sleep, when
  * affected by a change to the state of edges.
  */
abstract class EdgeProcessor(
  edges: AbstractEdgeCollectionView,
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
    atTime: () => EventTime,
  )(implicit logConfig: LogConfig): Future[Unit]

  /** Apply a single edge event to the edge collection without triggering standing queries
    */
  def updateEdgeCollection(event: EdgeEvent)(implicit logConfig: LogConfig): Unit

  import edges.{toSyncFuture, toSyncStream}
  def size: Int = toSyncFuture(edges.size)

  def all: Iterator[HalfEdge] = toSyncStream(edges.all)

  def toSet: Set[HalfEdge] = all.toSet

  protected[graph] def toSerialize: Iterable[HalfEdge] = edges.toSerialize

  def nonEmpty: Boolean = toSyncFuture(edges.nonEmpty)

  def matching(edgeType: Symbol): Iterator[HalfEdge] = toSyncStream(edges.edgesByType(edgeType))

  def matching(edgeType: Symbol, direction: EdgeDirection): Iterator[HalfEdge] =
    toSyncStream(edges.qidsByTypeAndDirection(edgeType, direction)).map(HalfEdge(edgeType, direction, _))

  def matching(edgeType: Symbol, id: QuineId): Iterator[HalfEdge] =
    toSyncStream(edges.directionsByTypeAndQid(edgeType, id)).map(HalfEdge(edgeType, _, id))

  def matching(edgeType: Symbol, direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] = {
    val edge = HalfEdge(edgeType, direction, id)
    if (toSyncFuture(edges.contains(edge))) Iterator.single(edge) else Iterator.empty
  }
  def matching(direction: EdgeDirection): Iterator[HalfEdge] = toSyncStream(edges.edgesByDirection(direction))

  def matching(direction: EdgeDirection, id: QuineId): Iterator[HalfEdge] =
    toSyncStream(edges.typesByDirectionAndQid(direction, id)).map(HalfEdge(_, direction, id))

  def matching(id: QuineId): Iterator[HalfEdge] = toSyncStream(edges.edgesByQid(id)).map(_.toHalfEdge(id))

  def matching(genEdge: GenericEdge): Iterator[HalfEdge] = matching(genEdge.edgeType, genEdge.direction)

  def contains(edge: HalfEdge): Boolean = toSyncFuture(edges.contains(edge))

  def hasUniqueGenEdges(requiredEdges: Set[DomainEdge], thisQid: QuineId): Boolean =
    toSyncFuture(edges.hasUniqueGenEdges(requiredEdges))

  /** Callback for actions to be performed when the node successfully goes to sleep. May be called from any thread.
    * The implementation should update any relevant metrics to reflect the node's sleep state.
    */
  def onSleep(): Unit
}

abstract class SynchronousEdgeProcessor(
  edgeCollection: SyncEdgeCollection,
  qid: QuineId,
  costToSleep: CostToSleep,
  nodeEdgesCounter: BinaryHistogramCounter,
)(implicit idProvider: QuineIdProvider)
    extends EdgeProcessor(edgeCollection)
    with LazySafeLogging {

  implicit protected def logConfig: LogConfig

  /** Fast check for if a number is a power of 2 */
  private def isPowerOfTwo(n: Int): Boolean = (n & (n - 1)) == 0

  private[this] def edgeEventHasEffect(event: EdgeEvent): Boolean = event match {
    case EdgeAdded(edge) => !edgeCollection.contains(edge)
    case EdgeRemoved(edge) => edgeCollection.contains(edge)
  }

  protected def journalAndApplyEffects(
    effectingEvents: NonEmptyList[EdgeEvent],
    produceTimestamp: () => EventTime,
  ): Future[Unit]

  def processEdgeEvents(events: List[EdgeEvent], atTime: () => EventTime)(implicit logConfig: LogConfig): Future[Unit] =
    NonEmptyList.fromList(events.filter(edgeEventHasEffect)) match {
      case Some(effectingEvents) => journalAndApplyEffects(effectingEvents, atTime)
      case None => Future.unit
    }

  def updateEdgeCollection(event: EdgeEvent)(implicit logConfig: LogConfig): Unit = {

    val oldSize = edgeCollection.size
    event match {
      case EdgeEvent.EdgeAdded(edge) =>
        edgeCollection.addEdge(edge)
        if (oldSize > 7 && isPowerOfTwo(oldSize)) costToSleep.incrementAndGet()

        val edgeCollectionSizeWarningInterval = 10000
        if ((oldSize + 1) % edgeCollectionSizeWarningInterval == 0)
          logger.warn(log"Node ${Safe(qid.pretty)} has: ${Safe(oldSize + 1)} edges")
        nodeEdgesCounter.increment(previousCount = oldSize)
      case EdgeEvent.EdgeRemoved(edge) =>
        edgeCollection.removeEdge(edge)
        nodeEdgesCounter.decrement(previousCount = oldSize)
    }
  }

  final def onSleep(): Unit =
    nodeEdgesCounter.bucketContaining(edgeCollection.size).dec()

}
