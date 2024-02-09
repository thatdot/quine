package com.thatdot.quine.persistor

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList

import com.thatdot.quine.graph.{
  BaseGraph,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}

/** Wrapper for a persistor that checks that some invariants are upheld:
  *
  *   - for every node: every event occurs at a unique time
  *   - for every node: every snapshot occurs at a unique time
  */
class InvariantWrapper(wrapped: PersistenceAgent) extends WrappedPersistenceAgent(wrapped) with PersistenceAgent {

  private val events = new ConcurrentHashMap[QuineId, ConcurrentHashMap[EventTime, NodeEvent]]
  private val snapshots = new ConcurrentHashMap[QuineId, ConcurrentHashMap[EventTime, Array[Byte]]]

  val namespace: NamespaceId = wrapped.namespace

  override def emptyOfQuineData(): Future[Boolean] =
    if (events.isEmpty && snapshots.isEmpty) wrapped.emptyOfQuineData()
    else Future.successful(false)

  /** Persist [[NodeChangeEvent]] values. */
  def persistNodeChangeEvents(
    id: QuineId,
    eventsWithTime: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]
  ): Future[Unit] = {
    for { NodeEvent.WithTime(event, atTime) <- eventsWithTime.toList } {
      val previous = events
        .computeIfAbsent(id, _ => new ConcurrentHashMap[EventTime, NodeEvent]())
        .put(atTime, event)
      assert(
        (previous eq null) || (previous eq event),
        s"Duplicate events at node id $id and time $atTime: $event & $previous"
      )
    }
    wrapped.persistNodeChangeEvents(id, eventsWithTime)
  }

  /** Persist [[DomainIndexEvent]] values. */
  def persistDomainIndexEvents(
    id: QuineId,
    eventsWithTime: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]
  ): Future[Unit] = {
    for { NodeEvent.WithTime(event, atTime) <- eventsWithTime.toList } {
      val previous = events
        .computeIfAbsent(id, _ => new ConcurrentHashMap[EventTime, NodeEvent]())
        .put(atTime, event)
      assert(
        (previous eq null) || (previous eq event),
        s"Duplicate events at node id $id and time $atTime: $event & $previous"
      )
    }
    wrapped.persistDomainIndexEvents(id, eventsWithTime)
  }

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] =
    wrapped.getNodeChangeEventsWithTime(id, startingAt, endingAt)

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] =
    wrapped.getDomainIndexEventsWithTime(id, startingAt, endingAt)

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = wrapped.enumerateJournalNodeIds()

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = wrapped.enumerateSnapshotNodeIds()

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    val previous = snapshots
      .computeIfAbsent(id, _ => new ConcurrentHashMap[EventTime, Array[Byte]]())
      .put(atTime, state)
    assert(
      (previous eq null) || (previous eq state),
      s"Duplicate snapshots at node id $id and time $atTime: $state & $previous"
    )
    wrapped.persistSnapshot(id, atTime, state)
  }

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] =
    wrapped.getLatestSnapshot(id, upToTime)

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrapped.persistStandingQuery(standingQuery)

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] = wrapped.removeStandingQuery(standingQuery)

  def getStandingQueries: Future[List[StandingQuery]] = wrapped.getStandingQueries

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    wrapped.getMultipleValuesStandingQueryStates(id)

  override def deleteSnapshots(qid: QuineId): Future[Unit] = wrapped.deleteSnapshots(qid)
  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = wrapped.deleteNodeChangeEvents(qid)
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = wrapped.deleteDomainIndexEvents(qid)
  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] =
    wrapped.deleteMultipleValuesStandingQueryStates(id)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = wrapped.getAllMetaData()
  def getMetaData(key: String): Future[Option[Array[Byte]]] = wrapped.getMetaData(key)
  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = wrapped.setMetaData(key, newValue)

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    wrapped.persistDomainGraphNodes(domainGraphNodes)
  def removeDomainGraphNodes(domainGraphNodes: Set[DomainGraphNodeId]): Future[Unit] = wrapped.removeDomainGraphNodes(
    domainGraphNodes
  )
  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] = wrapped.getDomainGraphNodes()

  override def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = wrapped.setMultipleValuesStandingQueryState(standingQuery, id, standingQueryId, state)

  override def declareReady(graph: BaseGraph): Unit = wrapped.declareReady(graph)

  def shutdown(): Future[Unit] = wrapped.shutdown()

  def persistenceConfig: PersistenceConfig = wrapped.persistenceConfig

  /** Delete all [DomainIndexEvent]]s by their held DgnId. Note that depending on the storage implementation
    * this may be an extremely slow operation.
    *
    * @param dgnId
    */
  override def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    wrapped.deleteDomainIndexEventsByDgnId(dgnId)

  def delete(): Future[Unit] = wrapped.delete()
}
