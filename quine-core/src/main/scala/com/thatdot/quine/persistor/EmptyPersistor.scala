package com.thatdot.quine.persistor

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList

import com.thatdot.common.logging.Log.SafeLoggableInterpolator
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.QueryPlan
import com.thatdot.quine.graph.{
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQueryId,
  StandingQueryInfo,
}
import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId

/** Persistence agent which never saves anything
  *
  * Since Quine's bottleneck is usually disk access, this is useful for
  * benchmarking storage-unrelated issues (since it makes all storage operations
  * no-ops).
  */
class EmptyPersistor(
  val persistenceConfig: PersistenceConfig = PersistenceConfig(),
  val namespace: NamespaceId = None,
) extends PersistenceAgent {

  override def emptyOfQuineData(): Future[Boolean] =
    Future.successful(true)

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = {
    logger.warn(
      safe"Attempted to enumerate all node IDs on an empty persistor which never returns anything.",
    )
    Source.empty[QuineId]
  }

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = this.enumerateSnapshotNodeIds()

  override def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Vector[NodeEvent.WithTime[NodeChangeEvent]]] = Future.successful(Vector.empty)

  override def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Vector[NodeEvent.WithTime[DomainIndexEvent]]] = Future.successful(Vector.empty)

  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    Future.unit

  def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = Future.unit

  def persistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    Future.unit

  def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = Future.unit

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]) = Future.unit

  def deleteSnapshots(qid: QuineId): Future[Unit] = Future.unit

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] =
    Future.successful(None)

  def persistStandingQuery(standingQuery: StandingQueryInfo) = Future.unit
  def removeStandingQuery(standingQuery: StandingQueryInfo) = Future.unit
  def getStandingQueries: Future[List[StandingQueryInfo]] = Future.successful(List.empty)

  def getMultipleValuesStandingQueryStates(
    id: QuineId,
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    Future.successful(Map.empty)

  def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]],
  ): Future[Unit] = Future.unit

  def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] = Future.unit

  def containsMultipleValuesStates(): Future[Boolean] = Future.successful(false)

  override def persistQueryPlan(standingQueryId: StandingQueryId, qp: QueryPlan): Future[Unit] = Future.unit

  def getMetaData(key: String): Future[Option[Array[Byte]]] = Future.successful(None)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = Future.successful(Map.empty)

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = Future.unit

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] = Future.unit

  def removeDomainGraphNodes(domainGraphNodes: Set[DomainGraphNodeId]): Future[Unit] = Future.unit

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] = Future.successful(Map.empty)

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] = Future.unit

  def shutdown(): Future[Unit] = Future.unit
  def delete(): Future[Unit] = Future.unit
}

object EmptyPersistor extends EmptyPersistor(PersistenceConfig(), None)
