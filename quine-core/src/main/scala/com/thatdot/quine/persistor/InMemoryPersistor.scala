package com.thatdot.quine.persistor

import java.util.concurrent._

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList

import com.thatdot.quine.graph.cypher.QuinePattern
import com.thatdot.quine.graph.{
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQueryId,
  StandingQueryInfo
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}
import com.thatdot.quine.util.Log._

/** Persistence implementation which actually just keeps everything in memory
  *
  * This is useful primarily as a debugging or testing mechanism - it should
  * behave like other persistors with the exception that it will consume
  * increasing amounts of memory. It is also convenient as a code-explanation
  * of what the persistor API is supposed to be doing.
  *
  * @param journals map storing all node events
  * @param snapshots map storing all snapshots
  * @param standingQueries set storing all standing queries
  * @param multipleValuesStandingQueryStates map storing all standing query states
  * @param metaData map storing all meta data
  * @param persistenceConfig persistence options
  */
class InMemoryPersistor(
  journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] = new ConcurrentHashMap(),
  domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
    new ConcurrentHashMap(),
  snapshots: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]] = new ConcurrentHashMap(),
  standingQueries: ConcurrentMap[StandingQueryId, StandingQueryInfo] = new ConcurrentHashMap(),
  multipleValuesStandingQueryStates: ConcurrentMap[
    QuineId,
    ConcurrentMap[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[
      Byte
    ]]
  ] = new ConcurrentHashMap(),
  quinePatterns: ConcurrentMap[StandingQueryId, QuinePattern] = new ConcurrentHashMap(),
  metaData: ConcurrentMap[String, Array[Byte]] = new ConcurrentHashMap(),
  domainGraphNodes: ConcurrentMap[DomainGraphNodeId, DomainGraphNode] = new ConcurrentHashMap(),
  val persistenceConfig: PersistenceConfig = PersistenceConfig(),
  val namespace: NamespaceId = None
)(implicit val logConfig: LogConfig)
    extends PersistenceAgent {

  private val allTables =
    Seq(journals, domainIndexEvents, snapshots, standingQueries, multipleValuesStandingQueryStates, domainGraphNodes)
  override def emptyOfQuineData(): Future[Boolean] =
    Future.successful(
      allTables.forall(_.isEmpty)
    )

  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] = {
    for { NodeEvent.WithTime(event, atTime) <- events.toList } journals
      .computeIfAbsent(id, (_: QuineId) => new ConcurrentSkipListMap())
      .put(atTime, event)
    Future.unit
  }

  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = {
    journals.remove(qid)
    Future.unit
  }

  def persistDomainIndexEvents(
    id: QuineId,
    events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]
  ): Future[Unit] = {
    for { NodeEvent.WithTime(event, atTime) <- events.toList } domainIndexEvents
      .computeIfAbsent(id, (_: QuineId) => new ConcurrentSkipListMap())
      .put(atTime, event)
    Future.unit
  }

  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = {
    domainIndexEvents.remove(qid)
    Future.unit
  }

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = {
    val eventsMap = journals.get(id)
    Future.successful(
      if (eventsMap == null)
        Iterable.empty
      else
        eventsMap
          .subMap(startingAt, true, endingAt, true)
          .entrySet()
          .iterator
          .asScala
          .flatMap(a => Iterator.single(NodeEvent.WithTime(a.getValue, a.getKey)))
          .toSeq
    )
  }

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] = {
    val eventsMap = domainIndexEvents.get(id)
    Future.successful(
      if (eventsMap == null)
        Iterable.empty
      else
        eventsMap
          .subMap(startingAt, true, endingAt, true)
          .entrySet()
          .iterator
          .asScala
          .flatMap(a => Iterator.single(NodeEvent.WithTime(a.getValue, a.getKey)))
          .toSeq
    )
  }

  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] = {

    domainIndexEvents.asScala.map { case (_: QuineId, m: ConcurrentNavigableMap[EventTime, DomainIndexEvent]) =>
      m.entrySet()
        .removeIf(entry =>
          entry.getValue match {
            case event: DomainIndexEvent if event.dgnId == dgnId => true
            case _ => false
          }
        )
    }

    Future.unit
  }

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    Source.fromIterator(() => journals.keySet().iterator.asScala)

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    Source.fromIterator(() => snapshots.keySet().iterator.asScala)

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    snapshots
      .computeIfAbsent(id, (_: QuineId) => new ConcurrentSkipListMap())
      .put(atTime, state)
    Future.unit
  }

  override def deleteSnapshots(qid: QuineId): Future[Unit] = {
    snapshots.remove(qid)
    Future.unit
  }

  def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[Array[Byte]]] = {
    val snapshotsMap = snapshots.get(id)
    Future.successful(
      if (snapshotsMap == null) None
      else
        Option
          .apply(snapshotsMap.floorEntry(upToTime))
          .map(e => e.getValue)
    )
  }

  def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = {
    standingQueries.put(standingQuery.id, standingQuery)
    Future.unit
  }

  def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = {
    standingQueries.remove(standingQuery.id)
    Future.unit
  }

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    Future.successful(
      Option
        .apply(multipleValuesStandingQueryStates.get(id))
        .fold(Map.empty[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]])(m => m.asScala.toMap)
    )

  def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = {
    state match {
      case Some(bytes) =>
        multipleValuesStandingQueryStates
          .computeIfAbsent(id, (_: QuineId) => new ConcurrentHashMap())
          .put((standingQuery, standingQueryId), bytes)

      case None =>
        Option
          .apply(multipleValuesStandingQueryStates.get(id))
          .map(states => states.remove((standingQuery, standingQueryId)))
    }
    Future.unit
  }

  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] = {
    multipleValuesStandingQueryStates.remove(id)
    Future.unit
  }

  def containsMultipleValuesStates(): Future[Boolean] = Future.successful(!multipleValuesStandingQueryStates.isEmpty)

  def getStandingQueries: Future[List[StandingQueryInfo]] =
    Future.successful(standingQueries.values.asScala.toList)

  override def persistQuinePattern(standingQueryId: StandingQueryId, qp: QuinePattern): Future[Unit] = {
    quinePatterns.put(standingQueryId, qp)
    Future.unit
  }

  def getMetaData(key: String): Future[Option[Array[Byte]]] =
    Future.successful(Option(metaData.get(key)))

  def getAllMetaData(): Future[Map[String, Array[Byte]]] =
    Future.successful(metaData.asScala.toMap)

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = {
    newValue match {
      case None => metaData.remove(key)
      case Some(bytes) => metaData.put(key, bytes)
    }
    Future.unit
  }

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    Future.successful(
      this.domainGraphNodes.putAll(domainGraphNodes.asJava)
    )

  def removeDomainGraphNodes(domainGraphNodes: Set[DomainGraphNodeId]): Future[Unit] = Future.successful(
    for { domainGraphNodesId <- domainGraphNodes } this.domainGraphNodes.remove(domainGraphNodesId)
  )

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    Future.successful(domainGraphNodes.asScala.toMap)

  def shutdown(): Future[Unit] = Future.unit

  def delete(): Future[Unit] = Future.successful(
    allTables.foreach(_.clear())
  )
}

object InMemoryPersistor {

  /** Create a new empty in-memory persistor */
  def empty() = new InMemoryPersistor()(LogConfig.strictest)

  def namespacePersistor: PrimePersistor = new StatelessPrimePersistor(
    PersistenceConfig(),
    None,
    (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns)(LogConfig.strictest)
  )(null, LogConfig.strictest) // Materializer is never used if bloomFilterSize is set to none
  def persistorMaker: ActorSystem => PrimePersistor =
    _ => namespacePersistor
}
