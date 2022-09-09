package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source

import com.google.common.hash.{BloomFilter, Funnel, Funnels, PrimitiveSink}

import com.thatdot.quine.graph.{
  BaseGraph,
  EventTime,
  MemberIdx,
  NodeChangeEvent,
  StandingQuery,
  StandingQueryId,
  StandingQueryPartId
}
import com.thatdot.quine.model.QuineId

// This needs to be serializable for the bloom filter to be serializable
case object QuineIdFunnel extends Funnel[QuineId] {
  override def funnel(from: QuineId, into: PrimitiveSink): Unit = Funnels.byteArrayFunnel.funnel(from.array, into)
}

object BloomFilteredPersistor {
  def maybeBloomFilter(
    maybeSize: Option[Long],
    persistor: PersistenceAgent,
    persistenceConfig: PersistenceConfig
  )(implicit
    materializer: Materializer
  ): PersistenceAgent =
    maybeSize.fold(persistor)(new BloomFilteredPersistor(persistor, _, persistenceConfig))
}

/** [[PersistenceAgent]] wrapper that short-circuits read calls to [[getJournal]],
  * [[getJournalWithTime]], [[getLatestSnapshot]], and [[getStandingQueryStates]] regarding
  * QuineIds assigned to this position that the persistor knows not to exist with empty results.
  *
  * @param wrappedPersistor The persistor implementation to wrap
  * @param bloomFilterSize The number of expected nodes
  * @param falsePositiveRate The false positive probability
  */
private class BloomFilteredPersistor(
  wrappedPersistor: PersistenceAgent,
  bloomFilterSize: Long,
  val persistenceConfig: PersistenceConfig,
  falsePositiveRate: Double = 0.1
)(implicit materializer: Materializer)
    extends PersistenceAgent {

  private val bloomFilter: BloomFilter[QuineId] =
    BloomFilter.create[QuineId](QuineIdFunnel, bloomFilterSize, falsePositiveRate)

  logger.info(s"Initialized persistor bloom filter with size: $bloomFilterSize records")

  /** Indicates that the existing bloom filter state has been restored from the persistor
    * and the bloom filter is therefore ready for use in short circuiting queries of known
    * non-existent nodes.
    */
  private var bloomFilterIsReady: Boolean = false

  private def mightContain(qid: QuineId): Boolean =
    !bloomFilterIsReady || bloomFilter.mightContain(qid)

  override def emptyOfQuineData(): Future[Boolean] =
    // TODO if bloomFilter.approximateElementCount() == 0 and the bloom filter is the only violation, that's also fine
    wrappedPersistor.emptyOfQuineData()

  override def persistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]): Future[Unit] = {
    bloomFilter.put(id)
    wrappedPersistor.persistEvents(id, events)
  }

  override def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeChangeEvent]] =
    if (mightContain(id))
      wrappedPersistor.getJournal(id, startingAt, endingAt)
    else
      Future.successful(Iterable.empty)

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeChangeEvent.WithTime]] =
    if (mightContain(id))
      wrappedPersistor.getJournalWithTime(id, startingAt, endingAt)
    else
      Future.successful(Iterable.empty)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = wrappedPersistor.enumerateJournalNodeIds()

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = wrappedPersistor.enumerateSnapshotNodeIds()

  override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    bloomFilter.put(id)
    wrappedPersistor.persistSnapshot(id, atTime, state)
  }

  override def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] =
    if (mightContain(id))
      wrappedPersistor.getLatestSnapshot(id, upToTime)
    else
      Future.successful(None)

  override def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrappedPersistor.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrappedPersistor.removeStandingQuery(standingQuery)

  override def getStandingQueries: Future[List[StandingQuery]] = wrappedPersistor.getStandingQueries

  override def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    if (mightContain(id))
      wrappedPersistor.getStandingQueryStates(id)
    else
      Future.successful(Map.empty)

  override def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = {
    bloomFilter.put(id)
    wrappedPersistor.setStandingQueryState(standingQuery, id, standingQueryId, state)
  }

  override def getAllMetaData(): Future[Map[String, Array[Byte]]] = wrappedPersistor.getAllMetaData()

  override def getMetaData(key: String): Future[Option[Array[Byte]]] = wrappedPersistor.getMetaData(key)

  override def getLocalMetaData(key: String, localMemberId: MemberIdx): Future[Option[Array[Byte]]] =
    wrappedPersistor.getLocalMetaData(key, localMemberId)

  override def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    wrappedPersistor.setMetaData(key, newValue)

  override def setLocalMetaData(key: String, localMemberId: MemberIdx, newValue: Option[Array[Byte]]): Future[Unit] =
    wrappedPersistor.setLocalMetaData(key, localMemberId, newValue)

  /** Begins asynchronously loading all node ID into the bloom filter set.
    */
  override def ready(graph: BaseGraph): Unit = {
    super.ready(graph)
    val t0 = System.currentTimeMillis
    val source =
      if (persistenceConfig.journalEnabled) enumerateJournalNodeIds()
      else enumerateSnapshotNodeIds()
    val filteredSource = source.filter(graph.isLocalGraphNode)
    filteredSource
      .runForeach { q => // TODO consider using Sink.foreachAsync instead
        bloomFilter.put(q)
        ()
      }
      .onComplete {
        case Success(_) =>
          val d = System.currentTimeMillis - t0
          val c = bloomFilter.approximateElementCount()
          logger.info(s"Finished loading in duration: $d ms; node set size ~ $c QuineIDs)")
          bloomFilterIsReady = true
        case Failure(ex) =>
          logger.warn("Error loading; continuing to run in degraded state", ex)
      }(ExecutionContexts.parasitic)
    ()
  }

  override def shutdown(): Future[Unit] =
    wrappedPersistor.shutdown()
}
