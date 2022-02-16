package com.thatdot.quine.persistor

import java.io.ByteArrayInputStream

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}

import com.google.common.hash.{BloomFilter, Funnel, Funnels, PrimitiveSink}

import com.thatdot.quine.graph.{
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
  final val storageKey = "bloom-filter"

  def maybeBloomFilter(
    maybeSize: Option[Long],
    persistor: PersistenceAgent,
    persistenceConfig: PersistenceConfig
  )(implicit
    materializer: Materializer
  ): PersistenceAgent =
    maybeSize.fold(persistor)(new BloomFilteredPersistor(persistor, _, persistenceConfig))
}

/** A persistor that wraps another persistor, and short-circuits read-calls to getJournal,
  * getLatestSnapshot, and getStandingQueryStates regarding QuineIds it knows not to exist with
  * empty results. It keeps tally of the set of known QuineIds
  *
  * @param wrappedPersistor The persistor implementation to wrap
  * @param bloomFilterSize The number of expected nodes
  * @param falsePositiveRate The false positive probability
  */
class BloomFilteredPersistor(
  wrappedPersistor: PersistenceAgent,
  bloomFilterSize: Long,
  val persistenceConfig: PersistenceConfig,
  falsePositiveRate: Double = 0.1
)(implicit materializer: Materializer)
    extends PersistenceAgent {

  private val bloomFilter: BloomFilter[QuineId] = {
    import materializer.executionContext
    Await.result(
      getMetaData(BloomFilteredPersistor.storageKey)
        .flatMap {
          case Some(data) =>
            logger.debug("Restoring bloom filter saved at '{}'", BloomFilteredPersistor.storageKey)
            /* Clear out the saved bloom filter once we've read it,
             * to avoid reading a stale bloom filter next time in the
             * event of a crash before clean shutdown.
             */
            setMetaData(BloomFilteredPersistor.storageKey, None).map { _ =>
              BloomFilter.readFrom(new ByteArrayInputStream(data), QuineIdFunnel)
            }
          case None =>
            logger.info("No saved bloom filter found - creating one from existing data.")
            def enumerateAllNodeIds() =
              if (persistenceConfig.journalEnabled) enumerateJournalNodeIds() else enumerateSnapshotNodeIds()
            val allLocalQuineids =
              enumerateAllNodeIds()
            allLocalQuineids.runWith(
              StreamConverters.javaCollectorParallelUnordered(4)(() =>
                BloomFilter.toBloomFilter[QuineId](QuineIdFunnel, bloomFilterSize, falsePositiveRate)
              )
            )
        },
      2.minutes
    )
  }

  override def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] = {
    // TODO if bloomFilter.approximateElementCount() == 0 and the bloom filter is the only violation, that's also fine
    val noBloomFilter = getMetaData(BloomFilteredPersistor.storageKey).map(_.isEmpty)
    val noOtherData = wrappedPersistor.emptyOfQuineData()
    noBloomFilter.zipWith(noOtherData)(_ && _)(ec)
  }

  override def persistEvent(id: QuineId, atTime: EventTime, event: NodeChangeEvent): Future[Unit] = {
    bloomFilter.put(id)
    wrappedPersistor.persistEvent(id, atTime, event)
  }

  override def getJournal(id: QuineId, startingAt: EventTime, endingAt: EventTime): Future[Vector[NodeChangeEvent]] =
    if (bloomFilter.mightContain(id))
      wrappedPersistor.getJournal(id, startingAt, endingAt)
    else
      Future.successful(Vector.empty)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = wrappedPersistor.enumerateJournalNodeIds()

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = wrappedPersistor.enumerateSnapshotNodeIds()

  override def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    bloomFilter.put(id)
    wrappedPersistor.persistSnapshot(id, atTime, state)
  }

  override def getLatestSnapshot(id: QuineId, upToTime: EventTime): Future[Option[(EventTime, Array[Byte])]] =
    if (bloomFilter.mightContain(id))
      wrappedPersistor.getLatestSnapshot(id, upToTime)
    else
      Future.successful(None)

  override def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrappedPersistor.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    wrappedPersistor.removeStandingQuery(standingQuery)

  override def getStandingQueries: Future[List[StandingQuery]] = wrappedPersistor.getStandingQueries

  override def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    if (bloomFilter.mightContain(id))
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

  override def shutdown(): Future[Unit] =
    /* TODO: blobs larger than 1MB are bad for Cassandra.
       Save these somewhere else.
    val baos = new ByteArrayOutputStream(bloomFilterSize.toInt * 2)
    bloomFilter.writeTo(baos)
    setLocalMetaData(storageKey, Some(baos.toByteArray))
      .flatMap(_ => wrappedPersistor.shutdown())(ExecutionContexts.parasitic)
     */
    wrappedPersistor.shutdown()

}
