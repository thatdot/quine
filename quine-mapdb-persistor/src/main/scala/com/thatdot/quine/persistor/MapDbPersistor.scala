package com.thatdot.quine.persistor

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.ConcurrentNavigableMap
import java.util.{Comparator, Map => JavaMap, UUID}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{Cancellable, Scheduler}
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList
import com.codahale.metrics.{Counter, Histogram, MetricRegistry, NoopMetricRegistry}
import com.typesafe.scalalogging.StrictLogging
import org.mapdb._
import org.mapdb.serializer.{SerializerArrayTuple, SerializerCompressionWrapper, SerializerLong}

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
import com.thatdot.quine.persistor.codecs.{
  DomainGraphNodeCodec,
  DomainIndexEventCodec,
  NodeChangeEventCodec,
  StandingQueryCodec
}
import com.thatdot.quine.util.ComputeAndBlockingExecutionContext
import com.thatdot.quine.util.PekkoStreams.distinctConsecutive

/** Embedded persistence implementation based on MapDB
  *
  * MapDB has a couple of issues that end up mattering to Quine. These include:
  *
  *   1. An upstream bug (`GetVoid: record does not exist`) which we've worked around with retries
  *
  *   2. Skyrocketing times for `commit` as DB files reach 2GB which we've worked around by adding
  *      support for sharded persistors (see [[ShardedPersistor]])
  *
  *   3. All DB files are all fully memory-mapped, so we end up memory-mapping an ever growing
  *      amount of data! This either crashes or becomes really inefficient if the data files exceed
  *      the amount of RAM a user has.
  *
  *   4. The write ahead log doesn't work on windows
  *
  *   5. Closing and deleting the DB file doesn't work on windows (something related to `mmap`)
  *
  * The main strength that MapDB brings is that it does not rely on native code, so it can run on
  * almost any system that the JVM supports (although you may need to disable the WAL and it may
  * be a lot slower).
  *
  * @param filePath location for the MapDB database file
  * @param writeAheadLog whether or to enable the WAL (doesn't work on Windows)
  * @param transactionCommitInterval used only when the WAL is enabled
  * @param persistenceConfig configuration for persistence
  * @param metricRegistry registry used for metrics for this persistor
  */
final class MapDbPersistor(
  filePath: MapDbPersistor.DbPath,
  val namespace: NamespaceId,
  writeAheadLog: Boolean = false,
  transactionCommitInterval: Option[FiniteDuration] = None,
  val persistenceConfig: PersistenceConfig = PersistenceConfig(),
  metricRegistry: MetricRegistry = new NoopMetricRegistry(),
  ExecutionContext: ComputeAndBlockingExecutionContext,
  scheduler: Scheduler
) extends PersistenceAgent {

  val nodeEventSize: Histogram =
    metricRegistry.histogram(MetricRegistry.name("map-db-persistor", "journal-event-size"))
  val nodeEventTotalSize: Counter =
    metricRegistry.counter(MetricRegistry.name("map-db-persistor", "journal-event-total-size"))

  import ExecutionContext.{blockingDispatcherEC, nodeDispatcherEC}

  // TODO: Consider: should the concurrencyScale parameter equal the thread pool size in `pekko.quine.persistor-blocking-dispatcher.thread-pool-executor.fixed-pool-size ?  Or a multiple of...?
  // TODO: don't hardcode magical values - config them
  protected val db: DB = {
    val dbBuilder1 = filePath
      .makeDB()
      .concurrencyScale(32)
      .allocateIncrement(1000000L)
      .allocateStartSize(10000000L)
    val dbBuilder2 = if (writeAheadLog) dbBuilder1.transactionEnable() else dbBuilder1
    dbBuilder2.make()
  }

  // Periodic commits - this matters especially in the `writeAheadLog = true` case
  val transactionCommitCancellable: Cancellable = transactionCommitInterval match {
    case None => Cancellable.alreadyCancelled
    case Some(dur) =>
      scheduler.scheduleWithFixedDelay(dur, dur)(() => db.commit())(
        blockingDispatcherEC
      )
  }

  private type QuineIdTimestampTuple = Array[AnyRef]

  // TODO: investigate using `valuesOutsideNodesEnable` for the `treeMap`

  private val nodeChangeEvents: ConcurrentNavigableMap[QuineIdTimestampTuple, Array[Byte]] = db
    .treeMap("nodeChangeEvents")
    .keySerializer(
      new SerializerArrayTuple(
        Serializer.BYTE_ARRAY, // QuineId
        MapDbPersistor.SerializerUnsignedLong // Node event timestamp
      )
    )
    .valueSerializer(Serializer.BYTE_ARRAY) // NodeEvent
    .createOrOpen()

  private val domainIndexEvents: ConcurrentNavigableMap[QuineIdTimestampTuple, Array[Byte]] = db
    .treeMap("domainIndexEvents")
    .keySerializer(
      new SerializerArrayTuple(
        Serializer.BYTE_ARRAY, // QuineId
        MapDbPersistor.SerializerUnsignedLong // Node event timestamp
      )
    )
    .valueSerializer(Serializer.BYTE_ARRAY) // NodeEvent
    .createOrOpen()

  private val snapshots: ConcurrentNavigableMap[QuineIdTimestampTuple, Array[Byte]] = db
    .treeMap(s"snapshots")
    .keySerializer(
      new SerializerArrayTuple(
        Serializer.BYTE_ARRAY, // QuineId
        MapDbPersistor.SerializerUnsignedLong // Node event timestamp
      )
    )
    .valueSerializer(new SerializerCompressionWrapper(Serializer.BYTE_ARRAY)) // SerializedNodeSnapshot
    .createOrOpen()

  private val standingQueries: util.AbstractSet[Array[Byte]] = db
    .hashSet(s"standingQueries")
    .serializer(
      Serializer.BYTE_ARRAY // Standing query
    )
    .createOrOpen()

  private val multipleValuesStandingQueryStates: ConcurrentNavigableMap[Array[AnyRef], Array[Byte]] = db
    .treeMap("multipleValuesStandingQueryStates")
    .keySerializer(
      new SerializerArrayTuple(
        Serializer.UUID, // Top-level standing query ID
        Serializer.BYTE_ARRAY, // QuineId
        Serializer.UUID // Standing sub-query ID
      )
    )
    .valueSerializer(new SerializerCompressionWrapper(Serializer.BYTE_ARRAY)) // standing query state
    .createOrOpen()

  private val metaData: HTreeMap[String, Array[Byte]] = db
    .hashMap("metaData", Serializer.STRING, Serializer.BYTE_ARRAY)
    .createOrOpen()

  private val domainGraphNodes: ConcurrentNavigableMap[java.lang.Long, Array[Byte]] = db
    .treeMap("domainGraphNodes")
    .keySerializer(
      Serializer.LONG // Domain graph node ID
    )
    .valueSerializer(new SerializerCompressionWrapper(Serializer.BYTE_ARRAY)) // Domain graph node
    .createOrOpen()

  override def emptyOfQuineData(): Future[Boolean] =
    // on the io dispatcher: check that each column family is empty
    Future(
      nodeChangeEvents.isEmpty && domainIndexEvents.isEmpty && snapshots.isEmpty && standingQueries.isEmpty && multipleValuesStandingQueryStates.isEmpty && domainGraphNodes.isEmpty
    )(blockingDispatcherEC)

  private def quineIdTimeRangeEntries(
    map: ConcurrentNavigableMap[QuineIdTimestampTuple, Array[Byte]],
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Iterator[java.util.Map.Entry[QuineIdTimestampTuple, Array[Byte]]] = {
    // missing values in array key = -infinity, `null` = +infinity
    val startingKey: Array[AnyRef] = startingAt match {
      case EventTime.MinValue => Array[AnyRef](id.array)
      case _ => Array[AnyRef](id.array, Long.box(startingAt.eventTime))
    }
    val endingKey: Array[AnyRef] = endingAt match {
      case EventTime.MaxValue => Array[AnyRef](id.array, null)
      case _ => Array[AnyRef](id.array, Long.box(endingAt.eventTime))
    }
    val includeStartingKey = true
    val includeEndingKey = true

    map
      .subMap(startingKey, includeStartingKey, endingKey, includeEndingKey)
      .entrySet()
      .iterator()
      .asScala
  }

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = Future {
    quineIdTimeRangeEntries(nodeChangeEvents, id, startingAt, endingAt).map { entry =>
      val eventTime = EventTime.fromRaw(Long.unbox(entry.getKey()(1)))
      val event = NodeChangeEventCodec.format.read(entry.getValue).get
      NodeEvent.WithTime(event, eventTime)
    }.toSeq
  }(blockingDispatcherEC)
    .recoverWith { case e =>
      logger.error("getNodeChangeEvents failed", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] = Future {
    quineIdTimeRangeEntries(domainIndexEvents, id, startingAt, endingAt).map { entry =>
      val eventTime = EventTime.fromRaw(Long.unbox(entry.getKey()(1)))
      val event = DomainIndexEventCodec.format.read(entry.getValue).get
      NodeEvent.WithTime(event, eventTime)
    }.toSeq
  }(blockingDispatcherEC)
    .recoverWith { case e =>
      logger.error("getDomainIndexEvents failed", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    Future {
      val eventsMap = for { NodeEvent.WithTime(event, atTime) <- events.toList } yield {
        val serializedEvent = NodeChangeEventCodec.format.write(event)
        nodeEventSize.update(serializedEvent.length)
        nodeEventTotalSize.inc(serializedEvent.length.toLong)
        Array[AnyRef](id.array, Long.box(atTime.eventTime)) -> serializedEvent
      }
      val _ = nodeChangeEvents.putAll((eventsMap.toMap).asJava)
    }(blockingDispatcherEC).recoverWith { case e =>
      logger.error("persist NodeChangeEvent failed.", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  private def deleteQuineIdEntries(
    map: ConcurrentNavigableMap[QuineIdTimestampTuple, Array[Byte]],
    qid: QuineId,
    methodName: String
  ): Future[Unit] = Future {
    quineIdTimeRangeEntries(map, qid, EventTime.MinValue, EventTime.MaxValue)
      .foreach(entry => map.remove(entry.getKey))
  }(blockingDispatcherEC).recoverWith { case e =>
    logger.error(methodName + " failed.", e)
    Future.failed(e)
  }(nodeDispatcherEC)

  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] =
    deleteQuineIdEntries(nodeChangeEvents, qid, "deleteNodeChangeEvents")

  def persistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    Future {
      val eventsMap = for { NodeEvent.WithTime(event, atTime) <- events.toList } yield {
        val serializedEvent = DomainIndexEventCodec.format.write(event.asInstanceOf[DomainIndexEvent])
        nodeEventSize.update(serializedEvent.length)
        nodeEventTotalSize.inc(serializedEvent.length.toLong)
        Array[AnyRef](id.array, Long.box(atTime.eventTime)) -> serializedEvent
      }
      val _ = domainIndexEvents.putAll((eventsMap toMap).asJava)
    }(blockingDispatcherEC).recoverWith { case e =>
      logger.error("persist DomainIndexEvent failed.", e); Future.failed(e)
    }(nodeDispatcherEC)

  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] =
    deleteQuineIdEntries(domainIndexEvents, qid, "deleteDomainIndexEvents")

  def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    Source
      .fromIterator(() => nodeChangeEvents.keySet().iterator().asScala)
      .map(x => QuineId(x.head.asInstanceOf[Array[Byte]]))
      .via(distinctConsecutive)
      .named("mapdb-all-node-scan-via-journals")

  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    Source
      .fromIterator(() => snapshots.keySet().iterator().asScala)
      .map(x => QuineId(x.head.asInstanceOf[Array[Byte]]))
      .via(distinctConsecutive)
      .named("mapdb-all-node-scan-via-snapshots")

  def persistSnapshot(id: QuineId, atTime: EventTime, snapshotBytes: Array[Byte]): Future[Unit] =
    Future {
      val _ = snapshots.put(Array[AnyRef](id.array, Long.box(atTime.eventTime)), snapshotBytes)
    }(blockingDispatcherEC).recoverWith { case e =>
      logger.error("persistSnapshot failed.", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  override def deleteSnapshots(qid: QuineId): Future[Unit] =
    deleteQuineIdEntries(snapshots, qid, "deleteSnapshots")

  /* MapDB has a [bug](https://github.com/jankotek/mapdb/issues/966) that sporadically causes
   * errors in `getLatestSnapshot`. This is an attempt to reduce the likelihood of this error
   * (which we hypothesize might occur due to some race condition under heavy concurrent writes)
   * by retrying.
   */
  private[this] def tryGetLatestSnapshot(
    startingKey: Array[AnyRef],
    endingKey: Array[AnyRef],
    remainingAttempts: Int
  ): Future[Option[JavaMap.Entry[Array[AnyRef], Array[Byte]]]] =
    Future(Option(snapshots.subMap(startingKey, true, endingKey, true).lastEntry()))(blockingDispatcherEC)
      .recoverWith {
        case e: org.mapdb.DBException.GetVoid if remainingAttempts > 0 =>
          // This is a known MapDB issue, see <https://github.com/jankotek/mapdb/issues/966>
          logger.info(s"tryGetLatestSnapshot failed. Remaining attempts: $remainingAttempts Message: ${e.getMessage}")
          tryGetLatestSnapshot(startingKey, endingKey, remainingAttempts - 1)
      }(nodeDispatcherEC)

  def getLatestSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[Array[Byte]]] = {
    // missing values in key = -infinity, `null` = +infinity
    val startingKey: Array[AnyRef] = Array[AnyRef](id.array)
    val endingKey: Array[AnyRef] = upToTime match {
      case EventTime.MaxValue => Array[AnyRef](id.array, null)
      case _ => Array[AnyRef](id.array, Long.box(upToTime.eventTime))
    }

    tryGetLatestSnapshot(startingKey, endingKey, 5)
      .map { (maybeEntry: Option[JavaMap.Entry[Array[AnyRef], Array[Byte]]]) =>
        maybeEntry.map(_.getValue)
      }(blockingDispatcherEC)
      .recoverWith { case e =>
        logger.error(s"getLatestSnapshot failed on $id. ${e.getMessage}")
        Future.failed(e)
      }(nodeDispatcherEC)
  }

  def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = Future {
    val bytes = StandingQueryCodec.format.write(standingQuery)
    val _ = standingQueries.add(bytes)
  }(blockingDispatcherEC)

  def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = Future {
    val bytes = StandingQueryCodec.format.write(standingQuery)
    val _ = standingQueries.remove(bytes)

    val topLevelId = standingQuery.id.uuid
    multipleValuesStandingQueryStates
      .subMap(Array[AnyRef](topLevelId), Array[AnyRef](topLevelId, null))
      .clear()
  }(blockingDispatcherEC)

  def getStandingQueries: Future[List[StandingQueryInfo]] =
    Future(standingQueries.iterator().asScala)(blockingDispatcherEC)
      .map(_.map(b => StandingQueryCodec.format.read(b).get).toList)(nodeDispatcherEC)
      .recoverWith { case e =>
        logger.error("getStandingQueries failed.", e); Future.failed(e)
      }(nodeDispatcherEC)

  override def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] = Future {
    val toReturn = Map.newBuilder[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]

    // Loop through each of the top-level standing query IDs
    var remainingStates = multipleValuesStandingQueryStates
    while (!remainingStates.isEmpty) {
      val topLevelId = remainingStates.firstEntry.getKey.apply(0).asInstanceOf[UUID]
      remainingStates
        .subMap(
          Array[AnyRef](topLevelId, id.array),
          Array[AnyRef](topLevelId, id.array, null)
        )
        .forEach { (key: Array[AnyRef], value: Array[Byte]) =>
          val subQueryId = key(2).asInstanceOf[UUID]
          toReturn += (StandingQueryId(topLevelId) -> MultipleValuesStandingQueryPartId(subQueryId)) -> value
        }

      // Advance to the next top-level standing query
      remainingStates = remainingStates.tailMap(Array[AnyRef](topLevelId, null))
    }

    toReturn.result()
  }(blockingDispatcherEC)

  override def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = Future {
    state match {
      case None =>
        val _ =
          multipleValuesStandingQueryStates.remove(Array[AnyRef](standingQuery.uuid, id.array, standingQueryId.uuid))
      case Some(newValue) =>
        val _ = multipleValuesStandingQueryStates.put(
          Array[AnyRef](standingQuery.uuid, id.array, standingQueryId.uuid),
          newValue
        )
    }
  }(blockingDispatcherEC)

  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] = Future {
    val idBytes = id.array
    multipleValuesStandingQueryStates.keySet().asScala.foreach { key =>
      val keyIdBytes = key(1).asInstanceOf[Array[Byte]]
      if (java.util.Arrays.equals(idBytes, keyIdBytes)) {
        multipleValuesStandingQueryStates.remove(key)
      }
    }
  }(blockingDispatcherEC)
    .recoverWith { case e =>
      logger.error("deleteMultipleValuesStandingQueryStates failed", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  def containsMultipleValuesStates(): Future[Boolean] = Future {
    !multipleValuesStandingQueryStates.isEmpty
  }(blockingDispatcherEC)

  def getMetaData(key: String): Future[Option[Array[Byte]]] = Future(Option(metaData.get(key)))(blockingDispatcherEC)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] = Future(metaData.asScala.toMap)(blockingDispatcherEC)

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = Future {
    newValue match {
      case None =>
        val _ = metaData.remove(key)
      case Some(value) =>
        val _ = metaData.put(key, value)
    }
  }(blockingDispatcherEC)

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] = Future {
    this.domainGraphNodes.putAll(domainGraphNodes.map { case (dgbId, dgb) =>
      Long.box(dgbId) -> DomainGraphNodeCodec.format.write(dgb)
    }.asJava)
  }(blockingDispatcherEC)

  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] = Future {
    domainGraphNodeIds foreach { dgnId =>
      this.domainGraphNodes.remove(dgnId)
    }
  }(blockingDispatcherEC)

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    Future(domainGraphNodes.asScala.toMap.map { case (dgnId, dgnBytes) =>
      Long.unbox(dgnId) -> DomainGraphNodeCodec.format.read(dgnBytes).get
    })(blockingDispatcherEC).recoverWith { case e =>
      logger.error("getDomainGraphNodes failed.", e)
      Future.failed(e)
    }(nodeDispatcherEC)

  /** Shutdown the DB cleanly, so that it can be opened back up later */
  def shutdown(): Future[Unit] = Future {
    if (writeAheadLog) db.commit()
    transactionCommitCancellable.cancel()
    db.close()
  }(blockingDispatcherEC)

  /** Delete everything that has been persisted (clear all the in-memory stuff
    * as well as durable storage)
    *
    * This doesn't work on Windows
    */
  def delete(): Future[Unit] = Future {
    val files: List[String] = db.getStore.getAllFiles.asScala.toList
    transactionCommitCancellable.cancel()
    db.close()
    for (file <- files)
      try Files.deleteIfExists(Paths.get(file))
      catch {
        case NonFatal(err) =>
          logger.error("Failed to delete DB file {} ({})", file, err)
      }
  }(blockingDispatcherEC)

  /** Delete all [[DomainIndexEvent]]s by their held DgnId. Note that depending on the storage implementation
    * this may be an extremely slow operation.
    */
  override def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    Future {
      domainIndexEvents.entrySet().removeIf(e => DomainIndexEventCodec.format.read(e.getValue).get.dgnId == dgnId)
      ()
    }(blockingDispatcherEC)
}

object MapDbPersistor {

  /** Location of MapDB data */
  sealed abstract class DbPath {
    def makeDB(): DBMaker.Maker = this match {
      case TemporaryDb => DBMaker.tempFileDB().fileMmapEnable()
      case PersistedDb(p) => DBMaker.fileDB(p).fileMmapEnable()
      case InMemoryDb => DBMaker.memoryDB()
    }

    private[persistor] def deleteStore(): Boolean = this match {
      case PersistedDb(path) => path.delete()
      case _ => true
    }

    def map(f: File => File): DbPath = this match {
      case PersistedDb(p) => PersistedDb(f(p))
      case o => o
    }
  }
  case object TemporaryDb extends DbPath
  case object InMemoryDb extends DbPath
  final case class PersistedDb(path: File) extends DbPath
  object PersistedDb extends StrictLogging {
    def makeDirIfNotExists(createParentDir: Boolean, path: File): PersistedDb = {
      val parentDir = path.getAbsoluteFile.getParentFile
      if (createParentDir)
        if (parentDir.mkdirs())
          logger.warn("Parent directory: {} of requested persistence location did not exist; created.", parentDir)
        else if (!parentDir.isDirectory) sys.error(s"$parentDir is not a directory")
      PersistedDb(path)
    }
  }

  /** MapDB serializer for `Long` which treats inputs as unsigned numbers - the
    * overridden methods are the ones that define order-related functionality
    */
  object SerializerUnsignedLong extends SerializerLong {

    override def compare(first: java.lang.Long, second: java.lang.Long): Int =
      java.lang.Long.compareUnsigned(first.longValue, second.longValue)

    override def valueArraySearch(keys: Any, keyBoxed: java.lang.Long): Int = {
      val longKeys = keys.asInstanceOf[Array[Long]]
      var low: Int = 0
      var high: Int = longKeys.length - 1
      val key: Long = keyBoxed.longValue

      while (low <= high) {
        val mid = (low + high) >>> 1 // avoids the "overflow bug"
        val comp = java.lang.Long.compareUnsigned(longKeys(mid), key)

        if (comp < 0) low = mid + 1
        else if (comp > 0) high = mid - 1
        else return mid // Found position
      }

      // No key found
      -(low + 1)
    }

    @throws[IOException]
    override def valueArrayBinarySearch(
      keyBoxed: java.lang.Long,
      input: DataInput2,
      len: Int,
      comparator: Comparator[_]
    ): Int =
      if (comparator != this) {
        super.valueArrayBinarySearch(keyBoxed, input, len, comparator)
      } else {
        var pos: Int = 0
        val key: Long = keyBoxed.longValue

        while (pos < len) {
          val from = input.readLong()
          if (java.lang.Long.compareUnsigned(key, from) <= 0) {
            input.skipBytes((len - pos - 1) * 8)
            return if (key == from) pos else -(pos + 1) // Found position
          }
          pos += 1
        }

        // No key found
        -(pos + 1)
      }
  }
}
