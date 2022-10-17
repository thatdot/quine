package com.thatdot.quine.persistor

import scala.compat.CompatBuildFrom.implicitlyBF
import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.{
  BaseGraph,
  DomainIndexEvent,
  EventTime,
  MemberIdx,
  MultipleValuesStandingQueryPartId,
  NodeEvent,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}
import com.thatdot.quine.persistor.PersistenceAgent.CurrentVersion

object PersistenceAgent {

  /** persistence version implemented by the running persistor */
  val CurrentVersion: Version = Version(13, 0, 0)

  /** key used to store [[Version]] in persistence metadata */
  val VersionMetadataKey = "serialization_version"
}

/** Interface for a Quine storage layer */
abstract class PersistenceAgent extends StrictLogging {

  /** Returns `true` if this persistor definitely contains no Quine-core data
    * May return `true` even when the persistor contains application (eg Quine) metadata
    * May return `true` even when the persistor contains a version number
    * May return `false` even when the persistor contains no Quine-core data, though this should be avoided
    * when possible.
    *
    * This is used to determine when an existing persistor's data may be safely used by any Quine version.
    */
  def emptyOfQuineData(): Future[Boolean] = Future.successful(false)

  /** Get the version that will be used for a certain subset of data
    *
    * This will return a failed future if the version is incompatible.
    *
    * @see IncompatibleVersion
    * @param context what is the versioned data? This is only used in logs and error messages.
    * @param versionMetaDataKey key in the metadata table
    * @param currentVersion persistence version tracked by current code
    * @param isDataEmpty check whether there is any data relevant to the version
    */
  def syncVersion(
    context: String,
    versionMetaDataKey: String,
    currentVersion: Version,
    isDataEmpty: () => Future[Boolean]
  ): Future[Version] =
    getMetaData(versionMetaDataKey).flatMap {
      case None =>
        logger.info(s"No version was set in the persistence backend for: $context, initializing to: $currentVersion")
        setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))
          .map(_ => currentVersion)(ExecutionContexts.parasitic)

      case Some(persistedVBytes) =>
        Version.fromBytes(persistedVBytes) match {
          case None =>
            val msg = s"Persistence backend cannot parse version for: $context at: $versionMetaDataKey"
            Future.failed(new IllegalStateException(msg))
          case Some(compatibleV) if currentVersion.canReadFrom(compatibleV) =>
            if (currentVersion <= compatibleV) {
              logger.info(
                s"Persistence backend for: $context is at: $compatibleV, this is usable as-is by: $currentVersion"
              )
              Future.successful(compatibleV)
            } else {
              logger.info(
                s"Persistence backend for: $context was at: $compatibleV, upgrading to compatible: $currentVersion"
              )
              setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))
                .map(_ => currentVersion)(ExecutionContexts.parasitic)
            }
          case Some(incompatibleV) =>
            isDataEmpty().flatMap {
              case true =>
                logger.warn(
                  s"Persistor reported that the last run used an incompatible: $incompatibleV for: $context, but no data was saved, so setting version to: $currentVersion and continuing"
                )
                setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))
                  .map(_ => currentVersion)(ExecutionContexts.parasitic)
              case false =>
                Future.failed(new IncompatibleVersion(context, incompatibleV, currentVersion))
            }(ExecutionContexts.parasitic)
        }
    }(ExecutionContexts.parasitic)

  /** Gets the version of data last stored by this persistor, or PersistenceAgent.CurrentVersion
    *
    * Invariant: This will implicitly set the version to CurrentVersion if the previous version
    * is forwards-compatible with CurrentVersion
    *
    * This Future may be a Failure if the persistor abstracts over mutually-incompatible data
    * (eg a sharded persistor with underlying persistors operating over different format versions)
    *
    * The default implementation defers to the metadata storage API
    */
  def syncVersion(): Future[Version] =
    syncVersion(
      "core quine data",
      PersistenceAgent.VersionMetadataKey,
      CurrentVersion,
      () => emptyOfQuineData()
    )

  /** Persist a series of [[NodeEvent]]s affecting a node's state.
    *
    * @param id    affected node
    * @param events event records to write
    * @return something that completes 'after' the write finishes
    */
  final def persistEvents(id: QuineId, events: Seq[NodeEvent.WithTime]): Future[Unit] = {
    val (domainIndexEvents, nodeChangeEvents) = events.partition(e => e.event.isInstanceOf[DomainIndexEvent])
    implicit val ctx: ExecutionContext = ExecutionContexts.parasitic
    for {
      _ <- persistDomainIndexEvents(id, domainIndexEvents)
      _ <- persistNodeChangeEvents(id, nodeChangeEvents)
    } yield ()
  }

  /** Persist [[NodeChangeEvent]] values. */
  def persistNodeChangeEvents(id: QuineId, events: Seq[NodeEvent.WithTime]): Future[Unit]

  /** Persist [[DomainIndexEvent]] values. */
  def persistDomainIndexEvents(id: QuineId, events: Seq[NodeEvent.WithTime]): Future[Unit]

  /** Fetch a time-ordered list of events without timestamps affecting a node's state.
    *
    * @param id         affected node
    * @param startingAt only get events that occurred 'at' or 'after' this moment
    * @param endingAt   only get events that occurred 'at' or 'before' this moment
    * @param includeDomainIndexEvents whether to include [[com.thatdot.quine.graph.DomainIndexEvent]] type events in the result
    * @return node events without timestamps, ordered by ascending timestamp
    */
  final def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
    includeDomainIndexEvents: Boolean
  ): Future[Iterable[NodeEvent]] =
    getJournalWithTime(id, startingAt, endingAt, includeDomainIndexEvents).map(_.map(_.event))(
      ExecutionContexts.parasitic
    )

  /** Fetch a time-ordered list of events with timestamps affecting a node's state,
    * discarding timestamps.
    *
    * @param id         affected node
    * @param startingAt only get events that occurred 'at' or 'after' this moment
    * @param endingAt   only get events that occurred 'at' or 'before' this moment
    * @param includeDomainIndexEvents whether to include [[com.thatdot.quine.graph.DomainIndexEvent]] type events in the result
    * @return node events with timestamps, ordered by ascending timestamp
    */
  final def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
    includeDomainIndexEvents: Boolean
  ): Future[Iterable[NodeEvent.WithTime]] = {

    def mergeEvents(
      i1: Iterable[NodeEvent.WithTime],
      i2: Iterable[NodeEvent.WithTime]
    ): Iterable[NodeEvent.WithTime] = (i1 ++ i2).toVector.sortBy(e => e.atTime.millis)

    val nceEvents = getNodeChangeEventsWithTime(id, startingAt, endingAt)

    if (!includeDomainIndexEvents) {
      nceEvents
    } else {
      implicit val ctx: ExecutionContext = ExecutionContexts.parasitic
      for {
        h: Iterable[NodeEvent.WithTime] <- nceEvents
        i: Iterable[NodeEvent.WithTime] <- getDomainIndexEventsWithTime(id, startingAt, endingAt)
      } yield mergeEvents(h, i)
    }
  }

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime]]

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime]]

  /** Get a source of every node in the graph which has been written to the
    * journal store.
    *
    * @note output source is weakly-consistent
    * @note output source does not contain duplicates
    * @return the set of nodes this persistor knows of
    */
  def enumerateJournalNodeIds(): Source[QuineId, NotUsed]

  /** Get a source of every node in the graph which has been written to the
    * snapshot store.
    *
    * @note output source is weakly-consistent
    * @note output source does not contain duplicates
    * @return the set of nodes this persistor knows of
    */
  def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed]

  /** Persist a full snapshot of a node
    *
    * @param id     affected node
    * @param atTime time at which the snapshot was taken
    * @param state  snapshot to save
    * @return something that completes 'after' the write finishes
    */
  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit]

  /** Fetch the latest snapshot of a node
    *
    * @param id       affected node
    * @param upToTime snapshot must have been taken 'at' or 'before' this time
    * @return latest snapshot, along with the timestamp at which it was taken
    */
  def getLatestSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[Array[Byte]]]

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit]

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit]

  def getStandingQueries: Future[List[StandingQuery]]

  /** Fetch the intermediate standing query states associated with a node
    *
    * @param id node
    * @return standing query states, keyed by the top-level standing query and sub-query
    */
  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]]

  /** Set the intermediate standing query state associated with a node
    *
    * @param standingQuery   top-level standing query
    * @param id              node
    * @param standingQueryId sub-query ID
    * @param state           what to store ([[None]] corresponds to clearing out the state)
    */
  def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit]

  /** Fetch a metadata value with a known name
    *
    * @param key name of the metadata
    * @return metadata (or [[None]] if no corresponding data was found)
    */
  def getMetaData(key: String): Future[Option[Array[Byte]]]

  /** Get a key scoped to this process. For a local persistor, this is the same
    * as getMetaData.
    *
    * @param key           name of the local metadata
    * @param localMemberId Identifier for this member's position in the cluster.
    * @return              metadata (or [[None]] if no corresponding data was found)
    */
  def getLocalMetaData(key: String, localMemberId: MemberIdx): Future[Option[Array[Byte]]] =
    getMetaData(s"$localMemberId-$key")

  /** Fetch all defined metadata values
    *
    * @return metadata key-value pairs
    */
  def getAllMetaData(): Future[Map[String, Array[Byte]]]

  /** Update (or remove) a given metadata key
    *
    * @param key      name of the metadata - must be nonempty
    * @param newValue what to store ([[None]] corresponds to clearing out the value)
    */
  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit]

  /** Update (or remove) a local metadata key.
    * For a local persistor, this is the same as setMetaData.
    *
    * @param key           name of the metadata - must be nonempty
    * @param localMemberId Identifier for this member's position in the cluster.
    * @param newValue      what to store ([[None]] corresponds to clearing out the value)
    */
  def setLocalMetaData(key: String, localMemberId: MemberIdx, newValue: Option[Array[Byte]]): Future[Unit] =
    setMetaData(s"$localMemberId-$key", newValue)

  /** Provides the [[BaseGraph]] instance to the [[PersistenceAgent]] when the [[BaseGraph]] is ready for use.
    * Used to trigger initialization behaviors that depend on [[BaseGraph]].
    * Default implementation is a no op.
    */
  def ready(graph: BaseGraph): Unit = ()

  /** Saves [[DomainGraphNode]]s to persistent storage.
    *
    * Note that [[DomainGraphNodeId]] is fully computed from [[DomainGraphNode]], therefore a
    * Domain Graph Node cannot be updated. Calling this function with [[DomainGraphNode]] that
    * is already stored is a no-op.
    *
    * @param domainGraphNodes [[DomainGraphNode]]s to be saved
    * @return Future completes successfully when the external operation completes successfully
    */
  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit]

  /** Removes [[DomainGraphNode]]s from persistent storage.
    *
    * @param domainGraphNodeIds IDs of DGNs to remove
    * @return Future completes successfully when the external operation completes successfully
    */
  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit]

  /** @return All [[DomainGraphNode]]s stored in persistent storage.
    */
  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]]

  /** Delete all [DomainIndexEvent]]s by their held DgnId. Note that depending on the storage implementation
    * this may be an extremely slow operation.
    * @param dgnId
    */
  def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit]

  /** Close this persistence agent
    *
    * TODO: perhaps we should make this wait until all pending writes finish?
    *
    * @return something that completes 'after' the write finishes
    */
  def shutdown(): Future[Unit]

  /** Configuration that determines how the client of PersistenceAgent should use it.
    */
  def persistenceConfig: PersistenceConfig
}

object MultipartSnapshotPersistenceAgent {
  case class MultipartSnapshot(time: EventTime, parts: Seq[MultipartSnapshotPart])
  case class MultipartSnapshotPart(partBytes: Array[Byte], multipartIndex: Int, multipartCount: Int)
}

/** Mixin for [[PersistenceAgent]] that stores snapshot blobs as smaller multi-part blobs.
  * Because this makes snapshot writes non-atomic, it is possible only part of a snapshot will be
  * successfully written. Therefore, when a snapshot is read, the snapshot's integrity is checked.
  */
trait MultipartSnapshotPersistenceAgent {
  this: PersistenceAgent =>

  import MultipartSnapshotPersistenceAgent._

  val multipartSnapshotExecutionContext: ExecutionContext
  val snapshotPartMaxSizeBytes: Int

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    val parts = state.sliding(snapshotPartMaxSizeBytes, snapshotPartMaxSizeBytes).toSeq
    val partCount = parts.length
    if (partCount > 1000) logger.warn(s"Writing multipart snapshot for node: $id with part count: $partCount")
    Future
      .sequence {
        for {
          (partBytes, partIndex) <- parts.zipWithIndex
          multipartSnapshotPart = MultipartSnapshotPart(partBytes, partIndex, partCount)
        } yield persistSnapshotPart(id, atTime, multipartSnapshotPart)
      }(implicitlyBF, multipartSnapshotExecutionContext)
      .map(_ => ())(ExecutionContexts.parasitic)
  }

  def getLatestSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[Array[Byte]]] =
    getLatestMultipartSnapshot(id, upToTime).flatMap {
      case Some(MultipartSnapshot(time, parts)) =>
        if (validateSnapshotParts(parts))
          Future.successful(Some(parts.flatMap(_.partBytes).toArray))
        else {
          logger.warn(s"Failed reading multipart snapshot for id: $id upToTime: $upToTime; retrying with time: $time")
          getLatestSnapshot(id, time)
        }
      case None =>
        Future.successful(None)
    }(multipartSnapshotExecutionContext)

  private def validateSnapshotParts(parts: Seq[MultipartSnapshotPart]): Boolean = {
    val partsLength = parts.length
    var result = true
    for { (MultipartSnapshotPart(_, multipartIndex, multipartCount), partIndex) <- parts.zipWithIndex } {
      if (multipartIndex != partIndex) {
        logger.warn(s"Snapshot part has unexpected index: $multipartIndex (expected: $partIndex)")
        result = false
      }
      if (multipartCount != partsLength) {
        logger.warn(s"Snapshot part has unexpected count: $multipartCount (expected: $partsLength)")
        result = false
      }
    }
    result
  }

  def persistSnapshotPart(id: QuineId, atTime: EventTime, part: MultipartSnapshotPart): Future[Unit]

  def getLatestMultipartSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[MultipartSnapshot]]
}
