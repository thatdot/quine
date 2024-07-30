package com.thatdot.quine.persistor

import scala.compat.CompatBuildFrom.implicitlyBF
import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList

import com.thatdot.quine.graph.{
  BaseGraph,
  DomainIndexEvent,
  EventTime,
  MemberIdx,
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
import com.thatdot.quine.util.Log.implicits._

object PersistenceAgent {

  /** persistence version implemented by the running persistor */
  val CurrentVersion: Version = Version(13, 2, 0)

  /** key used to store [[Version]] in persistence metadata */
  val VersionMetadataKey = "serialization_version"
}

/** Interface for a Quine storage layer that only exposes a namespace's data */
trait NamespacedPersistenceAgent extends StrictSafeLogging {

  /** Each persistor is instantiated with exactly one namespace which it is allowed to access, and prohibited from
    * accessing any other namespace. The allowed namespace is determined and passed in by the system instantiating
    * the persistor. There may be multiple instances of the same PersistenceAgent subtype if each one has a distinct
    * `namespace` value.
    */
  val namespace: NamespaceId

  /** Returns `true` if this persistor definitely contains no Quine-core data
    * May return `true` even when the persistor contains application (eg Quine) metadata
    * May return `true` even when the persistor contains a version number
    * May return `false` even when the persistor contains no Quine-core data, though this should be avoided
    * when possible.
    *
    * This is used to determine when an existing persistor's data may be safely used by any Quine version.
    */
  def emptyOfQuineData(): Future[Boolean]

  /** Returns `true` if and only if this persistor contains any MultipleValuesStandingQueryStates
    */
  def containsMultipleValuesStates(): Future[Boolean]

  /** Provides the [[BaseGraph]] instance to the [[PersistenceAgent]] when the [[BaseGraph]] is ready for use.
    * Used to trigger initialization behaviors that depend on [[BaseGraph]].
    * Default implementation is a no op.
    */
  def declareReady(graph: BaseGraph): Unit = ()

  def persistNodeChangeEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit]

  def deleteNodeChangeEvents(qid: QuineId): Future[Unit]

  /** Persist [[DomainIndexEvent]] values. */
  def persistDomainIndexEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit]

  def deleteDomainIndexEvents(qid: QuineId): Future[Unit]

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
      ExecutionContext.parasitic
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
  ): Future[Iterable[NodeEvent.WithTime[NodeEvent]]] = {

    def mergeEvents(
      i1: Iterable[NodeEvent.WithTime[NodeChangeEvent]],
      i2: Iterable[NodeEvent.WithTime[DomainIndexEvent]]
    ): Iterable[NodeEvent.WithTime[NodeEvent]] = (i1 ++ i2).toVector.sortBy(e => e.atTime.millis)

    val nceEvents = getNodeChangeEventsWithTime(id, startingAt, endingAt)

    if (!includeDomainIndexEvents)
      nceEvents
    else
      nceEvents.zipWith(
        getDomainIndexEventsWithTime(id, startingAt, endingAt)
      )(mergeEvents)(ExecutionContext.parasitic)
  }

  def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]]

  def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]]

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

  def deleteSnapshots(qid: QuineId): Future[Unit]

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

  def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit]

  /** Remove a standing query from the persistence layer
    * Implementers MUST remove the record of the standing query itself (such that it
    * no longer appears in the results of [[getStandingQueries]]), and SHOULD remove
    * additional state associated with the standing query on a best-effort basis (e.g.,
    * MVSQ states for that query).
    */
  def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit]

  def getStandingQueries: Future[List[StandingQueryInfo]]

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
    * NB the (StandingQueryId, QuineId, MultipleValuesStandingQueryPartId) tuple is necessary to
    * allow persistors to efficiently implement an appropriate keying strategy for MVSQ states.
    * However, the StandingQueryId and MultipleValuesStandingQueryPartId are currently duplicated
    * in the serialized `state` parameter. Our current [[codecs.MultipleValuesStandingQueryStateCodec]]
    * directly deserializes a [[MultipleValuesStandingQueryState]] rather than a POJO (or more accurately,
    * POSO) and therefore requires the StandingQueryId and MultipleValuesStandingQueryPartId to be
    * available within the serialized state. We could reduce the disk footprint by altering the serialization
    * codec to instead [de]serialize an intermediate representation of the SQ states which could be zipped
    * together with information from their keys at read time. This would save 32 bytes (or 2 UUIDs) per state.
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

  def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit]

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

  def delete(): Future[Unit]

  /** Configuration that determines how the client of PersistenceAgent should use it.
    */
  def persistenceConfig: PersistenceConfig
}

object MultipartSnapshotPersistenceAgent {
  case class MultipartSnapshot(time: EventTime, parts: Seq[MultipartSnapshotPart])
  case class MultipartSnapshotPart(partBytes: Array[Byte], multipartIndex: Int, multipartCount: Int)
}

/** Mixin for [[NamespacedPersistenceAgent]] that stores snapshot blobs as smaller multi-part blobs.
  * Because this makes snapshot writes non-atomic, it is possible only part of a snapshot will be
  * successfully written. Therefore, when a snapshot is read, the snapshot's integrity is checked.
  */
trait MultipartSnapshotPersistenceAgent {
  this: NamespacedPersistenceAgent =>

  import MultipartSnapshotPersistenceAgent._

  protected val multipartSnapshotExecutionContext: ExecutionContext
  protected val snapshotPartMaxSizeBytes: Int

  def persistSnapshot(id: QuineId, atTime: EventTime, state: Array[Byte]): Future[Unit] = {
    val parts = state.sliding(snapshotPartMaxSizeBytes, snapshotPartMaxSizeBytes).toSeq
    val partCount = parts.length
    if (partCount > 1000)
      logger.warn(safe"Writing multipart snapshot for node: ${Safe(id)} with part count: ${Safe(partCount)}")
    Future
      .sequence {
        for {
          (partBytes, partIndex) <- parts.zipWithIndex
          multipartSnapshotPart = MultipartSnapshotPart(partBytes, partIndex, partCount)
        } yield persistSnapshotPart(id, atTime, multipartSnapshotPart)
      }(implicitlyBF, multipartSnapshotExecutionContext)
      .map(_ => ())(ExecutionContext.parasitic)
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
          logger.warn(
            safe"Failed reading multipart snapshot for id: ${Safe(id)} upToTime: ${Safe(upToTime)}; retrying with time: ${Safe(time)}"
          )
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
        logger.warn(safe"Snapshot part has unexpected index: ${Safe(multipartIndex)} (expected: ${Safe(partIndex)})")
        result = false
      }
      if (multipartCount != partsLength) {
        logger.warn(safe"Snapshot part has unexpected count: ${Safe(multipartCount)} (expected: ${Safe(partsLength)})")
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

/** A namespaced persistence agent that also exposes global data (metadata, domain graph nodes)
  * Intended as a legacy shim for persistor impls where those app data and namespaced graph data are
  * still stored in he same place.
  */
trait PersistenceAgent extends NamespacedPersistenceAgent {

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

}
