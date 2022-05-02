package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.{
  EventTime,
  MemberIdx,
  NodeChangeEvent,
  StandingQuery,
  StandingQueryId,
  StandingQueryPartId
}
import com.thatdot.quine.model.QuineId

import PersistenceAgent.CurrentVersion

object PersistenceAgent {

  /** persistence version implemented by the running persistor */
  val CurrentVersion: Version = Version(11, 0, 1)

  /** key used to store [[Versions]] in persistence metadata */
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
    * Implementations should at least call [[PersistenceAgent.quineMetadataKeys]] to determine which metadata
    * are considered Quine-core data.
    *
    * This is used to determine when an existing persistor's data may be safely used by any Quine version.
    *
    * @param ec This operation queries several different kinds of data (journals, snapshots, etc). `ec` acts
    *           as a suggestion for which execution context to use while combining these data sources. Implementations
    *           may ignore this parameter.
    */
  def emptyOfQuineData()(implicit ec: ExecutionContext): Future[Boolean] = Future.successful(false)

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
  ): Future[Version] = {
    implicit val ec = ExecutionContexts.parasitic
    getMetaData(versionMetaDataKey).flatMap {
      case None =>
        logger.info(s"No version was set in the persistence backend for $context, initializing to $currentVersion")
        setMetaData(versionMetaDataKey, Some(currentVersion.toBytes)).map(_ => currentVersion)

      case Some(persistedVBytes) =>
        Version.fromBytes(persistedVBytes) match {
          case None =>
            val msg = s"Persistence backend cannot parse version for $context at $versionMetaDataKey"
            Future.failed(new IllegalStateException(msg))
          case Some(compatibleV) if currentVersion.canReadFrom(compatibleV) =>
            if (currentVersion <= compatibleV) {
              logger.info(
                s"Persistence backend for $context is at $compatibleV, this is usable as-is by $currentVersion"
              )
              Future.successful(compatibleV)
            } else {
              logger.info(
                s"Persistence backend for $context was at $compatibleV, upgrading to compatible $currentVersion"
              )
              setMetaData(versionMetaDataKey, Some(currentVersion.toBytes)).map(_ => currentVersion)
            }
          case Some(incompatibleV) =>
            isDataEmpty().flatMap {
              case true =>
                logger.warn(
                  s"Persistor reported that the last run used an incompatible $incompatibleV for $context, but no data was saved, so setting version to $currentVersion and continuing"
                )
                setMetaData(versionMetaDataKey, Some(currentVersion.toBytes)).map(_ => currentVersion)
              case false =>
                Future.failed(new IncompatibleVersion(context, incompatibleV, currentVersion))
            }
        }
    }
  }

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
      () => emptyOfQuineData()(ExecutionContexts.parasitic)
    )

  /** Persist an individual event affecting a node's state
    *
    * @param id    affected node
    * @param atTime when the event occurs
    * @param event state change
    * @return something that completes 'after' the write finishes
    */
  def persistEvent(id: QuineId, atTime: EventTime, event: NodeChangeEvent): Future[Unit]

  /** Fetch a time-ordered list of events affecting a node's state
    *
    * TODO: for debug purposes, consider adding a version of this function that returns events
    *       along with the time at which they occur
    *
    * @param id         affected node
    * @param startingAt only get events that occured 'at' or 'after' this moment
    * @param endingAt   only get events that occured 'at' or 'before' this moment
    * @return node events, ordered by ascending timestamp
    */
  def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Vector[NodeChangeEvent]]

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
  ): Future[Option[(EventTime, Array[Byte])]]

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit]

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit]

  def getStandingQueries: Future[List[StandingQuery]]

  /** Fetch the intermediate standing query states associated with a node
    *
    * @param id node
    * @return standing query states, keyed by the top-level standing query and sub-query
    */
  def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]]

  /** Set the intermediate standing query state associated with a node
    *
    * @param standingQuery   top-level standing query
    * @param id              node
    * @param standingQueryId sub-query ID
    * @param state           what to store ([[None]] corresponds to clearing out the state)
    */
  def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
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
    * @param key      name of the metadata
    * @param newValue what to store ([[None]] corresponds to clearing out the value)
    */
  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit]

  /** Update (or remove) a local metadata key.
    * For a local persistor, this is the same as setMetaData.
    *
    * @param key           name of the metadata
    * @param localMemberId Identifier for this member's position in the cluster.
    * @param newValue      what to store ([[None]] corresponds to clearing out the value)
    */
  def setLocalMetaData(key: String, localMemberId: MemberIdx, newValue: Option[Array[Byte]]): Future[Unit] =
    setMetaData(s"$localMemberId-$key", newValue)

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

/** Mix-in for persistors that don't save events (implements event related functions as no-ops) */
trait NoJournalPersistenceAgent extends PersistenceAgent {

  override def persistEvent(id: QuineId, atTime: EventTime, event: NodeChangeEvent): Future[Unit] = Future.unit

  override def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Vector[NodeChangeEvent]] = Future.successful(Vector.empty)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = Source.empty
}

final case class PersistorTerminatedException(msg: String) extends RuntimeException(msg)
