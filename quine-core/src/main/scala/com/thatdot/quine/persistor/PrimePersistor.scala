package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import org.apache.pekko.stream.Materializer

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.{BaseGraph, MemberIdx, NamespaceId, StandingQuery, defaultNamespaceId}
import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.PersistenceAgent.CurrentVersion

abstract class PrimePersistor(val persistenceConfig: PersistenceConfig, bloomFilterSize: Option[Long])(implicit
  materializer: Materializer
) extends ExceptionWrapper {

  type PersistenceAgentType <: NamespacedPersistenceAgent

  protected var persistors: Map[NamespaceId, NamespacedPersistenceAgent] = Map.empty

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgentType

  private def bloomFilter(persistor: NamespacedPersistenceAgent): NamespacedPersistenceAgent =
    BloomFilteredPersistor.maybeBloomFilter(bloomFilterSize, persistor, persistenceConfig)

  private def wrapExceptions(persistor: NamespacedPersistenceAgent): NamespacedPersistenceAgent =
    new ExceptionWrappingPersistenceAgent(persistor)

  def shutdown(): Future[Unit]

  def apply(namespace: NamespaceId): Option[NamespacedPersistenceAgent] = persistors.get(namespace)

  // Use an AtomicReference to make deletes of this threadsafe?
  private var default: PersistenceAgentType = _

  /** Can't be called in this abstract class constructor because `agentCreator` isn't defined yet.
    */
  protected def initializeDefault(): Unit = {
    Await.ready(prepareNamespace(defaultNamespaceId), 35.seconds)
    default = agentCreator(persistenceConfig, defaultNamespaceId)
    persistors += (defaultNamespaceId -> bloomFilter(wrapExceptions(default)))
  }

  // This is called from `getDefault` to make sure the default persistor has been created
  // lazy val so we only instantiate the default persistor once.
  // Could use at AtomicBoolean for this, instead
  // Some different strategy would be nice - i.e. if the default persistor wasn't special-cased and assumed to exist
  // but was initialized on startup the same as any other namespace.
  lazy val initializeOnce: Unit = initializeDefault()
  def getDefault: PersistenceAgentType = {
    initializeOnce
    default
  }

  /** Create the on-disk representation of the namespace - e.g. create its Cassandra tables or whatever
    * @param namespace
    * @return
    */
  def prepareNamespace(namespace: NamespaceId): Future[Unit] = Future.unit

  def createNamespace(namespace: NamespaceId): Boolean = {
    val didChange = !persistors.contains(namespace)
    if (didChange) persistors += (namespace -> bloomFilter(wrapExceptions(agentCreator(persistenceConfig, namespace))))
    didChange
  }

  def deleteNamespace(namespace: NamespaceId): Boolean =
    if (namespace == defaultNamespaceId) {
      default.delete()
      initializeDefault()
      true
    } else {
      val didChange = persistors.contains(namespace)
      if (didChange) {
        val toDelete = persistors(namespace)
        toDelete.delete()
        persistors -= namespace
      }
      if (namespace == defaultNamespaceId) createNamespace(defaultNamespaceId) // Default namespace should always exist
      didChange
    }

  /** Get all standing queries across all namespaces
    */
  def getAllStandingQueries(): Future[Map[NamespaceId, List[StandingQuery]]] =
    Future
      .traverse(persistors: Iterable[(NamespaceId, NamespacedPersistenceAgent)]) { case (ns, pa) =>
        pa.getStandingQueries.map(ns -> _)(parasitic)
      }(implicitly, parasitic)
      .map(_.toMap)(parasitic)

  /** Returns `true` if all of the namespaces definitely contain no Quine-core data
    * May return `true` even when a persistor contains application (eg Quine) metadata
    * May return `true` even when a persistor contains a version number
    * May return `false` even when a persistor contains no Quine-core data, though this should be avoided
    * when possible.
    *
    * This is used to determine when an existing persistor's data may be safely used by any Quine version.
    */
  def emptyOfQuineData(): Future[Boolean] =
    Future.traverse(persistors.values)(_.emptyOfQuineData())(implicitly, parasitic).map(_.forall(identity))(parasitic)

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
  ): Future[Unit] =
    getMetaData(versionMetaDataKey).flatMap {
      case None =>
        logger.info(s"No version was set in the persistence backend for: $context, initializing to: $currentVersion")
        setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))

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
              Future.unit
            } else {
              logger.info(
                s"Persistence backend for: $context was at: $compatibleV, upgrading to compatible: $currentVersion"
              )
              setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))
            }
          case Some(incompatibleV) =>
            isDataEmpty().flatMap {
              case true =>
                logger.warn(
                  s"Persistor reported that the last run used an incompatible: $incompatibleV for: $context, but no data was saved, so setting version to: $currentVersion and continuing"
                )
                setMetaData(versionMetaDataKey, Some(currentVersion.toBytes))
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
  def syncVersion(): Future[Unit] =
    syncVersion(
      "core quine data",
      PersistenceAgent.VersionMetadataKey,
      CurrentVersion,
      () => emptyOfQuineData()
    )

  /** Fetch a metadata value with a known name
    *
    * @param key name of the metadata
    * @return metadata (or [[None]] if no corresponding data was found)
    */
  def getMetaData(key: String): Future[Option[Array[Byte]]] = wrapException(
    GetMetaData(key),
    internalGetMetaData(key)
  )

  protected def internalGetMetaData(key: String): Future[Option[Array[Byte]]]

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
  def getAllMetaData(): Future[Map[String, Array[Byte]]] = wrapException(
    GetAllMetaData,
    internalGetAllMetaData()
  )

  protected def internalGetAllMetaData(): Future[Map[String, Array[Byte]]]

  /** Update (or remove) a given metadata key
    *
    * @param key      name of the metadata - must be nonempty
    * @param newValue what to store ([[None]] corresponds to clearing out the value)
    */
  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] = wrapException(
    SetMetaData(key, newValue.map(_.length)),
    internalSetMetaData(key, newValue)
  )

  protected def internalSetMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit]

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
  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] = wrapException(
    PersistDomainGraphNodes(domainGraphNodes),
    internalPersistDomainGraphNodes(domainGraphNodes)
  )

  protected def internalPersistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit]

  /** Removes [[DomainGraphNode]]s from persistent storage.
    *
    * @param domainGraphNodeIds IDs of DGNs to remove
    * @return Future completes successfully when the external operation completes successfully
    */
  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] = wrapException(
    RemoveDomainGraphNodes(domainGraphNodeIds),
    internalRemoveDomainGraphNodes(domainGraphNodeIds)
  )

  protected def internalRemoveDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit]

  /** @return All [[DomainGraphNode]]s stored in persistent storage.
    */
  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] = wrapException(
    GetDomainGraphNodes,
    internalGetDomainGraphNodes()
  )

  protected def internalGetDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]]

  /** Provides the [[BaseGraph]] instance to the [[NamespacedPersistenceAgent]] when the [[BaseGraph]] is ready for use.
    * Used to trigger initialization behaviors that depend on [[BaseGraph]].
    * Default implementation is a no op.
    */
  def declareReady(graph: BaseGraph): Unit = ()

}

/** A GlobalPersistor where the global data is stored in the default instance of the NamespacedPersistenceAgents
  * @param persistenceConfig
  */
abstract class UnifiedPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long]
)(implicit materializer: Materializer)
    extends PrimePersistor(persistenceConfig, bloomFilterSize) {

  type PersistenceAgentType = PersistenceAgent

  def shutdown(): Future[Unit] = Future.unit

  protected def internalGetMetaData(key: String): Future[Option[Array[Byte]]] = getDefault.getMetaData(key)

  protected def internalGetAllMetaData(): Future[Map[String, Array[Byte]]] = getDefault.getAllMetaData()

  protected def internalSetMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    getDefault.setMetaData(key, newValue)

  protected def internalPersistDomainGraphNodes(
    domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]
  ): Future[Unit] =
    getDefault.persistDomainGraphNodes(domainGraphNodes)

  protected def internalRemoveDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    getDefault.removeDomainGraphNodes(domainGraphNodeIds)

  protected def internalGetDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    getDefault.getDomainGraphNodes()
}
class StatelessPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  create: (PersistenceConfig, NamespaceId) => PersistenceAgent
)(implicit materializer: Materializer)
    extends UnifiedPrimePersistor(persistenceConfig, bloomFilterSize) {

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgent =
    create(persistenceConfig, namespace)
}
