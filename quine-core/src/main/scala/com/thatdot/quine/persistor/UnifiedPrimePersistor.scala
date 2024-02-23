package com.thatdot.quine.persistor

import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.Future

import org.apache.pekko.stream.Materializer

import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId

/** A GlobalPersistor where the global data is stored in the default instance of the NamespacedPersistenceAgents
  *
  * @param persistenceConfig
  */
abstract class UnifiedPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long]
)(implicit materializer: Materializer)
    extends PrimePersistor(persistenceConfig, bloomFilterSize) {

  type PersistenceAgentType = PersistenceAgent

  def shutdown(): Future[Unit] =
    Future.traverse(persistors.values)(_.shutdown())(implicitly, parasitic).map(_ => ())(parasitic)

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
