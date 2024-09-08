package com.thatdot.quine.persistor.cassandra

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

import org.apache.pekko.stream.Materializer

import cats.Monad
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.apply._
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.{PersistenceConfig, PrimePersistor}

abstract class PrimeCassandraPersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  session: CqlSession,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  shouldCreateTables: Boolean,
  verifyTable: CqlSession => CqlIdentifier => Future[Unit],
)(implicit materializer: Materializer)
    extends PrimePersistor(persistenceConfig, bloomFilterSize) {

  type PersistenceAgentType = CassandraPersistor

  protected val chunker: Chunker

  // This is so we can have syntax like .mapN and .tupled, without making the parasitic ExecutionContext implicit.
  implicit protected val futureInstance: Monad[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)
  def shutdown(): Future[Unit] = session.closeAsync().asScala.void

  private lazy val (metaData, domainGraphNodes) = Await.result(
    (
      MetaDataDefinition
        .create(session, verifyTable(session), readSettings, writeSettings, shouldCreateTables),
      DomainGraphNodesDefinition
        .create(session, verifyTable(session), chunker, readSettings, writeSettings, shouldCreateTables),
    ).tupled,
    36.seconds,
  )

  protected def internalGetMetaData(key: String): Future[Option[Array[Byte]]] = metaData.getMetaData(key)

  protected def internalGetAllMetaData(): Future[Map[String, Array[Byte]]] = metaData.getAllMetaData()

  protected def internalSetMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    metaData.setMetaData(key, newValue)

  protected def internalPersistDomainGraphNodes(
    domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode],
  ): Future[Unit] =
    this.domainGraphNodes.persistDomainGraphNodes(domainGraphNodes)

  protected def internalRemoveDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    domainGraphNodes.removeDomainGraphNodes(domainGraphNodeIds)

  protected def internalGetDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    domainGraphNodes.getDomainGraphNodes()
}
