package com.thatdot.quine.persistor.cassandra.vanilla

import scala.compat.ExecutionContexts
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.stream.Materializer

import cats.Monad
import cats.implicits._
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}

import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId

trait DomainGraphNodeColumnNames {
  import CassandraCodecs._
  final protected val domainGraphNodeIdColumn: CassandraColumn[DomainGraphNodeId] = CassandraColumn[Long]("dgn_id")
  final protected val dataColumn: CassandraColumn[DomainGraphNode] = CassandraColumn[DomainGraphNode]("data")
}

object DomainGraphNodes extends TableDefinition with DomainGraphNodeColumnNames {
  protected val tableName = "domain_graph_nodes"
  protected val partitionKey: CassandraColumn[DomainGraphNodeId] = domainGraphNodeIdColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[DomainGraphNode]] = List(dataColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement.build
      .setTimeout(createTableTimeout)

  private val selectAllStatement: SimpleStatement = select
    .columns(domainGraphNodeIdColumn.name, dataColumn.name)
    .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(domainGraphNodeIdColumn.is.eq)
      .build()
      .setIdempotent(true)

  def create(
    session: CqlSession,
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit
    mat: Materializer,
    futureMonad: Monad[Future]
  ): Future[DomainGraphNodes] = {
    logger.debug("Preparing statements for {}", tableName)

    def prepare(statement: SimpleStatement): Future[PreparedStatement] = {
      logger.trace("Preparing {}", statement.getQuery)
      session.prepareAsync(statement).toScala
    }

    val createdSchema =
      if (shouldCreateTables)
        session.executeAsync(createTableStatement).toScala
      else
        Future.unit

    createdSchema.flatMap(_ =>
      (
        prepare(insertStatement.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency)),
        prepare(selectAllStatement.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(deleteStatement.setConsistencyLevel(readConsistency))
      ).mapN(new DomainGraphNodes(session, _, _, _))
    )(ExecutionContexts.parasitic)
  }
}

class DomainGraphNodes(
  session: CqlSession,
  insertStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
  deleteStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with DomainGraphNodeColumnNames {

  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(StandingQueries.arbitraryRowStatement)

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    executeFuture(
      BatchStatement.newInstance(
        DefaultBatchType.LOGGED,
        domainGraphNodes.toSeq map { case (domainGraphNodeId, domainGraphNode) =>
          insertStatement.bindColumns(
            domainGraphNodeIdColumn.set(domainGraphNodeId),
            dataColumn.set(domainGraphNode)
          )
        }: _*
      )
    )

  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    executeFuture(
      BatchStatement.newInstance(
        DefaultBatchType.LOGGED,
        domainGraphNodeIds.toSeq map { domainGraphNodeId =>
          deleteStatement.bindColumns(
            domainGraphNodeIdColumn.set(domainGraphNodeId)
          )
        }: _*
      )
    )

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    selectColumns(selectAllStatement.bind(), domainGraphNodeIdColumn, dataColumn)
}
