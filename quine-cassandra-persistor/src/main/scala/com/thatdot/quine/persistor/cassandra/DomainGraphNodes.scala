package com.thatdot.quine.persistor.cassandra

import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.stream.Materializer

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}

import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.T2

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
    .build

  private val deleteStatement: SimpleStatement =
    delete
      .where(domainGraphNodeIdColumn.is.eq)
      .build

  def create(
    session: CqlSession,
    verifyTable: String => Future[Unit],
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
    shouldCreateTables: Boolean
  )(implicit
    mat: Materializer,
    futureInstance: Applicative[Future]
  ): Future[DomainGraphNodes] = {
    import shapeless.syntax.std.tuple._
    logger.debug("Preparing statements for {}", tableName)

    val createdSchema = futureInstance.whenA(
      shouldCreateTables
    )(
      session
        .executeAsync(createTableStatement)
        .toScala
        .flatMap(_ => verifyTable(tableName))(ExecutionContexts.parasitic)
    )

    createdSchema.flatMap(_ =>
      (
        T2(insertStatement, deleteStatement).map(prepare(session, writeSettings)).toTuple :+
        prepare(session, readSettings)(selectAllStatement)
      ).mapN(new DomainGraphNodes(session, chunker, writeSettings, _, _, _))
    )(ExecutionContexts.parasitic)
  }
}

class DomainGraphNodes(
  session: CqlSession,
  chunker: Chunker,
  writeSettings: CassandraStatementSettings,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with DomainGraphNodeColumnNames {

  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(StandingQueries.firstRowStatement)

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    chunker(domainGraphNodes.toList) { dgns =>
      executeFuture(
        writeSettings(
          BatchStatement.newInstance(
            BatchType.UNLOGGED,
            dgns.map { case (domainGraphNodeId, domainGraphNode) =>
              insertStatement.bindColumns(
                domainGraphNodeIdColumn.set(domainGraphNodeId),
                dataColumn.set(domainGraphNode)
              )
            }: _*
          )
        )
      )
    }

  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    chunker(domainGraphNodeIds.toList) { dgnIds =>
      executeFuture(
        writeSettings(
          BatchStatement.newInstance(
            BatchType.UNLOGGED,
            dgnIds.map(id => deleteStatement.bindColumns(domainGraphNodeIdColumn.set(id))): _*
          )
        )
      )
    }

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    selectColumns(selectAllStatement.bind(), domainGraphNodeIdColumn, dataColumn)
}
