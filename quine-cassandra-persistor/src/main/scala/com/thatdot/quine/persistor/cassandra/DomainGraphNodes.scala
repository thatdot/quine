package com.thatdot.quine.persistor.cassandra

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

import org.apache.pekko.stream.Materializer

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.model.DomainGraphNode
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.T2
trait DomainGraphNodeColumnNames {
  import CassandraCodecs._
  final protected val domainGraphNodeIdColumn: CassandraColumn[DomainGraphNodeId] = CassandraColumn[Long]("dgn_id")
  final protected val dataColumn: CassandraColumn[DomainGraphNode] = CassandraColumn[DomainGraphNode]("data")
}

case class DomainGraphNodesCreateConfig(
  session: CqlSession,
  verifyTable: CqlIdentifier => Future[Unit],
  chunker: Chunker,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  shouldCreateTables: Boolean,
)

object DomainGraphNodesDefinition
    extends TableDefinition[DomainGraphNodes, DomainGraphNodesCreateConfig]("domain_graph_nodes", None)
    with DomainGraphNodeColumnNames {
  protected val partitionKey: CassandraColumn[DomainGraphNodeId] = domainGraphNodeIdColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[DomainGraphNode]] = List(dataColumn)

  protected val createTableStatement: SimpleStatement =
    makeCreateTableStatement.build
      .setTimeout(ddlTimeout)

  private val selectAllStatement: SimpleStatement = select
    .columns(domainGraphNodeIdColumn.name, dataColumn.name)
    .build

  private val deleteStatement: SimpleStatement =
    delete
      .where(domainGraphNodeIdColumn.is.eq)
      .build

  def create(
    config: DomainGraphNodesCreateConfig,
  )(implicit mat: Materializer, futureInstance: Applicative[Future], logConfig: LogConfig): Future[DomainGraphNodes] = {
    import shapeless.syntax.std.tuple._
    logger.debug(safe"Preparing statements for ${Safe(tableName.toString)}")

    val createdSchema = futureInstance.whenA(
      config.shouldCreateTables,
    )(
      config.session
        .executeAsync(createTableStatement)
        .asScala
        .flatMap(_ => config.verifyTable(tableName))(ExecutionContext.parasitic),
    )

    createdSchema.flatMap(_ =>
      (
        T2(insertStatement, deleteStatement).map(prepare(config.session, config.writeSettings)).toTuple :+
        prepare(config.session, config.readSettings)(selectAllStatement)
      ).mapN(
        new DomainGraphNodes(
          config.session,
          config.chunker,
          config.writeSettings,
          firstRowStatement,
          dropTableStatement,
          _,
          _,
          _,
        ),
      ),
    )(ExecutionContext.parasitic)
  }
}

class DomainGraphNodes(
  session: CqlSession,
  chunker: Chunker,
  writeSettings: CassandraStatementSettings,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
)(implicit mat: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with DomainGraphNodeColumnNames {

  import syntax._

  def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    chunker(domainGraphNodes.toList) { dgns =>
      executeFuture(
        writeSettings(
          BatchStatement.newInstance(
            BatchType.UNLOGGED,
            dgns.map { case (domainGraphNodeId, domainGraphNode) =>
              insertStatement.bindColumns(
                domainGraphNodeIdColumn.set(domainGraphNodeId),
                dataColumn.set(domainGraphNode),
              )
            }: _*,
          ),
        ),
      )
    }

  def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    chunker(domainGraphNodeIds.toList) { dgnIds =>
      executeFuture(
        writeSettings(
          BatchStatement.newInstance(
            BatchType.UNLOGGED,
            dgnIds.map(id => deleteStatement.bindColumns(domainGraphNodeIdColumn.set(id))): _*,
          ),
        ),
      )
    }

  def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    selectColumns(selectAllStatement.bind(), domainGraphNodeIdColumn, dataColumn)
}
