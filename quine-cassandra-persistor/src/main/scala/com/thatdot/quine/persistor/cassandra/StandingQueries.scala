package com.thatdot.quine.persistor.cassandra

import scala.concurrent.Future

import org.apache.pekko.stream.Materializer

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, StandingQueryInfo}
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.persistor.codecs.StandingQueryCodec
import com.thatdot.quine.util.T2
trait StandingQueriesColumnNames {
  import CassandraCodecs._
  implicit def logConfig: LogConfig
  val standingQueryCodec: TypeCodec[StandingQueryInfo] = fromBinaryFormat(StandingQueryCodec.format)
  final protected val queryIdColumn: CassandraColumn[StandingQueryId] = CassandraColumn("query_id")
  final protected val queriesColumn: CassandraColumn[StandingQueryInfo] = CassandraColumn("queries")(standingQueryCodec)
}

class StandingQueriesDefinition(namespace: NamespaceId)(implicit val logConfig: LogConfig)
    extends TableDefinition.DefaultType[StandingQueries]("standing_queries", namespace)
    with StandingQueriesColumnNames {
  protected val partitionKey: CassandraColumn[StandingQueryId] = queryIdColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[StandingQueryInfo]] = List(queriesColumn)

  protected val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(ddlTimeout)

  private val selectAllStatement: SimpleStatement = select
    .column(queriesColumn.name)
    .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(queryIdColumn.is.eq)
      .build()
      .setIdempotent(true)

  def create(config: TableDefinition.DefaultCreateConfig)(implicit
    mat: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[StandingQueries] = {
    import shapeless.syntax.std.tuple._
    logger.debug(log"Preparing statements for ${Safe(tableName.toString)}")

    (
      T2(insertStatement, deleteStatement).map(prepare(config.session, config.writeSettings)).toTuple :+
      prepare(config.session, config.readSettings)(selectAllStatement)
    ).mapN(new StandingQueries(config.session, firstRowStatement, dropTableStatement, _, _, _))
  }
}

class StandingQueries(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
)(implicit mat: Materializer, val logConfig: LogConfig)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with StandingQueriesColumnNames {

  import syntax._

  def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] =
    executeFuture(insertStatement.bindColumns(queryIdColumn.set(standingQuery.id), queriesColumn.set(standingQuery)))

  def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] =
    executeFuture(deleteStatement.bindColumns(queryIdColumn.set(standingQuery.id)))

  def getStandingQueries: Future[List[StandingQueryInfo]] =
    selectColumn(selectAllStatement.bind(), queriesColumn)
}
