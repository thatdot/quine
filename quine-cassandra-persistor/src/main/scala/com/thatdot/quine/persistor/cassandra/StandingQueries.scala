package com.thatdot.quine.persistor.cassandra

import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import org.apache.pekko.stream.Materializer

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import com.thatdot.quine.graph.{NamespaceId, StandingQuery, StandingQueryId}
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.persistor.codecs.StandingQueryCodec
import com.thatdot.quine.util.T2

trait StandingQueriesColumnNames {
  import CassandraCodecs._
  val standingQueryCodec: TypeCodec[StandingQuery] = fromBinaryFormat(StandingQueryCodec.format)
  final protected val queryIdColumn: CassandraColumn[StandingQueryId] = CassandraColumn("query_id")
  final protected val queriesColumn: CassandraColumn[StandingQuery] = CassandraColumn("queries")(standingQueryCodec)
}

class StandingQueriesDefinition(namespace: NamespaceId)
    extends TableDefinition[StandingQueries]("standing_queries", namespace)
    with StandingQueriesColumnNames {
  protected val partitionKey: CassandraColumn[StandingQueryId] = queryIdColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[StandingQuery]] = List(queriesColumn)

  protected val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(ddlTimeout)

  private val selectAllStatement: SimpleStatement = select
    .column(queriesColumn.name)
    .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(queryIdColumn.is.eq)
      .build()
      .setIdempotent(true)

  def create(
    session: CqlSession,
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings
  )(implicit
    mat: Materializer,
    futureInstance: Applicative[Future]
  ): Future[StandingQueries] = {
    import shapeless.syntax.std.tuple._
    logger.debug("Preparing statements for {}", tableName)

    (
      T2(insertStatement, deleteStatement).map(prepare(session, writeSettings)).toTuple :+
      prepare(session, readSettings)(selectAllStatement)
    ).mapN(new StandingQueries(session, firstRowStatement, dropTableStatement, _, _, _))
  }
}

class StandingQueries(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with StandingQueriesColumnNames {

  import syntax._

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    executeFuture(insertStatement.bindColumns(queryIdColumn.set(standingQuery.id), queriesColumn.set(standingQuery)))

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    executeFuture(deleteStatement.bindColumns(queryIdColumn.set(standingQuery.id)))

  def getStandingQueries: Future[List[StandingQuery]] =
    selectColumn(selectAllStatement.bind(), queriesColumn)
}
