package com.thatdot.quine.persistor.cassandra

import scala.concurrent.Future

import org.apache.pekko.stream.Materializer

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.cypher.quinepattern.QueryPlan
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}
import com.thatdot.quine.persistor.cassandra.support.{
  CassandraCodecs,
  CassandraColumn,
  CassandraStatementSettings,
  CassandraTable,
  TableDefinition,
}
import com.thatdot.quine.persistor.codecs.QueryPlanCodec
import com.thatdot.quine.util.T2

trait QuinePatternsColumnNames {
  import CassandraCodecs._
  implicit def logConfig: LogConfig
  val quinePatternCodec: TypeCodec[QueryPlan] = fromBinaryFormat(QueryPlanCodec.format)
  final protected val queryIdColumn: CassandraColumn[StandingQueryId] = CassandraColumn("query_id")
  final protected val queriesColumn: CassandraColumn[QueryPlan] = CassandraColumn("queries")(quinePatternCodec)
}

class QuinePatternsDefinition(namespace: NamespaceId)(implicit val logConfig: LogConfig)
    extends TableDefinition[QuinePatterns]("quine_patterns", namespace)
    with QuinePatternsColumnNames {

  private val selectAllStatement: SimpleStatement = select
    .column(queriesColumn.name)
    .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(queryIdColumn.is.eq)
      .build()
      .setIdempotent(true)

  override def create(
    session: CqlSession,
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
  )(implicit
    materializer: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[QuinePatterns] = {
    import shapeless.syntax.std.tuple._
    logger.debug(log"Preparing statements for ${Safe(tableName.toString)}")

    (
      T2(insertStatement, deleteStatement).map(prepare(session, writeSettings)).toTuple :+
      prepare(session, readSettings)(selectAllStatement)
    ).mapN(new QuinePatterns(session, firstRowStatement, dropTableStatement, _, _, _))
  }

  override protected def partitionKey: CassandraColumn[StandingQueryId] = queryIdColumn

  override protected def clusterKeys: List[CassandraColumn[_]] = List.empty

  override protected def dataColumns: List[CassandraColumn[QueryPlan]] = List(queriesColumn)

  override protected val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(ddlTimeout)
}

class QuinePatterns(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
)(implicit
  //mat: Materializer,
  val logConfig: LogConfig,
) extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with StandingQueriesColumnNames {}
