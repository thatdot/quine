package com.thatdot.quine.persistor.cassandra

import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink

import cats.Applicative
import cats.syntax.apply._
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}

import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, NamespaceId, StandingQueryId}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.{T2, T4}

trait StandingQueryStatesColumnNames {
  import CassandraCodecs._
  final protected val standingQueryIdColumn: CassandraColumn[StandingQueryId] =
    CassandraColumn[StandingQueryId]("standing_query_id")
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val multipleValuesStandingQueryPartIdColumn: CassandraColumn[MultipleValuesStandingQueryPartId] =
    CassandraColumn[MultipleValuesStandingQueryPartId]("standing_query_part_id")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
}

class StandingQueryStatesDefinition(namespace: NamespaceId)
    extends TableDefinition[StandingQueryStates]("standing_query_states", namespace)
    with StandingQueryStatesColumnNames {
  //protected val indexName = "standing_query_states_idx"
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[_]] =
    List(standingQueryIdColumn, multipleValuesStandingQueryPartIdColumn)
  protected val dataColumns: List[CassandraColumn[Array[Byte]]] = List(dataColumn)

  protected val createTableStatement: SimpleStatement =
    makeCreateTableStatement.build.setTimeout(ddlTimeout)

  /*
  private val createIndexStatement: SimpleStatement =
    createIndex(indexName)
      .ifNotExists()
      .onTable(tableName)
      .andColumn(standingQueryIdColumn.name)
      .build()
   */

  private val getMultipleValuesStandingQueryStates =
    select
      .columns(standingQueryIdColumn.name, multipleValuesStandingQueryPartIdColumn.name, dataColumn.name)
      .where(quineIdColumn.is.eq)
      .build()

  private val removeStandingQueryState =
    delete
      .where(
        quineIdColumn.is.eq,
        standingQueryIdColumn.is.eq,
        multipleValuesStandingQueryPartIdColumn.is.eq
      )
      .build()
      .setIdempotent(true)

  private val getIdsForStandingQuery =
    select
      .columns(quineIdColumn.name)
      .where(standingQueryIdColumn.is.eq)
      .allowFiltering
      .build()

  private val removeStandingQuery =
    delete
      .where(
        quineIdColumn.is.eq,
        standingQueryIdColumn.is.eq
      )
      .build()
      .setIdempotent(true)

  def create(
    session: CqlSession,
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings
  )(implicit
    materializer: Materializer,
    futureInstance: Applicative[Future]
  ): Future[StandingQueryStates] = {
    import shapeless.syntax.std.tuple._
    logger.debug("Preparing statements for {}", tableName)

    (
      T4(insertStatement, removeStandingQueryState, removeStandingQuery, deleteAllByPartitionKeyStatement)
        .map(prepare(session, writeSettings))
        .toTuple ++
      T2(getMultipleValuesStandingQueryStates, getIdsForStandingQuery)
        .map(prepare(session, readSettings))
        .toTuple
    ).mapN(new StandingQueryStates(session, firstRowStatement, dropTableStatement, _, _, _, _, _, _))
  }

}
class StandingQueryStates(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  removeStandingQueryStateStatement: PreparedStatement,
  removeStandingQueryStatement: PreparedStatement,
  deleteStandingQueryStatesByQid: PreparedStatement,
  getMultipleValuesStandingQueryStatesStatement: PreparedStatement,
  getIdsForStandingQueryStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with StandingQueryStatesColumnNames {

  import syntax._

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    executeSelect[((StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]), Map[
      (StandingQueryId, MultipleValuesStandingQueryPartId),
      Array[Byte]
    ]](getMultipleValuesStandingQueryStatesStatement.bindColumns(quineIdColumn.set(id)))(row =>
      (standingQueryIdColumn.get(row) -> multipleValuesStandingQueryPartIdColumn.get(row)) -> dataColumn.get(row)
    )

  def setStandingQueryState(
    standingQuery: StandingQueryId,
    qid: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] =
    executeFuture(
      state match {
        case None =>
          removeStandingQueryStateStatement.bindColumns(
            quineIdColumn.set(qid),
            standingQueryIdColumn.set(standingQuery),
            multipleValuesStandingQueryPartIdColumn.set(standingQueryId)
          )

        case Some(bytes) =>
          insertStatement.bindColumns(
            quineIdColumn.set(qid),
            standingQueryIdColumn.set(standingQuery),
            multipleValuesStandingQueryPartIdColumn.set(standingQueryId),
            dataColumn.set(bytes)
          )
      }
    )

  def deleteStandingQueryStates(id: QuineId): Future[Unit] = executeFuture(
    deleteStandingQueryStatesByQid.bindColumns(quineIdColumn.set(id))
  )

  def removeStandingQuery(standingQuery: StandingQueryId): Future[Unit] =
    executeSource(getIdsForStandingQueryStatement.bindColumns(standingQueryIdColumn.set(standingQuery)))
      .named("cassandra-get-standing-query-ids")
      .runWith(
        Sink
          .foreachAsync[ReactiveRow](16) { row =>
            val deleteStatement = removeStandingQueryStatement.bindColumns(
              quineIdColumn.set(quineIdColumn.get(row)),
              standingQueryIdColumn.set(standingQuery)
            )
            executeFuture(deleteStatement)
          }
          .named("cassandra-remove-standing-queries")
      )
      .map(_ => ())(ExecutionContexts.parasitic)
}
