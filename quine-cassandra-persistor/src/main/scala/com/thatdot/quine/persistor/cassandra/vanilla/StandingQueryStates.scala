package com.thatdot.quine.persistor.cassandra.vanilla

import scala.compat.ExecutionContexts
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.stream.Materializer
import akka.stream.scaladsl.Sink

import cats.Monad
import cats.implicits._
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}

import com.thatdot.quine.graph.{MultipleValuesStandingQueryPartId, StandingQueryId}
import com.thatdot.quine.model.QuineId

trait StandingQueryStatesColumnNames {
  import CassandraCodecs._
  final protected val standingQueryIdColumn: CassandraColumn[StandingQueryId] =
    CassandraColumn[StandingQueryId]("standing_query_id")
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val MultipleValuesStandingQueryPartIdColumn: CassandraColumn[MultipleValuesStandingQueryPartId] =
    CassandraColumn[MultipleValuesStandingQueryPartId]("standing_query_part_id")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
}

object StandingQueryStates extends TableDefinition with StandingQueryStatesColumnNames {
  val tableName = "standing_query_states"
  //protected val indexName = "standing_query_states_idx"
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[_]] =
    List(standingQueryIdColumn, MultipleValuesStandingQueryPartIdColumn)
  protected val dataColumns: List[CassandraColumn[Array[Byte]]] = List(dataColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement.build.setTimeout(createTableTimeout)

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
      .columns(standingQueryIdColumn.name, MultipleValuesStandingQueryPartIdColumn.name, dataColumn.name)
      .where(quineIdColumn.is.eq)
      .build()

  private val removeStandingQueryState =
    delete
      .where(
        quineIdColumn.is.eq,
        standingQueryIdColumn.is.eq,
        MultipleValuesStandingQueryPartIdColumn.is.eq
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
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit
    futureMonad: Monad[Future],
    mat: Materializer
  ): Future[StandingQueryStates] = {
    logger.debug("Preparing statements for {}", tableName)

    def prepare(statement: SimpleStatement): Future[PreparedStatement] = {
      logger.trace("Preparing {}", statement.getQuery)
      session.prepareAsync(statement).toScala
    }

    val createdSchema =
      if (shouldCreateTables)
        session
          .executeAsync(createTableStatement)
          .toScala
      //.flatMap(_ => session.executeAsync(createIndexStatement).toScala)(ExecutionContexts.parasitic)
      else
        Future.unit

    createdSchema.flatMap(_ =>
      (
        prepare(insertStatement.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency)),
        prepare(
          getMultipleValuesStandingQueryStates.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)
        ),
        prepare(removeStandingQueryState.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency)),
        prepare(getIdsForStandingQuery.setTimeout(insertTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(removeStandingQuery.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency))
      ).mapN(new StandingQueryStates(session, _, _, _, _, _))
    )(ExecutionContexts.parasitic)
  }

}
class StandingQueryStates(
  session: CqlSession,
  insertStatement: PreparedStatement,
  getMultipleValuesStandingQueryStatesStatement: PreparedStatement,
  removeStandingQueryStateStatement: PreparedStatement,
  getIdsForStandingQueryStatement: PreparedStatement,
  removeStandingQueryStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with StandingQueryStatesColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(StandingQueryStates.arbitraryRowStatement)

  def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    executeSelect[((StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]), Map[
      (StandingQueryId, MultipleValuesStandingQueryPartId),
      Array[Byte]
    ]](getMultipleValuesStandingQueryStatesStatement.bindColumns(quineIdColumn.set(id)))(row =>
      (standingQueryIdColumn.get(row) -> MultipleValuesStandingQueryPartIdColumn.get(row)) -> dataColumn.get(row)
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
            MultipleValuesStandingQueryPartIdColumn.set(standingQueryId)
          )

        case Some(bytes) =>
          insertStatement.bindColumns(
            quineIdColumn.set(qid),
            standingQueryIdColumn.set(standingQuery),
            MultipleValuesStandingQueryPartIdColumn.set(standingQueryId),
            dataColumn.set(bytes)
          )
      }
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
