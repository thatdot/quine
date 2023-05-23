package com.thatdot.quine.persistor.cassandra

import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.cassandra.support.{
  CassandraCodecs,
  CassandraColumn,
  CassandraStatementSettings,
  CassandraTable,
  TableDefinition,
  syntax
}
import com.thatdot.quine.util.{T2, T9}

trait JournalColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[NodeChangeEvent] = CassandraColumn[NodeChangeEvent]("data")
}
abstract class JournalsTableDefinition extends TableDefinition with JournalColumnNames {
  protected val tableName = "journals"
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[NodeChangeEvent]] = List(dataColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement
      .withClusteringOrder(timestampColumn.name, ASC)
      .withCompaction(timeWindowCompactionStrategy)
      .build
      .setTimeout(createTableTimeout)

  protected val selectAllQuineIds: SimpleStatement

  private val selectByQuineIdQuery: Select =
    select
      .column(dataColumn.name)
      .where(quineIdColumn.is.eq)

  private val selectByQuineIdSinceTimestampQuery: SimpleStatement =
    selectByQuineIdQuery
      .where(timestampColumn.is.gte)
      .build()

  private val selectByQuineIdUntilTimestampQuery: SimpleStatement =
    selectByQuineIdQuery
      .where(timestampColumn.is.lte)
      .build()

  private val selectByQuineIdSinceUntilTimestampQuery: SimpleStatement =
    selectByQuineIdQuery
      .where(
        timestampColumn.is.gte,
        timestampColumn.is.lte
      )
      .build()

  private val selectWithTimeByQuineIdQuery: Select =
    selectByQuineIdQuery
      .column(timestampColumn.name)

  private val selectWithTimeByQuineIdSinceTimestampQuery: SimpleStatement =
    selectWithTimeByQuineIdQuery
      .where(timestampColumn.is.gte)
      .build()

  private val selectWithTimeByQuineIdUntilTimestampQuery: SimpleStatement =
    selectWithTimeByQuineIdQuery
      .where(timestampColumn.is.lte)
      .build()

  private val selectWithTimeByQuineIdSinceUntilTimestampQuery: SimpleStatement =
    selectWithTimeByQuineIdQuery
      .where(
        timestampColumn.is.gte,
        timestampColumn.is.lte
      )
      .build()

  def create(
    session: CqlSession,
    verifyTable: String => Future[Unit],
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
    shouldCreateTables: Boolean
  )(implicit materializer: Materializer, futureInstance: Applicative[Future]): Future[Journals] = {
    import shapeless.syntax.std.tuple._ // to concatenate tuples
    logger.debug("Preparing statements for {}", tableName)

    val createdSchema = futureInstance.whenA(shouldCreateTables)(
      session
        .executeAsync(createTableStatement)
        .toScala
        .flatMap(_ => verifyTable(tableName))(ExecutionContexts.parasitic)
    )

    // *> or .productR cannot be used in place of the .flatMap here, as that would run the Futures in parallel,
    // and we need the prepare statements to be executed after the table as been created.
    // Even though there is no "explicit" dependency being passed between the two parts.
    createdSchema.flatMap(_ =>
      (
        T9(
          selectAllQuineIds,
          selectByQuineIdQuery.build,
          selectByQuineIdSinceTimestampQuery,
          selectByQuineIdUntilTimestampQuery,
          selectByQuineIdSinceUntilTimestampQuery,
          selectWithTimeByQuineIdQuery.build,
          selectWithTimeByQuineIdSinceTimestampQuery,
          selectWithTimeByQuineIdUntilTimestampQuery,
          selectWithTimeByQuineIdSinceUntilTimestampQuery
        ).map(prepare(session, readSettings)).toTuple ++
        T2(insertStatement, deleteAllByPartitionKeyStatement)
          .map(prepare(session, writeSettings))
          .toTuple
      ).mapN(new Journals(session, writeSettings, firstRowStatement, _, _, _, _, _, _, _, _, _, _, _))
    )(ExecutionContexts.parasitic)

  }
}

class Journals(
  session: CqlSession,
  writeSettings: CassandraStatementSettings,
  firstRowStatement: SimpleStatement,
  selectAllQuineIds: PreparedStatement,
  selectByQuineId: PreparedStatement,
  selectByQuineIdSinceTimestamp: PreparedStatement,
  selectByQuineIdUntilTimestamp: PreparedStatement,
  selectByQuineIdSinceUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineId: PreparedStatement,
  selectWithTimeByQuineIdSinceTimestamp: PreparedStatement,
  selectWithTimeByQuineIdUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineIdSinceUntilTimestamp: PreparedStatement,
  insert: PreparedStatement,
  deleteByQuineId: PreparedStatement
)(implicit materializer: Materializer)
    extends CassandraTable(session)
    with JournalColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(firstRowStatement)

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")

  def persistEvents(id: QuineId, events: Seq[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    executeFuture(
      writeSettings(
        BatchStatement
          .newInstance(
            BatchType.UNLOGGED,
            events map { case NodeEvent.WithTime(event, atTime) =>
              insert.bindColumns(
                quineIdColumn.set(id),
                timestampColumn.set(atTime),
                dataColumn.set(event)
              )
            }: _*
          )
      )
    )

  def deleteEvents(qid: QuineId): Future[Unit] = executeFuture(
    deleteByQuineId.bindColumns(quineIdColumn.set(qid))
  )

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = executeSelect(
    (startingAt, endingAt) match {
      case (EventTime.MinValue, EventTime.MaxValue) =>
        selectWithTimeByQuineId.bindColumns(quineIdColumn.set(id))

      case (EventTime.MinValue, _) =>
        selectWithTimeByQuineIdUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(endingAt)
        )

      case (_, EventTime.MaxValue) =>
        selectWithTimeByQuineIdSinceTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt)
        )

      case _ =>
        selectWithTimeByQuineIdSinceUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
          timestampColumn.setLt(endingAt)
        )
    }
  )(row => NodeEvent.WithTime(dataColumn.get(row), timestampColumn.get(row)))

  def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent]] = selectColumn(
    (startingAt, endingAt) match {
      case (EventTime.MinValue, EventTime.MaxValue) =>
        selectByQuineId.bindColumns(quineIdColumn.set(id))

      case (EventTime.MinValue, _) =>
        selectByQuineIdUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(endingAt)
        )

      case (_, EventTime.MaxValue) =>
        selectByQuineIdSinceTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt)
        )

      case _ =>
        selectByQuineIdSinceUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
          timestampColumn.setLt(endingAt)
        )
    },
    dataColumn
  )
}
