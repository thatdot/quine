package com.thatdot.quine.persistor.cassandra

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import cats.Applicative
import cats.data.NonEmptyList
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.quine.graph.{EventTime, NamespaceId, NodeChangeEvent, NodeEvent}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.cassandra.support.{
  CassandraCodecs,
  CassandraColumn,
  CassandraStatementSettings,
  CassandraTable,
  TableDefinition,
  syntax,
}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.{T2, T9}

trait JournalColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[NodeChangeEvent] = CassandraColumn[NodeChangeEvent]("data")
}
abstract class JournalsTableDefinition(namespace: NamespaceId)
    extends TableDefinition[Journals]("journals", namespace)
    with JournalColumnNames {
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[NodeChangeEvent]] = List(dataColumn)

  protected val createTableStatement: SimpleStatement =
    makeCreateTableStatement
      .withClusteringOrder(timestampColumn.name, ASC)
      .withCompaction(timeWindowCompactionStrategy)
      .build
      .setTimeout(ddlTimeout)

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
        timestampColumn.is.lte,
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
        timestampColumn.is.lte,
      )
      .build()

  def create(
    session: CqlSession,
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
  )(implicit
    materializer: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[Journals] = {
    import shapeless.syntax.std.tuple._ // to concatenate tuples
    logger.debug(safe"Preparing statements for ${Safe(tableName.toString)}")

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
        selectWithTimeByQuineIdSinceUntilTimestampQuery,
      ).map(prepare(session, readSettings)).toTuple ++
      T2(insertStatement, deleteAllByPartitionKeyStatement)
        .map(prepare(session, writeSettings))
        .toTuple
    ).mapN(
      new Journals(
        session,
        chunker,
        writeSettings,
        firstRowStatement,
        dropTableStatement,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
      ),
    )

  }
}

class Journals(
  session: CqlSession,
  chunker: Chunker,
  writeSettings: CassandraStatementSettings,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
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
  deleteByQuineId: PreparedStatement,
)(implicit materializer: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with JournalColumnNames {
  import syntax._

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")

  def persistEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    chunker(events.toList) { events =>
      executeFuture(
        writeSettings(
          BatchStatement
            .newInstance(
              BatchType.UNLOGGED,
              events.map { case NodeEvent.WithTime(event, atTime) =>
                insert.bindColumns(
                  quineIdColumn.set(id),
                  timestampColumn.set(atTime),
                  dataColumn.set(event),
                )
              }.toList: _*,
            ),
        ),
      )
    }

  def deleteEvents(qid: QuineId): Future[Unit] = executeFuture(
    deleteByQuineId.bindColumns(quineIdColumn.set(qid)),
  )

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = executeSelect(
    (startingAt, endingAt) match {
      case (EventTime.MinValue, EventTime.MaxValue) =>
        selectWithTimeByQuineId.bindColumns(quineIdColumn.set(id))

      case (EventTime.MinValue, _) =>
        selectWithTimeByQuineIdUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(endingAt),
        )

      case (_, EventTime.MaxValue) =>
        selectWithTimeByQuineIdSinceTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
        )

      case _ =>
        selectWithTimeByQuineIdSinceUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
          timestampColumn.setLt(endingAt),
        )
    },
  )(row => NodeEvent.WithTime(dataColumn.get(row), timestampColumn.get(row)))

  def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Iterable[NodeEvent]] = selectColumn(
    (startingAt, endingAt) match {
      case (EventTime.MinValue, EventTime.MaxValue) =>
        selectByQuineId.bindColumns(quineIdColumn.set(id))

      case (EventTime.MinValue, _) =>
        selectByQuineIdUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(endingAt),
        )

      case (_, EventTime.MaxValue) =>
        selectByQuineIdSinceTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
        )

      case _ =>
        selectByQuineIdSinceUntilTimestamp.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setGt(startingAt),
          timestampColumn.setLt(endingAt),
        )
    },
    dataColumn,
  )
}
