package com.thatdot.quine.persistor.cassandra

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink

import cats.Applicative
import cats.data.NonEmptyList
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.{DomainIndexEvent, EventTime, NamespaceId, NodeEvent}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.{T3, T9}

trait DomainIndexEventColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dgnIdColumn: CassandraColumn[DomainGraphNodeId] = CassandraColumn[DomainGraphNodeId]("dgn_id")
  final protected val dataColumn: CassandraColumn[DomainIndexEvent] = CassandraColumn[DomainIndexEvent]("data")
}

class DomainIndexEvents(
  session: CqlSession,
  chunker: Chunker,
  writeSettings: CassandraStatementSettings,
  val logConfig: LogConfig,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  selectByQuineId: PreparedStatement,
  selectByQuineIdSinceTimestamp: PreparedStatement,
  selectByQuineIdUntilTimestamp: PreparedStatement,
  selectByQuineIdSinceUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineId: PreparedStatement,
  selectWithTimeByQuineIdSinceTimestamp: PreparedStatement,
  selectWithTimeByQuineIdUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineIdSinceUntilTimestamp: PreparedStatement,
  selectByDgnId: PreparedStatement,
  insert: PreparedStatement,
  deleteByQuineIdTimestamp: PreparedStatement,
  deleteByQuineId: PreparedStatement,
)(implicit materializer: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with DomainIndexEventColumnNames
    with LazySafeLogging {

  import syntax._

  def persistEvents(id: QuineId, events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    chunker(events.toList) { events =>
      executeFuture(
        writeSettings(
          BatchStatement
            .newInstance(
              BatchType.UNLOGGED,
              events.map { case NodeEvent.WithTime(event: DomainIndexEvent, atTime) =>
                insert.bindColumns(
                  quineIdColumn.set(id),
                  timestampColumn.set(atTime),
                  dgnIdColumn.set(event.dgnId),
                  dataColumn.set(event),
                )
              }.toList: _*,
            ),
        ),
      )
    }

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] = executeSelect(
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
  ): Future[Iterable[DomainIndexEvent]] = selectColumn(
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

  def deleteByDgnId(id: DomainGraphNodeId): Future[Unit] = {
    /* TODO - testing for a proper value here; This is only a guess as to
     a reasonable default for delete parallelism */
    val deleteParallelism = 10
    executeSource(selectByDgnId.bindColumns(dgnIdColumn.set(id)))
      .map(pair(quineIdColumn, timestampColumn))
      .runWith(Sink.foreachAsync(deleteParallelism) { case (id, timestamp) =>
        executeFuture(deleteByQuineIdTimestamp.bindColumns(quineIdColumn.set(id), timestampColumn.set(timestamp)))
      })
      .map(_ => ())(ExecutionContext.parasitic)
  }

  def deleteEvents(qid: QuineId): Future[Unit] = executeFuture(
    deleteByQuineId.bindColumns(quineIdColumn.set(qid)),
  )

}

class DomainIndexEventsDefinition(namespace: NamespaceId)(implicit val logConfig: LogConfig)
    extends TableDefinition.DefaultType[DomainIndexEvents]("domain_index_events", namespace)
    with DomainIndexEventColumnNames {
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[_]] = List(dgnIdColumn, dataColumn)

  protected val createTableStatement: SimpleStatement =
    makeCreateTableStatement
      .withClusteringOrder(timestampColumn.name, ASC)
      .withCompaction(timeWindowCompactionStrategy)
      .build
      .setTimeout(ddlTimeout)

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

  val selectByDgnId: SimpleStatement =
    select.columns(quineIdColumn.name, timestampColumn.name).where(dgnIdColumn.is.eq).allowFiltering().build()

  val deleteStatement: SimpleStatement =
    delete
      .where(quineIdColumn.is.eq, timestampColumn.is.eq)
      .build()

  def create(config: TableDefinition.DefaultCreateConfig)(implicit
    materializer: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[DomainIndexEvents] = {
    import shapeless.syntax.std.tuple._
    logger.debug(safe"Preparing statements for ${Safe(tableName.toString)}")

    val selects = T9(
      selectByQuineIdQuery.build,
      selectByQuineIdSinceTimestampQuery,
      selectByQuineIdUntilTimestampQuery,
      selectByQuineIdSinceUntilTimestampQuery,
      selectWithTimeByQuineIdQuery.build,
      selectWithTimeByQuineIdSinceTimestampQuery,
      selectWithTimeByQuineIdUntilTimestampQuery,
      selectWithTimeByQuineIdSinceUntilTimestampQuery,
      selectByDgnId,
    ).map(prepare(config.session, config.readSettings))
    val updates = T3(
      insertStatement,
      deleteStatement,
      deleteAllByPartitionKeyStatement,
    ).map(prepare(config.session, config.writeSettings))
    (selects ++ updates).mapN(
      new DomainIndexEvents(
        config.session,
        config.chunker,
        config.writeSettings,
        logConfig,
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
        _,
      ),
    )

  }
}
