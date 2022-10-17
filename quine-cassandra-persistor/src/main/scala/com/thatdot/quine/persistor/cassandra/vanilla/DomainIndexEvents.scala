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
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy
import com.datastax.oss.driver.api.querybuilder.select.Select
import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.{DomainIndexEvent, EventTime, NodeEvent}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.QuineId

trait DomainIndexEventColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dgnIdColumn: CassandraColumn[DomainGraphNodeId] = CassandraColumn[DomainGraphNodeId]("dgn_id")
  final protected val dataColumn: CassandraColumn[DomainIndexEvent] = CassandraColumn[DomainIndexEvent]("data")
}

class DomainIndexEvents(
  session: CqlSession,
  insertTimeout: FiniteDuration,
  writeConsistency: ConsistencyLevel,
  selectByQuineId: PreparedStatement,
  selectByQuineIdSinceTimestamp: PreparedStatement,
  selectByQuineIdUntilTimestamp: PreparedStatement,
  selectByQuineIdSinceUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineId: PreparedStatement,
  selectWithTimeByQuineIdSinceTimestamp: PreparedStatement,
  selectWithTimeByQuineIdUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineIdSinceUntilTimestamp: PreparedStatement,
  insert: PreparedStatement,
  selectByDgnId: PreparedStatement,
  deleteByQuineIdTimestamp: PreparedStatement
)(implicit materializer: Materializer)
    extends CassandraTable(session)
    with DomainIndexEventColumnNames
    with StrictLogging {

  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(DomainIndexEvents.arbitraryRowStatement)

  def persistEvents(id: QuineId, events: Seq[NodeEvent.WithTime]): Future[Unit] =
    executeFuture(
      BatchStatement
        .newInstance(
          DefaultBatchType.UNLOGGED,
          //TODO this filter is temporary until NodeEvent.WithTime is split into
          // separate types for NodeChangeEvent and DomainIndexEvent
          events.collect { case NodeEvent.WithTime(event: DomainIndexEvent, atTime) =>
            insert.bindColumns(
              quineIdColumn.set(id),
              timestampColumn.set(atTime),
              dgnIdColumn.set(event.dgnId),
              dataColumn.set(event)
            )
          }: _*
        )
        .setTimeout(insertTimeout.toJava)
        .setConsistencyLevel(writeConsistency)
    )

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime]] = executeSelect(
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
  ): Future[Iterable[DomainIndexEvent]] = selectColumn(
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

  def deleteByDgnId(id: DomainGraphNodeId): Future[Unit] = {
    /* TODO - testing for a proper value here; This is only a guess as to
     a reasonable default for delete parallelism */
    val deleteParallelism = 10
    executeSource(selectByDgnId.bindColumns(dgnIdColumn.set(id)))
      .map(row => (quineIdColumn.get(row), timestampColumn.get(row)))
      .mapAsyncUnordered(deleteParallelism)(t =>
        executeFuture(deleteByQuineIdTimestamp.bindColumns(quineIdColumn.set(t._1), timestampColumn.set(t._2)))
      )
      .runWith(Sink.last)
  }

}

object DomainIndexEvents extends TableDefinition with DomainIndexEventColumnNames {
  protected val tableName = "domain_index_events"
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[_]] = List(dgnIdColumn, dataColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement
      .withClusteringOrder(timestampColumn.name, ASC)
      .withCompaction(timeWindowCompactionStrategy)
      .build
      .setTimeout(createTableTimeout)

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

  val selectByDgnId: SimpleStatement =
    select.columns(quineIdColumn.name, timestampColumn.name).where(dgnIdColumn.is.eq).allowFiltering().build()

  val deleteStatement: SimpleStatement =
    delete
      .where(quineIdColumn.is.eq, timestampColumn.is.eq)
      .build()

  def create(
    session: CqlSession,
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit materializer: Materializer, futureMonad: Monad[Future]): Future[DomainIndexEvents] = {
    logger.debug("Preparing statements for {}", tableName)

    def prepare(
      statement: SimpleStatement,
      timeout: FiniteDuration = selectTimeout,
      consistencyLevel: ConsistencyLevel = readConsistency
    ): Future[PreparedStatement] = {
      logger.trace("Preparing {}", statement.getQuery)
      session.prepareAsync(statement.setTimeout(timeout.toJava).setConsistencyLevel(consistencyLevel)).toScala
    }

    val createdSchema = {

      if (shouldCreateTables)
        session.executeAsync(createTableStatement).toScala
      else
        Future.unit
    }

    // *> or .productR cannot be used in place of the .flatMap here, as that would run the Futures in parallel,
    // and we need the prepare statements to be executed after the table as been created.
    // Even though there is no "explicit" dependency being passed between the two parts.
    createdSchema.flatMap(_ =>
      (
        prepare(selectByQuineIdQuery.build()),
        prepare(selectByQuineIdSinceTimestampQuery),
        prepare(selectByQuineIdUntilTimestampQuery),
        prepare(selectByQuineIdSinceUntilTimestampQuery),
        prepare(selectWithTimeByQuineIdQuery.build()),
        prepare(selectWithTimeByQuineIdSinceTimestampQuery),
        prepare(selectWithTimeByQuineIdUntilTimestampQuery),
        prepare(selectWithTimeByQuineIdSinceUntilTimestampQuery),
        prepare(insertStatement, insertTimeout, writeConsistency),
        prepare(selectByDgnId, insertTimeout, writeConsistency),
        prepare(deleteStatement, insertTimeout, writeConsistency)
      ).mapN(new DomainIndexEvents(session, insertTimeout, writeConsistency, _, _, _, _, _, _, _, _, _, _, _))
    )(ExecutionContexts.parasitic)

  }
}
