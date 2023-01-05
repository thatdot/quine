package com.thatdot.quine.persistor.cassandra.vanilla

import scala.compat.ExecutionContexts
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source

import cats.Monad
import cats.implicits._
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType, PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.ASC
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.quine.graph.{EventTime, NodeEvent}
import com.thatdot.quine.model.QuineId
trait JournalColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[NodeEvent] = CassandraColumn[NodeEvent]("data")
}

class Journals(
  session: CqlSession,
  insertTimeout: FiniteDuration,
  writeConsistency: ConsistencyLevel,
  selectAllQuineIds: PreparedStatement,
  selectByQuineId: PreparedStatement,
  selectByQuineIdSinceTimestamp: PreparedStatement,
  selectByQuineIdUntilTimestamp: PreparedStatement,
  selectByQuineIdSinceUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineId: PreparedStatement,
  selectWithTimeByQuineIdSinceTimestamp: PreparedStatement,
  selectWithTimeByQuineIdUntilTimestamp: PreparedStatement,
  selectWithTimeByQuineIdSinceUntilTimestamp: PreparedStatement,
  insert: PreparedStatement
)(implicit materializer: Materializer)
    extends CassandraTable(session)
    with JournalColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(Journals.arbitraryRowStatement)

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")

  def persistEvents(id: QuineId, events: Seq[NodeEvent.WithTime]): Future[Unit] =
    executeFuture(
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

object Journals extends TableDefinition with JournalColumnNames {
  protected val tableName = "journals"
  protected val partitionKey: CassandraColumn[QuineId] = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[NodeEvent]] = List(dataColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement
      .withClusteringOrder(timestampColumn.name, ASC)
      .withCompaction(timeWindowCompactionStrategy)
      .build
      .setTimeout(createTableTimeout)

  private val selectAllQuineIds: SimpleStatement = select.distinct
    .column(quineIdColumn.name)
    .build()

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
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit materializer: Materializer, futureMonad: Monad[Future]): Future[Journals] = {
    logger.debug("Preparing statements for {}", tableName)

    def prepare(statement: SimpleStatement): Future[PreparedStatement] = {
      logger.trace("Preparing {}", statement.getQuery)
      session.prepareAsync(statement).toScala
    }

    val createdSchema =
      if (shouldCreateTables)
        session.executeAsync(createTableStatement).toScala
      else
        Future.unit

    // *> or .productR cannot be used in place of the .flatMap here, as that would run the Futures in parallel,
    // and we need the prepare statements to be executed after the table as been created.
    // Even though there is no "explicit" dependency being passed between the two parts.
    createdSchema.flatMap(_ =>
      (
        prepare(selectAllQuineIds.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(selectByQuineIdQuery.build().setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(
          selectByQuineIdSinceTimestampQuery.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectByQuineIdUntilTimestampQuery.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectByQuineIdSinceUntilTimestampQuery.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectWithTimeByQuineIdQuery.build().setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectWithTimeByQuineIdSinceTimestampQuery
            .setTimeout(selectTimeout.toJava)
            .setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectWithTimeByQuineIdUntilTimestampQuery
            .setTimeout(selectTimeout.toJava)
            .setConsistencyLevel(readConsistency)
        ),
        prepare(
          selectWithTimeByQuineIdSinceUntilTimestampQuery
            .setTimeout(selectTimeout.toJava)
            .setConsistencyLevel(readConsistency)
        ),
        prepare(insertStatement.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency))
      ).mapN(new Journals(session, insertTimeout, writeConsistency, _, _, _, _, _, _, _, _, _, _))
    )(ExecutionContexts.parasitic)

  }
}
