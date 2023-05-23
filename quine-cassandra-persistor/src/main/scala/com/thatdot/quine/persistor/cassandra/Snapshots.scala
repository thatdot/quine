package com.thatdot.quine.persistor.cassandra

import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.DESC
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.quine.graph.EventTime
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.MultipartSnapshotPersistenceAgent.MultipartSnapshotPart
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.{T2, T4}

trait SnapshotsColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
  final protected val multipartIndexColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_index")
  final protected val multipartCountColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_count")
}

abstract class SnapshotsTableDefinition extends TableDefinition with SnapshotsColumnNames {
  protected val tableName = "snapshots"
  protected val partitionKey = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[_]] = List(timestampColumn, multipartIndexColumn)
  protected val dataColumns: List[CassandraColumn[_]] = List(dataColumn, multipartCountColumn)

  private val createTableStatement: SimpleStatement =
    makeCreateTableStatement.withClusteringOrder(timestampColumn.name, DESC).build.setTimeout(createTableTimeout)

  private val getLatestTime: Select =
    select
      .columns(timestampColumn.name)
      .where(quineIdColumn.is.eq)
      .limit(1)

  private val getLatestTimeBefore: SimpleStatement =
    getLatestTime
      .where(timestampColumn.is.lte)
      .build()

  private val getParts: SimpleStatement = select
    .columns(dataColumn.name, multipartIndexColumn.name, multipartCountColumn.name)
    .where(quineIdColumn.is.eq)
    .where(timestampColumn.is.eq)
    .build()

  protected val selectAllQuineIds: SimpleStatement

  def create(
    session: CqlSession,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
    shouldCreateTables: Boolean
  )(implicit
    futureInstance: Applicative[Future]
  ): Future[Snapshots] = {
    import shapeless.syntax.std.tuple._
    logger.debug("Preparing statements for {}", tableName)

    val createdSchema = futureInstance.whenA(
      shouldCreateTables
    )(session.executeAsync(createTableStatement).toScala)

    createdSchema.flatMap(_ =>
      (
        T2(insertStatement, deleteAllByPartitionKeyStatement)
          .map(prepare(session, writeSettings))
          .toTuple ++
        T4(getLatestTime.build, getLatestTimeBefore, getParts, selectAllQuineIds)
          .map(prepare(session, readSettings))
          .toTuple
      ).mapN(new Snapshots(session, firstRowStatement, _, _, _, _, _, _))
    )(ExecutionContexts.parasitic)
  }

}

class Snapshots(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteByQidStatement: PreparedStatement,
  getLatestTimeStatement: PreparedStatement,
  getLatestTimeBeforeStatement: PreparedStatement,
  getPartsStatement: PreparedStatement,
  selectAllQuineIds: PreparedStatement
) extends CassandraTable(session)
    with SnapshotsColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(firstRowStatement)

  def persistSnapshotPart(
    id: QuineId,
    atTime: EventTime,
    part: Array[Byte],
    partIndex: Int,
    partCount: Int
  ): Future[Unit] = executeFuture(
    insertStatement.bindColumns(
      quineIdColumn.set(id),
      timestampColumn.set(atTime),
      dataColumn.set(part),
      multipartIndexColumn.set(partIndex),
      multipartCountColumn.set(partCount)
    )
  )

  def deleteAllByQid(id: QuineId): Future[Unit] = executeFuture(deleteByQidStatement.bindColumns(quineIdColumn.set(id)))

  def getLatestSnapshotTime(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[EventTime]] = queryOne(
    upToTime match {
      case EventTime.MaxValue =>
        getLatestTimeStatement.bindColumns(quineIdColumn.set(id))
      case _ =>
        getLatestTimeBeforeStatement.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(upToTime)
        )
    },
    timestampColumn
  )

  def getSnapshotParts(
    id: QuineId,
    atTime: EventTime
  )(implicit mat: Materializer): Future[Seq[MultipartSnapshotPart]] =
    executeSelect(
      getPartsStatement.bindColumns(
        quineIdColumn.set(id),
        timestampColumn.set(atTime)
      )
    )(row => MultipartSnapshotPart(dataColumn.get(row), multipartIndexColumn.get(row), multipartCountColumn.get(row)))

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")
}
