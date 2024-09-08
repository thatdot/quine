package com.thatdot.quine.persistor.cassandra

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import cats.Applicative
import cats.syntax.apply._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.DESC
import com.datastax.oss.driver.api.querybuilder.select.Select

import com.thatdot.quine.graph.{EventTime, NamespaceId}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.MultipartSnapshotPersistenceAgent.MultipartSnapshotPart
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.{T2, T4}

trait SnapshotsColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
  final protected val multipartIndexColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_index")
  final protected val multipartCountColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_count")
}

abstract class SnapshotsTableDefinition(namespace: NamespaceId)
    extends TableDefinition[Snapshots]("snapshots", namespace)
    with SnapshotsColumnNames {
  protected val partitionKey = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[_]] = List(timestampColumn, multipartIndexColumn)
  protected val dataColumns: List[CassandraColumn[_]] = List(dataColumn, multipartCountColumn)

  protected val createTableStatement: SimpleStatement =
    makeCreateTableStatement.withClusteringOrder(timestampColumn.name, DESC).build.setTimeout(ddlTimeout)

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
    chunker: Chunker,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
  )(implicit
    materializer: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[Snapshots] = {
    import shapeless.syntax.std.tuple._
    logger.debug(log"Preparing statements for ${(Safe(tableName.toString))}")

    (
      T2(insertStatement, deleteAllByPartitionKeyStatement)
        .map(prepare(session, writeSettings))
        .toTuple ++
      T4(getLatestTime.build, getLatestTimeBefore, getParts, selectAllQuineIds)
        .map(prepare(session, readSettings))
        .toTuple
    ).mapN(new Snapshots(session, firstRowStatement, dropTableStatement, _, _, _, _, _, _))
  }

}

class Snapshots(
  session: CqlSession,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteByQidStatement: PreparedStatement,
  getLatestTimeStatement: PreparedStatement,
  getLatestTimeBeforeStatement: PreparedStatement,
  getPartsStatement: PreparedStatement,
  selectAllQuineIds: PreparedStatement,
) extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with SnapshotsColumnNames {
  import syntax._

  def persistSnapshotPart(
    id: QuineId,
    atTime: EventTime,
    part: Array[Byte],
    partIndex: Int,
    partCount: Int,
  ): Future[Unit] = executeFuture(
    insertStatement.bindColumns(
      quineIdColumn.set(id),
      timestampColumn.set(atTime),
      dataColumn.set(part),
      multipartIndexColumn.set(partIndex),
      multipartCountColumn.set(partCount),
    ),
  )

  def deleteAllByQid(id: QuineId): Future[Unit] = executeFuture(deleteByQidStatement.bindColumns(quineIdColumn.set(id)))

  def getLatestSnapshotTime(
    id: QuineId,
    upToTime: EventTime,
  ): Future[Option[EventTime]] = queryOne(
    upToTime match {
      case EventTime.MaxValue =>
        getLatestTimeStatement.bindColumns(quineIdColumn.set(id))
      case _ =>
        getLatestTimeBeforeStatement.bindColumns(
          quineIdColumn.set(id),
          timestampColumn.setLt(upToTime),
        )
    },
    timestampColumn,
  )

  def getSnapshotParts(
    id: QuineId,
    atTime: EventTime,
  )(implicit mat: Materializer): Future[Seq[MultipartSnapshotPart]] =
    executeSelect(
      getPartsStatement.bindColumns(
        quineIdColumn.set(id),
        timestampColumn.set(atTime),
      ),
    )(row => MultipartSnapshotPart(dataColumn.get(row), multipartIndexColumn.get(row), multipartCountColumn.get(row)))

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")
}
