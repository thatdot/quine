package com.thatdot.quine.persistor.cassandra

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

import org.apache.pekko.stream.Materializer

import cats.Applicative
import cats.implicits._
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlIdentifier, CqlSession}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.persistor.ConditionalWriteResult
import com.thatdot.quine.persistor.cassandra.support._
import com.thatdot.quine.util.T2
trait MetaDataColumnName {
  import CassandraCodecs._
  final protected val keyColumn: CassandraColumn[String] = CassandraColumn("key")
  final protected val valueColumn: CassandraColumn[Array[Byte]] = CassandraColumn("value")
}

case class MetaDataCreateConfig(
  session: CqlSession,
  verifyTable: CqlIdentifier => Future[Unit],
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  shouldCreateTables: Boolean,
)

object MetaDataDefinition
    extends TableDefinition[MetaData, MetaDataCreateConfig]("meta_data", defaultNamespaceId)
    with MetaDataColumnName {
  protected val partitionKey: CassandraColumn[String] = keyColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[Array[Byte]]] = List(valueColumn)

  protected val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(ddlTimeout)

  private val selectAllStatement: SimpleStatement =
    select
      .columns(keyColumn.name, valueColumn.name)
      .build()

  private val selectSingleStatement: SimpleStatement =
    select
      .column(valueColumn.name)
      .where(keyColumn.is.eq)
      .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(keyColumn.is.eq)
      .build()
      .setIdempotent(true)

  def create(config: MetaDataCreateConfig)(implicit
    mat: Materializer,
    futureInstance: Applicative[Future],
    logConfig: LogConfig,
  ): Future[MetaData] = {
    import shapeless.syntax.std.tuple._
    logger.debug(safe"Preparing statements for ${Safe(tableName.toString)}")

    val createdSchema = futureInstance.whenA(config.shouldCreateTables)(
      config.session
        .executeAsync(createTableStatement)
        .asScala
        .flatMap(_ => config.verifyTable(tableName))(ExecutionContext.parasitic),
    )

    createdSchema.flatMap(_ =>
      (
        T2(insertStatement, deleteStatement).map(prepare(config.session, config.writeSettings)).toTuple ++
        T2(selectAllStatement, selectSingleStatement).map(prepare(config.session, config.readSettings)).toTuple
      ).mapN(
        new MetaData(config.session, tableName, config.writeSettings, firstRowStatement, dropTableStatement, _, _, _, _),
      ),
    )(ExecutionContext.parasitic)
  }
}

class MetaData(
  session: CqlSession,
  /** Needed by [[setMetaDataIfValue]], which builds its statements as CQL rather than through the
    * prepared-statement plumbing; every other statement here arrives already built.
    */
  tableName: CqlIdentifier,
  /** Also needed by [[setMetaDataIfValue]]. Every other statement here arrives with these already
    * applied by the prepared-statement plumbing, which the conditional statements bypass.
    */
  writeSettings: CassandraStatementSettings,
  firstRowStatement: SimpleStatement,
  dropTableStatement: SimpleStatement,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
  selectSingleStatement: PreparedStatement,
)(implicit mat: Materializer)
    extends CassandraTable(session, firstRowStatement, dropTableStatement)
    with MetaDataColumnName {

  import syntax._

  def getMetaData(key: String): Future[Option[Array[Byte]]] =
    queryOne(
      selectSingleStatement.bindColumns(keyColumn.set(key)),
      valueColumn,
    )

  def getAllMetaData(): Future[Map[String, Array[Byte]]] =
    selectColumns(selectAllStatement.bind(), keyColumn, valueColumn)

  /** Compare-and-set on the stored value, as a Cassandra lightweight transaction.
    *
    * Built as unprepared statements on purpose. Preparing them would mean widening this table's
    * prepared-statement plumbing (and its constructor) by three, to serve a path that runs at
    * control-plane rates: a few writes a minute, against an operation that already pays four Paxos
    * round trips. The statement-cache saving is not measurable next to that, and the
    * plumbing would be.
    *
    * Deliberately NOT marked idempotent: the driver must not silently retry an LWT whose outcome
    * it could not read, because a retried conditional write is a different question ("does it
    * still match?") from the one that timed out, and answering the new question would report
    * `Conflict` for a write that in fact applied.
    */
  def setMetaDataIfValue(
    key: String,
    expected: Option[Array[Byte]],
    newValue: Option[Array[Byte]],
  ): Future[ConditionalWriteResult] = {
    val table = tableName.asCql(true)
    val k = keyColumn.name.asCql(true)
    val v = valueColumn.name.asCql(true)
    def buf(bytes: Array[Byte]): ByteBuffer = ByteBuffer.wrap(bytes)

    val statement = (expected, newValue) match {
      // Claim a key nobody holds. IF NOT EXISTS is the only form that can lose to a concurrent
      // first writer rather than overwrite it.
      case (None, Some(value)) =>
        SimpleStatement.newInstance(s"INSERT INTO $table ($k, $v) VALUES (?, ?) IF NOT EXISTS", key, buf(value))
      // Ordinary update: replace a value we still believe is there.
      case (Some(previous), Some(value)) =>
        SimpleStatement.newInstance(
          s"UPDATE $table SET $v = ? WHERE $k = ? IF $v = ?",
          buf(value),
          key,
          buf(previous),
        )
      // Conditional removal.
      case (Some(previous), None) =>
        SimpleStatement.newInstance(s"DELETE FROM $table WHERE $k = ? IF $v = ?", key, buf(previous))
      // "Delete it if it is already absent" is vacuously satisfied, and issuing a DELETE with no
      // condition would be an unconditional write, the one thing this method exists not to do.
      case (None, None) =>
        return Future.successful(ConditionalWriteResult.Written)
    }

    session
      // Two settings that would otherwise come from the driver's own profile, where an operator
      // can change them without knowing what depends on them.
      //
      // SERIAL rather than LOCAL_SERIAL: Paxos then reaches a quorum in every datacenter, which is
      // the scope a caller fencing on this needs. The driver already defaults to SERIAL, so
      // nothing on the wire changes today; what this stops is a multi-datacenter deployment
      // tuned to LOCAL_SERIAL for latency, which would narrow the exclusion to one datacenter
      // while callers still assumed cluster-wide atomicity. Two cluster-ingest coordinators in
      // different datacenters could then each win the same compare-and-set and take the same
      // fencing rank, silently. The extra cross-datacenter round trip is affordable where this
      // runs: control-plane writes, a few a minute.
      //
      // `writeSettings` for the rest: the commit phase lands at the same consistency as every
      // other write to this table instead of the driver's default of LOCAL_ONE, and the
      // operator's write timeout replaces a default sized for ordinary writes: this statement
      // pays up to four Paxos round trips and cannot be retried, so a timeout scaled to a
      // one-round-trip INSERT turns load into spurious failures.
      .executeAsync(writeSettings(statement).setSerialConsistencyLevel(ConsistencyLevel.SERIAL))
      .asScala
      .map { resultSet =>
        val row = resultSet.one()
        // A `null` row cannot happen for an LWT (Cassandra always returns the [applied] row), but
        // reading it as "applied" on a nonsense response would report durability we never saw.
        if (row == null) ConditionalWriteResult.Conflict(None)
        else if (row.getBoolean("[applied]")) ConditionalWriteResult.Written
        else {
          // On refusal the row carries the CURRENT column values, which is what the caller needs
          // to rebase onto. The column is absent from the row when the refusal was "no such row"
          // (a conditional update or delete against a key that has since been removed).
          val current =
            if (row.getColumnDefinitions.contains(valueColumn.name)) Option(valueColumn.get(row)) else None
          ConditionalWriteResult.Conflict(current)
        }
      }(ExecutionContext.parasitic)
  }

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    executeFuture(
      newValue match {
        case None => deleteStatement.bindColumns(keyColumn.set(key))
        case Some(value) => insertStatement.bindColumns(keyColumn.set(key), valueColumn.set(value))
      },
    )

}
