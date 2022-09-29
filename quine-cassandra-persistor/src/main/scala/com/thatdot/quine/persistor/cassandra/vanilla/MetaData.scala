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
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, SimpleStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}

trait MetaDataColumnName {
  import CassandraCodecs._
  final protected val keyColumn: CassandraColumn[String] = CassandraColumn("key")
  final protected val valueColumn: CassandraColumn[Array[Byte]] = CassandraColumn("value")
}

object MetaData extends TableDefinition with MetaDataColumnName {
  protected val tableName = "meta_data"
  protected val partitionKey: CassandraColumn[String] = keyColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[Array[Byte]]] = List(valueColumn)

  private val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(createTableTimeout)

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

  def create(
    session: CqlSession,
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit
    mat: Materializer,
    futureMonad: Monad[Future]
  ): Future[MetaData] = {
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

    createdSchema.flatMap(_ =>
      (
        prepare(insertStatement.setTimeout(insertTimeout.toJava).setConsistencyLevel(writeConsistency)),
        prepare(deleteStatement.setConsistencyLevel(readConsistency)),
        prepare(selectAllStatement.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(selectSingleStatement.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency))
      ).mapN(new MetaData(session, _, _, _, _))
    )(ExecutionContexts.parasitic)
  }
}

class MetaData(
  session: CqlSession,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement,
  selectSingleStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with MetaDataColumnName {

  import syntax._

  def getMetaData(key: String): Future[Option[Array[Byte]]] =
    executeSource(selectSingleStatement.bindColumns(keyColumn.set(key)))
      .map(row => valueColumn.get(row))
      .named(s"cassandra-get-metadata-$key")
      .runWith(Sink.headOption)

  def getAllMetaData(): Future[Map[String, Array[Byte]]] =
    selectColumns[String, Array[Byte], Map[String, Array[Byte]]](selectAllStatement.bind(), keyColumn, valueColumn)

  def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    executeFuture(
      newValue match {
        case None => deleteStatement.bindColumns(keyColumn.set(key))
        case Some(value) => insertStatement.bindColumns(keyColumn.set(key), valueColumn.set(value))
      }
    )

  def nonEmpty(): Future[Boolean] = yieldsResults(MetaData.arbitraryRowStatement)
}
