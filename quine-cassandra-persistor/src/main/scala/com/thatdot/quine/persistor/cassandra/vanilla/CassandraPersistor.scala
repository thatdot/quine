package com.thatdot.quine.persistor.cassandra.vanilla

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.time.Duration

import scala.collection.compat._
import scala.collection.immutable
import scala.compat.ExecutionContexts
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import cats.Monad
import cats.implicits._
import cats.instances.future.catsStdInstancesForFuture
import com.codahale.metrics.MetricRegistry
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.ExtraTypeCodecs.BLOB_TO_ARRAY
import com.datastax.oss.driver.api.core.`type`.codec.{MappingCodec, PrimitiveLongCodec, TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql.{
  BatchStatement,
  BoundStatement,
  BoundStatementBuilder,
  DefaultBatchType,
  PreparedStatement,
  Row,
  SimpleStatement,
  Statement
}
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder.{ASC, DESC}
import com.datastax.oss.driver.api.core.{
  ConsistencyLevel,
  CqlIdentifier,
  CqlSession,
  CqlSessionBuilder,
  InvalidKeyspaceException,
  ProtocolVersion
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, deleteFrom, insertInto, selectFrom}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.{
  createKeyspace,
  createTable,
  timeWindowCompactionStrategy
}
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection
import com.datastax.oss.driver.api.querybuilder.relation.{ColumnRelationBuilder, Relation}
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import com.datastax.oss.driver.api.querybuilder.select.{Select, SelectFrom}
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent, StandingQuery, StandingQueryId, StandingQueryPartId}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.{
  MultipartSnapshotPersistenceAgent,
  PersistenceAgent,
  PersistenceCodecs,
  PersistenceConfig
}

object syntax {
  private def genericType[A](implicit tag: ClassTag[A]): GenericType[A] =
    GenericType.of[A](tag.runtimeClass.asInstanceOf[Class[A]])

  implicit class PrimitiveLongCodecSyntax(longCodec: PrimitiveLongCodec) {
    def xmap[B: ClassTag](from: Long => B, to: B => Long): TypeCodec[B] = new TypeCodec[B] {
      override val getJavaType: GenericType[B] = genericType
      override val getCqlType: DataType = longCodec.getCqlType
      override def encode(value: B, protocolVersion: ProtocolVersion): ByteBuffer =
        longCodec.encodePrimitive(to(value), protocolVersion)
      override def decode(bytes: ByteBuffer, protocolVersion: ProtocolVersion): B = from(
        longCodec.decodePrimitive(bytes, protocolVersion)
      )
      override def format(value: B): String = longCodec.format(to(value))
      override def parse(value: String): B = from(longCodec.parse(value))
    }
  }
  implicit class TypeCodecSynax[A](innerCodec: TypeCodec[A]) {
    def xmap[B: ClassTag](from: A => B, to: B => A): TypeCodec[B] =
      new MappingCodec[A, B](innerCodec, genericType) {
        override def innerToOuter(value: A): B = from(value)
        override def outerToInner(value: B): A = to(value)
      }
  }

  implicit class PreparedStatementBinding(statement: PreparedStatement) {
    def bindColumns(bindings: BoundStatementBuilder => BoundStatementBuilder*): BoundStatement =
      bindings.foldRight((statement.boundStatementBuilder()))(_ apply _).build()
  }
}

object CassandraCodecs {
  import syntax._
  implicit val byteArrayCodec: TypeCodec[Array[Byte]] = BLOB_TO_ARRAY
  implicit val stringCodec: TypeCodec[String] = TypeCodecs.TEXT
  implicit val intCodec: TypeCodec[Int] = TypeCodecs.INT.asInstanceOf[TypeCodec[Int]]
  implicit val quineIdCodec: TypeCodec[QuineId] = BLOB_TO_ARRAY.xmap(QuineId(_), _.array)
  implicit val standingQueryIdCodec: TypeCodec[StandingQueryId] = TypeCodecs.UUID.xmap(StandingQueryId(_), _.uuid)
  implicit val standingQueryPartIdCodec: TypeCodec[StandingQueryPartId] =
    TypeCodecs.UUID.xmap(StandingQueryPartId(_), _.uuid)

  /** [[EventTime]] is represented using Cassandra's 64-bit `bigint`
    *
    * Since event time ordering is unsigned, we need to shift over the raw
    * underlying long by [[Long.MinValue]] in order to ensure that ordering
    * gets mapped over properly to the signed `bigint` ordering.
    *
    * {{{
    * EventTime.MinValue -> 0L + Long.MaxValue + 1L = Long.MinValue   // smallest event time
    * EventTime.MaxValue -> -1L + Long.MaxValue + 1L = Long.MaxValue  // largest event time
    * }}}
    */
  implicit val eventTimeCodec: TypeCodec[EventTime] =
    TypeCodecs.BIGINT.xmap(x => EventTime.fromRaw(x - Long.MaxValue - 1L), x => x.eventTime + Long.MaxValue + 1L)

  implicit val nodeChangeEventCodec: TypeCodec[NodeChangeEvent] = BLOB_TO_ARRAY.xmap(
    PersistenceCodecs.eventFormat.read(_).get,
    PersistenceCodecs.eventFormat.write
  )
}
final case class CassandraColumn[A](name: CqlIdentifier, codec: TypeCodec[A]) {
  def cqlType: DataType = codec.getCqlType
  def set(bindMarker: CqlIdentifier, value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(bindMarker, value, codec)
  def set(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(name, value)(statementBuilder)
  def setLt(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(ltMarker, value)(statementBuilder)
  def setGt(value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    set(gtMarker, value)(statementBuilder)

  def get(row: Row): A = row.get[A](name, codec)

  private def prefixCqlId(prefix: String): CqlIdentifier = CqlIdentifier.fromInternal(prefix + name.asInternal)
  def gtMarker: CqlIdentifier = prefixCqlId("gt_")
  def ltMarker: CqlIdentifier = prefixCqlId("lt_")

  // Relation builders for use when constructing prepared statements.
  object is {
    private def relBuilder: ColumnRelationBuilder[Relation] = Relation.column(name)
    def eq: Relation = relBuilder.isEqualTo(bindMarker(name))
    def lte: Relation = relBuilder.isLessThanOrEqualTo(bindMarker(ltMarker))
    def gte: Relation = relBuilder.isGreaterThanOrEqualTo(bindMarker(gtMarker))
  }
}
object CassandraColumn {
  def apply[A](name: String)(implicit codec: TypeCodec[A]): CassandraColumn[A] =
    new CassandraColumn(CqlIdentifier.fromCql(name), codec)
}

abstract class TableDefinition extends LazyLogging {
  protected def tableName: String

  protected def partitionKey: CassandraColumn[_]
  protected def clusterKeys: List[CassandraColumn[_]]
  protected def dataColumns: List[CassandraColumn[_]]

  /** Start building a CREATE TABLE statement, based on the {{{partitionKey}}}, {{{clusterKeys}}}, and {{{dataColumns}}}
    * specified. Set any other desired options (e.g. {{{.withClusteringOrder}}}) and then call {{{.build()}}} to
    * get a CQL statement to execute.
    * @return a CreateTable builder
    */
  final protected def makeCreateTableStatement: CreateTable = {
    val createKeys: CreateTable = clusterKeys.foldLeft(
      createTable(tableName).ifNotExists.withPartitionKey(partitionKey.name, partitionKey.cqlType)
    )((t, c) => t.withClusteringColumn(c.name, c.cqlType))
    dataColumns.foldLeft(createKeys)((t, c) => t.withColumn(c.name, c.cqlType))
  }

  protected val createTableTimeout: Duration = Duration.ofSeconds(5)

  protected def select: SelectFrom = selectFrom(tableName)
  protected def delete: DeleteSelection = deleteFrom(tableName)

  // The head of the list looks needlessly special-cased. That's just type-safety in the Cassandra Query Builder's preventing you from constructing an INSERT
  // statement with no values inserted. We could bypass it by casting the insertInto(tableName) to RegularInsert. Or we could just go with the types.
  // This requires a non-empty list of columns to insert.
  // The first element is the partition key, anyways - could just treat that separately, and skip the non-empty list

  /** Make an insert statement using all the configured columns of the table.
    * It's marked as idempotent, as it is believed all INSERTs of this form will be, and
    * this statement is not modifiable / customizable after creation.
    * @return An ordinary CQL statement (preparing it with some bind markers is suggested)
    */
  protected def insertStatement: SimpleStatement = (clusterKeys ++ dataColumns)
    .foldLeft(
      insertInto(tableName).value(partitionKey.name, bindMarker(partitionKey.name))
    )((s, c) => s.value(c.name, bindMarker(c.name)))
    .build()
    .setIdempotent(true)

  /** Gets an arbitrary row from this table
    * @return an ordinary CQL statement to get a single row from this table, if any exists.
    */
  def arbitraryRowStatement: SimpleStatement = selectFrom(tableName).column(partitionKey.name).limit(1).build()
}

abstract class CassandraTable(session: CqlSession) {

  /** Does the table have any rows?
    */
  def nonEmpty(): Future[Boolean]
  def pair[A, B](columnA: CassandraColumn[A], columnB: CassandraColumn[B])(row: Row): (A, B) =
    (columnA.get(row), columnB.get(row))
  def triple[A, B, C](columnA: CassandraColumn[A], columnB: CassandraColumn[B], columnC: CassandraColumn[C])(
    row: Row
  ): (A, B, C) =
    (columnA.get(row), columnB.get(row), columnC.get(row))

  /** Helper method for wrapping Java Reactive Streams CQL execution in Akka Streams
    *
    * @param statement A CQL statement to be executed - either prepared or not.
    * @return an Akka Source of result rows - intended for things that return multiple results
    */
  final protected def executeSource(statement: Statement[_]): Source[ReactiveRow, NotUsed] =
    Source.fromPublisher(session.executeReactive(statement))

  /** Run a CQL query and collect the results to a Scala collection.
    *
    * @param statement The CQL query to execute.
    * @param rowFn A function to apply to transform each returned Cassandra row.
    * @tparam A The desired type of the elements.
    * @tparam C The collection type returned - e.g. {{{List[String]}}}
    * @return a Scala collection containing the result of applying rowFn to the returned Cassandra rows.
    */
  final protected def executeSelect[A, C](statement: Statement[_])(rowFn: Row => A)(implicit
    materializer: Materializer,
    cbf: Factory[A, C with immutable.Iterable[_]]
  ): Future[C] =
    executeSource(statement).map(rowFn).named("cassandra-select-query").runWith(Sink.collection)

  /** Same as {{{executeSelect}}}, just with a {{{CassandraColumn.get}}} as the {{{rowFn}}}
    *
    * @param statement The CQL query to execute.
    * @param col Which column to select from the Cassandra rows.
    * @tparam A The type of the selected column.
    * @tparam C The collection type returned - e.g. {{{List[String]}}}
    * @return a Scala collection containing the selected column.
    */
  final protected def selectColumn[A, C](statement: Statement[_], col: CassandraColumn[A])(implicit
    materializer: Materializer,
    cbf: Factory[A, C with immutable.Iterable[_]]
  ): Future[C] =
    executeSelect(statement)(col.get)

  final protected def selectColumns[A, B, C](
    statement: Statement[_],
    colA: CassandraColumn[A],
    colB: CassandraColumn[B]
  )(implicit
    materializer: Materializer,
    cbf: Factory[(A, B), C with immutable.Iterable[_]]
  ): Future[C] =
    executeSelect(statement)(pair(colA, colB))

  final protected def selectColumns[A, B, C, D](
    statement: Statement[_],
    colA: CassandraColumn[A],
    colB: CassandraColumn[B],
    colC: CassandraColumn[C]
  )(implicit
    materializer: Materializer,
    cbf: Factory[(A, B, C), D with immutable.Iterable[_]]
  ): Future[D] =
    executeSelect(statement)(triple(colA, colB, colC))

  /** Helper method for converting no-op results to {{{Future[Unit]}}}
    *
    * @param statement A CQL statemment to be executed - either prepared or not.
    * @return Unit - intended for INSERT or CREATE TABLE statements that don't return a useful result.
    */
  final protected def executeFuture(statement: Statement[_]): Future[Unit] =
    session.executeAsync(statement).thenApply[Unit](_ => ()).toScala

  /** Helper function to evaluate if a statement yields at least one result
    * @param statement The statement to test
    * @return a future that returns true iff the provided query yields at least 1 result
    */
  final protected def yieldsResults(statement: Statement[_]): Future[Boolean] =
    session.executeAsync(statement).thenApply[Boolean](_.currentPage.iterator.hasNext).toScala
}

trait JournalColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[NodeChangeEvent] = CassandraColumn[NodeChangeEvent]("data")
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

  def persistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]): Future[Unit] =
    executeFuture(
      BatchStatement
        .newInstance(
          DefaultBatchType.UNLOGGED,
          events map { case NodeChangeEvent.WithTime(event, atTime) =>
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
  ): Future[Iterable[NodeChangeEvent.WithTime]] = executeSelect(
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
  )(row => NodeChangeEvent.WithTime(dataColumn.get(row), timestampColumn.get(row)))

  def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeChangeEvent]] = selectColumn(
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
  protected val partitionKey = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[EventTime]] = List(timestampColumn)
  protected val dataColumns: List[CassandraColumn[NodeChangeEvent]] = List(dataColumn)

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

trait SnapshotsColumnNames {
  import CassandraCodecs._
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val timestampColumn: CassandraColumn[EventTime] = CassandraColumn[EventTime]("timestamp")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
  final protected val multipartIndexColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_index")
  final protected val multipartCountColumn: CassandraColumn[Int] = CassandraColumn[Int]("multipart_count")
}

object Snapshots extends TableDefinition with SnapshotsColumnNames {
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

  private val selectAllQuineIds: SimpleStatement = select.distinct
    .column(quineIdColumn.name)
    .build()

  def create(
    session: CqlSession,
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    insertTimeout: FiniteDuration,
    selectTimeout: FiniteDuration,
    shouldCreateTables: Boolean
  )(implicit
    futureMonad: Monad[Future]
  ): Future[Snapshots] = {
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
        prepare(getLatestTime.build().setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(getLatestTimeBefore.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(getParts.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
        prepare(selectAllQuineIds.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency))
      ).mapN(new Snapshots(session, _, _, _, _, _))
    )(ExecutionContexts.parasitic)
  }

}

class Snapshots(
  session: CqlSession,
  insertStatement: PreparedStatement,
  getLatestTimeStatement: PreparedStatement,
  getLatestTimeBeforeStatement: PreparedStatement,
  getPartsStatement: PreparedStatement,
  selectAllQuineIds: PreparedStatement
) extends CassandraTable(session)
    with SnapshotsColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(Snapshots.arbitraryRowStatement)

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

  def getLatestSnapshotTime(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[EventTime]] =
    session
      .executeAsync(upToTime match {
        case EventTime.MaxValue =>
          getLatestTimeStatement.bindColumns(quineIdColumn.set(id))
        case _ =>
          getLatestTimeBeforeStatement.bindColumns(
            quineIdColumn.set(id),
            timestampColumn.setLt(upToTime)
          )
      })
      .toScala
      .map(results => Option(results.one).map(timestampColumn.get))(ExecutionContexts.parasitic)

  def getSnapshotParts(
    id: QuineId,
    atTime: EventTime
  )(implicit mat: Materializer): Future[Seq[(Array[Byte], Int, Int)]] =
    selectColumns(
      getPartsStatement.bindColumns(
        quineIdColumn.set(id),
        timestampColumn.set(atTime)
      ),
      dataColumn,
      multipartIndexColumn,
      multipartCountColumn
    )

  def enumerateAllNodeIds(): Source[QuineId, NotUsed] =
    executeSource(selectAllQuineIds.bind()).map(quineIdColumn.get).named("cassandra-all-node-scan")
}

trait StandingQueriesColumnNames {
  import CassandraCodecs._
  import syntax._
  val standingQueryCodec: TypeCodec[StandingQuery] = {
    val format = PersistenceCodecs.standingQueryFormat
    BLOB_TO_ARRAY.xmap(format.read(_).get, format.write)
  }
  final protected val queryIdColumn: CassandraColumn[StandingQueryId] = CassandraColumn("query_id")
  final protected val queriesColumn: CassandraColumn[StandingQuery] = CassandraColumn("queries")(standingQueryCodec)
}

object StandingQueries extends TableDefinition with StandingQueriesColumnNames {
  protected val tableName = "standing_queries"
  protected val partitionKey = queryIdColumn
  protected val clusterKeys = List.empty
  protected val dataColumns: List[CassandraColumn[StandingQuery]] = List(queriesColumn)

  private val createTableStatement: SimpleStatement = makeCreateTableStatement.build.setTimeout(createTableTimeout)

  private val selectAllStatement: SimpleStatement = select
    .column(queriesColumn.name)
    .build()

  private val deleteStatement: SimpleStatement =
    delete
      .where(queryIdColumn.is.eq)
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
  ): Future[StandingQueries] = {
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
        prepare(selectAllStatement.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency))
      ).mapN(new StandingQueries(session, _, _, _))
    )(ExecutionContexts.parasitic)
  }
}

class StandingQueries(
  session: CqlSession,
  insertStatement: PreparedStatement,
  deleteStatement: PreparedStatement,
  selectAllStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with StandingQueriesColumnNames {

  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(StandingQueries.arbitraryRowStatement)

  def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    executeFuture(insertStatement.bindColumns(queryIdColumn.set(standingQuery.id), queriesColumn.set(standingQuery)))

  def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    executeFuture(deleteStatement.bindColumns(queryIdColumn.set(standingQuery.id)))

  def getStandingQueries: Future[List[StandingQuery]] =
    selectColumn(selectAllStatement.bind(), queriesColumn)
}

trait StandingQueryStatesColumnNames {
  import CassandraCodecs._
  final protected val standingQueryIdColumn: CassandraColumn[StandingQueryId] =
    CassandraColumn[StandingQueryId]("standing_query_id")
  final protected val quineIdColumn: CassandraColumn[QuineId] = CassandraColumn[QuineId]("quine_id")
  final protected val standingQueryPartIdColumn: CassandraColumn[StandingQueryPartId] =
    CassandraColumn[StandingQueryPartId]("standing_query_part_id")
  final protected val dataColumn: CassandraColumn[Array[Byte]] = CassandraColumn[Array[Byte]]("data")
}

object StandingQueryStates extends TableDefinition with StandingQueryStatesColumnNames {
  val tableName = "standing_query_states"
  //protected val indexName = "standing_query_states_idx"
  protected val partitionKey = quineIdColumn
  protected val clusterKeys: List[CassandraColumn[_]] = List(standingQueryIdColumn, standingQueryPartIdColumn)
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

  private val getStandingQueryStates =
    select
      .columns(standingQueryIdColumn.name, standingQueryPartIdColumn.name, dataColumn.name)
      .where(quineIdColumn.is.eq)
      .build()

  private val removeStandingQueryState =
    delete
      .where(
        quineIdColumn.is.eq,
        standingQueryIdColumn.is.eq,
        standingQueryPartIdColumn.is.eq
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
        prepare(getStandingQueryStates.setTimeout(selectTimeout.toJava).setConsistencyLevel(readConsistency)),
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
  getStandingQueryStatesStatement: PreparedStatement,
  removeStandingQueryStateStatement: PreparedStatement,
  getIdsForStandingQueryStatement: PreparedStatement,
  removeStandingQueryStatement: PreparedStatement
)(implicit mat: Materializer)
    extends CassandraTable(session)
    with StandingQueryStatesColumnNames {
  import syntax._

  def nonEmpty(): Future[Boolean] = yieldsResults(StandingQueryStates.arbitraryRowStatement)

  def getStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    executeSelect[((StandingQueryId, StandingQueryPartId), Array[Byte]), Map[
      (StandingQueryId, StandingQueryPartId),
      Array[Byte]
    ]](getStandingQueryStatesStatement.bindColumns(quineIdColumn.set(id)))(row =>
      (standingQueryIdColumn.get(row) -> standingQueryPartIdColumn.get(row)) -> dataColumn.get(row)
    )

  def setStandingQueryState(
    standingQuery: StandingQueryId,
    qid: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] =
    executeFuture(
      state match {
        case None =>
          removeStandingQueryStateStatement.bindColumns(
            quineIdColumn.set(qid),
            standingQueryIdColumn.set(standingQuery),
            standingQueryPartIdColumn.set(standingQueryId)
          )

        case Some(bytes) =>
          insertStatement.bindColumns(
            quineIdColumn.set(qid),
            standingQueryIdColumn.set(standingQuery),
            standingQueryPartIdColumn.set(standingQueryId),
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

trait MetaDataColumnName {
  import CassandraCodecs._
  final protected val keyColumn: CassandraColumn[String] = CassandraColumn("key")
  final protected val valueColumn: CassandraColumn[Array[Byte]] = CassandraColumn("value")
}

object MetaData extends TableDefinition with MetaDataColumnName {
  protected val tableName = "meta_data"
  protected val partitionKey = keyColumn
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

/** Persistence implementation backed by Cassandra.
  *
  * @param keyspace The keyspace the quine tables should live in.
  * @param replicationFactor
  * @param readConsistency
  * @param writeConsistency
  * @param writeTimeout How long to wait for a response when running an INSERT statement.
  * @param readTimeout How long to wait for a response when running a SELECT statement.
  * @param endpoints address(s) (host and port) of the Cassandra cluster to connect to.
  * @param localDatacenter If endpoints are specified, this argument is required. Default value on a new Cassandra install is 'datacenter1'.
  * @param shouldCreateTables Whether or not to create the required tables if they don't already exist.
  * @param shouldCreateKeyspace Whether or not to create the specified keyspace if it doesn't already exist. If it doesn't exist, it'll run {{{CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}}}}
  */
class CassandraPersistor(
  val persistenceConfig: PersistenceConfig,
  keyspace: String,
  replicationFactor: Int,
  readConsistency: ConsistencyLevel,
  writeConsistency: ConsistencyLevel,
  endpoints: List[InetSocketAddress],
  localDatacenter: String,
  writeTimeout: FiniteDuration,
  readTimeout: FiniteDuration,
  shouldCreateTables: Boolean,
  shouldCreateKeyspace: Boolean,
  metricRegistry: Option[MetricRegistry],
  val snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer
) extends PersistenceAgent
    with MultipartSnapshotPersistenceAgent {

  import MultipartSnapshotPersistenceAgent._

  val multipartSnapshotExecutionContext: ExecutionContext = materializer.executionContext

  // This is so we can have syntax like .mapN and .tupled, without making the parasitic ExecutionContext implicit.
  // Technically only Apply[Future] is required, but people might not be familiar with that,
  // and Applicative[Future] is too long to type.
  implicit private val futureMonad: Monad[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)

  // This is mutable, so needs to be a def to get a new one w/out prior settings.
  private def sessionBuilder: CqlSessionBuilder = CqlSession.builder
    .addContactPoints(endpoints.asJava)
    .withLocalDatacenter(localDatacenter)

  private def createQualifiedSession: CqlSession = metricRegistry
    .fold(sessionBuilder)(sessionBuilder.withMetricRegistry)
    .withKeyspace(keyspace)
    .build()

  private val session: CqlSession =
    try {
      val session = createQualifiedSession
      // Log a warning if the Cassandra keyspace replication factor does not match Quine configuration
      for { keyspaceMetadata <- session.getMetadata.getKeyspace(keyspace).asScala } {
        val keyspaceReplicationConfig = keyspaceMetadata.getReplication.asScala.toMap
        val clazz = keyspaceReplicationConfig.get("class")
        val factor = keyspaceReplicationConfig.get("replication_factor")
        if (
          clazz.contains("org.apache.cassandra.locator.SimpleStrategy") && !factor.contains(replicationFactor.toString)
        )
          logger.warn(
            s"Unexpected replication factor: $factor (expected: $replicationFactor) for Cassandra keyspace: $keyspace"
          )
      }
      session
    } catch {
      case _: InvalidKeyspaceException if shouldCreateKeyspace =>
        val sess = sessionBuilder.build()
        // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}
        sess.execute(createKeyspace(keyspace).ifNotExists().withSimpleStrategy(replicationFactor).build())
        sess.close()
        createQualifiedSession
    }

  private val (journals, snapshots, standingQueries, standingQueryStates, metaData) = Await.result(
    (
      Journals.create(session, readConsistency, writeConsistency, writeTimeout, readTimeout, shouldCreateTables),
      Snapshots.create(session, readConsistency, writeConsistency, writeTimeout, readTimeout, shouldCreateTables),
      StandingQueries.create(
        session,
        readConsistency,
        writeConsistency,
        writeTimeout,
        readTimeout,
        shouldCreateTables
      ),
      StandingQueryStates.create(
        session,
        readConsistency,
        writeConsistency,
        writeTimeout,
        readTimeout,
        shouldCreateTables
      ),
      MetaData.create(session, readConsistency, writeConsistency, writeTimeout, readTimeout, shouldCreateTables)
    ).tupled,
    5.seconds
  )

  override def emptyOfQuineData(): Future[Boolean] = {
    val dataTables = Seq(journals, snapshots, standingQueries, standingQueryStates)
    // then combine them -- if any have results, then the system is not empty of quine data
    Future
      .traverse(dataTables)(_.nonEmpty())(implicitly, ExecutionContexts.parasitic)
      .map(_.exists(identity))(ExecutionContexts.parasitic)
  }

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = journals.enumerateAllNodeIds()

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = snapshots.enumerateAllNodeIds()

  override def persistEvents(id: QuineId, events: Seq[NodeChangeEvent.WithTime]): Future[Unit] =
    journals.persistEvents(id, events)

  override def getJournal(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeChangeEvent]] =
    journals
      .getJournal(id, startingAt, endingAt)

  def getJournalWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeChangeEvent.WithTime]] =
    journals
      .getJournalWithTime(id, startingAt, endingAt)

  override def persistSnapshotPart(
    id: QuineId,
    atTime: EventTime,
    part: MultipartSnapshotPart
  ): Future[Unit] = {
    val MultipartSnapshotPart(bytes, index, count) = part
    snapshots.persistSnapshotPart(id, atTime, bytes, index, count)
  }

  override def getLatestMultipartSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[MultipartSnapshot]] =
    snapshots
      .getLatestSnapshotTime(id, upToTime)
      .map({
        case Some(time) =>
          snapshots
            .getSnapshotParts(id, time)
            .map(parts =>
              parts
                .map { case (partBytes, partIndex, partCount) =>
                  MultipartSnapshotPart(partBytes, partIndex, partCount)
                }
            )(ExecutionContexts.parasitic)
            .map(multipartSnapshotParts => Some(MultipartSnapshot(time, multipartSnapshotParts)))(
              ExecutionContexts.parasitic
            )
        case _ => Future.successful(None)
      })(multipartSnapshotExecutionContext)
      .flatten

  override def persistStandingQuery(standingQuery: StandingQuery): Future[Unit] =
    standingQueries.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQuery): Future[Unit] = {
    // Just do the removal of standing query states as fire-and-forget in the background,
    // as it could take a while.
    standingQueryStates
      .removeStandingQuery(standingQuery.id)
      .onComplete {
        case Success(_) => ()
        case Failure(e) =>
          logger.error("Error when removing rows from table " + StandingQueryStates.tableName, e)
      }(materializer.executionContext)
    standingQueries.removeStandingQuery(standingQuery)
  }

  override def getStandingQueries: Future[List[StandingQuery]] =
    standingQueries.getStandingQueries

  override def getStandingQueryStates(id: QuineId): Future[Map[(StandingQueryId, StandingQueryPartId), Array[Byte]]] =
    standingQueryStates.getStandingQueryStates(id)

  override def setStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: StandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = standingQueryStates.setStandingQueryState(
    standingQuery,
    id,
    standingQueryId,
    state
  )

  override def getMetaData(key: String): Future[Option[Array[Byte]]] = metaData.getMetaData(key)

  override def getAllMetaData(): Future[Map[String, Array[Byte]]] = metaData.getAllMetaData()

  override def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    metaData.setMetaData(key, newValue)

  override def shutdown(): Future[Unit] = session.closeAsync().toScala.void

}
