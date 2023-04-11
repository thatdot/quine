package com.thatdot.quine.persistor.cassandra.vanilla

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.time.Duration

import scala.collection.compat._
import scala.collection.immutable
import scala.compat.ExecutionContexts
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
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.ExtraTypeCodecs.BLOB_TO_ARRAY
import com.datastax.oss.driver.api.core.`type`.codec.{MappingCodec, PrimitiveLongCodec, TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{bindMarker, deleteFrom, insertInto, literal, selectFrom}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.{createKeyspace, createTable}
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection
import com.datastax.oss.driver.api.querybuilder.relation.{ColumnRelationBuilder, Relation}
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.{
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NodeChangeEvent,
  NodeEvent,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, EdgeDirection, QuineId}
import com.thatdot.quine.persistor.codecs.{DomainGraphNodeCodec, DomainIndexEventCodec, NodeChangeEventCodec}
import com.thatdot.quine.persistor.{MultipartSnapshotPersistenceAgent, PersistenceAgent, PersistenceConfig}

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
  implicit class TypeCodecSyntax[A](innerCodec: TypeCodec[A]) {
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
  implicit val symbolCodec: TypeCodec[Symbol] = TypeCodecs.TEXT.xmap(Symbol(_), _.name)
  implicit val intCodec: TypeCodec[Int] = TypeCodecs.INT.asInstanceOf[TypeCodec[Int]]
  implicit val longCodec: TypeCodec[Long] = TypeCodecs.BIGINT.asInstanceOf[TypeCodec[Long]]
  implicit def listCodec[A](implicit elemCodec: TypeCodec[A]): TypeCodec[Seq[A]] =
    TypeCodecs.listOf(elemCodec).xmap(_.asScala.toSeq, _.asJava)
  implicit def setCodec[A](implicit elemCodec: TypeCodec[A]): TypeCodec[Set[A]] =
    TypeCodecs.setOf(elemCodec).xmap(_.asScala.toSet, _.asJava)
  implicit val quineIdCodec: TypeCodec[QuineId] = BLOB_TO_ARRAY.xmap(QuineId(_), _.array)
  implicit val edgeDirectionCodec: TypeCodec[EdgeDirection] =
    TypeCodecs.TINYINT.xmap(b => EdgeDirection.values(b.intValue), _.index)
  implicit val standingQueryIdCodec: TypeCodec[StandingQueryId] = TypeCodecs.UUID.xmap(StandingQueryId(_), _.uuid)
  implicit val MultipleValuesStandingQueryPartIdCodec: TypeCodec[MultipleValuesStandingQueryPartId] =
    TypeCodecs.UUID.xmap(MultipleValuesStandingQueryPartId(_), _.uuid)

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
    NodeChangeEventCodec.format.read(_).get,
    NodeChangeEventCodec.format.write
  )
  implicit val domainIndexEventCodec: TypeCodec[DomainIndexEvent] = BLOB_TO_ARRAY.xmap(
    DomainIndexEventCodec.format.read(_).get,
    DomainIndexEventCodec.format.write
  )
  implicit val domainGraphNodeCodec: TypeCodec[DomainGraphNode] = BLOB_TO_ARRAY.xmap(
    DomainGraphNodeCodec.format.read(_).get,
    DomainGraphNodeCodec.format.write
  )

}
final case class CassandraColumn[A](name: CqlIdentifier, codec: TypeCodec[A]) {
  def cqlType: DataType = codec.getCqlType
  private def set(bindMarker: CqlIdentifier, value: A)(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(bindMarker, value, codec)
  def setSeq(values: Seq[A])(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(name, values, CassandraCodecs.listCodec(codec))
  def setSet(values: Set[A])(statementBuilder: BoundStatementBuilder): BoundStatementBuilder =
    statementBuilder.set(name, values, CassandraCodecs.setCodec(codec))
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
    // The usual "templated" prepared statement variant
    def in: Relation = relBuilder.in(bindMarker(name))
    // The inline literal variant - to put a literal into the statement instead of a bindMarker.
    def in(values: Iterable[A]): Relation = relBuilder.in(values.map(v => literal(v, codec): Term).asJava)
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

  protected def prepare(session: AsyncCqlSession, settings: CassandraStatementSettings)(
    statement: SimpleStatement
  ): Future[PreparedStatement] = {
    logger.trace("Preparing {}", statement.getQuery)
    session.prepareAsync(settings(statement)).toScala
  }

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
    .build
    .setIdempotent(true)

  // Used to delete all entries with a particular Quine Id, pretty much
  protected def deleteAllByPartitionKeyStatement: SimpleStatement = delete.where(partitionKey.is.eq).build

  /** Gets an arbitrary row from this table
    * @return an ordinary CQL statement to get a single row from this table, if any exists.
    */
  def arbitraryRowStatement: SimpleStatement = selectFrom(tableName).column(partitionKey.name).limit(1).build
}

abstract class CassandraTable(session: CqlSession) {

  /** Does the table have any rows?
    */
  def nonEmpty(): Future[Boolean]
  protected def pair[A, B](columnA: CassandraColumn[A], columnB: CassandraColumn[B])(row: Row): (A, B) =
    (columnA.get(row), columnB.get(row))

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

  final protected def queryFuture[A](statement: Statement[_], f: AsyncResultSet => A): Future[A] =
    session.executeAsync(statement).toScala.map(f)(ExecutionContexts.parasitic)

  /** Helper method for converting no-op results to {{{Future[Unit]}}}
    *
    * @param statement A CQL statemment to be executed - either prepared or not.
    * @return Unit - intended for INSERT or CREATE TABLE statements that don't return a useful result.
    */
  final protected def executeFuture(statement: Statement[_]): Future[Unit] =
    queryFuture(statement, _ => ())

  final protected def singleRow[A](col: CassandraColumn[A])(resultSet: AsyncResultSet): Option[A] =
    Option(resultSet.one()).map(col.get)

  /** Helper function to evaluate if a statement yields at least one result
    * @param statement The statement to test
    * @return a future that returns true iff the provided query yields at least 1 result
    */
  final protected def yieldsResults(statement: Statement[_]): Future[Boolean] =
    session.executeAsync(statement).thenApply[Boolean](_.currentPage.iterator.hasNext).toScala
}

// to be applied to a statement
case class CassandraStatementSettings(consistency: ConsistencyLevel, timeout: FiniteDuration) {
  import scala.compat.java8.DurationConverters.toJava
  def apply[SelfT <: Statement[SelfT]](statement: SelfT): SelfT =
    statement.setConsistencyLevel(consistency).setTimeout(toJava(timeout))
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
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  endpoints: List[InetSocketAddress],
  localDatacenter: String,
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
  implicit protected val futureMonad: Monad[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)

  // This is mutable, so needs to be a def to get a new one w/out prior settings.
  private def sessionBuilder: CqlSessionBuilder = CqlSession.builder
    .addContactPoints(endpoints.asJava)
    .withLocalDatacenter(localDatacenter)

  private def createQualifiedSession: CqlSession = metricRegistry
    .fold(sessionBuilder)(sessionBuilder.withMetricRegistry)
    .withKeyspace(keyspace)
    .build

  protected val session: CqlSession =
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
        val sess = sessionBuilder.build
        // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}
        sess.execute(createKeyspace(keyspace).ifNotExists().withSimpleStrategy(replicationFactor).build())
        sess.close()
        createQualifiedSession
    }

  private val (
    journals,
    snapshots,
    standingQueries,
    standingQueryStates,
    metaData,
    domainGraphNodes,
    domainIndexEvents
  ) = Await.result(
    (
      Journals.create(session, readSettings, writeSettings, shouldCreateTables),
      Snapshots.create(session, readSettings, writeSettings, shouldCreateTables),
      StandingQueries.create(session, readSettings, writeSettings, shouldCreateTables),
      StandingQueryStates.create(session, readSettings, writeSettings, shouldCreateTables),
      MetaData.create(session, readSettings, writeSettings, shouldCreateTables),
      DomainGraphNodes.create(session, readSettings, writeSettings, shouldCreateTables),
      DomainIndexEvents.create(session, readSettings, writeSettings, shouldCreateTables)
    ).tupled,
    5.seconds
  )

  protected def dataTables: List[CassandraTable] =
    List(journals, domainIndexEvents, snapshots, standingQueries, standingQueryStates, domainGraphNodes)
  // then combine them -- if any have results, then the system is not empty of quine data
  override def emptyOfQuineData(): Future[Boolean] =
    Future
      .traverse(dataTables)(_.nonEmpty())(implicitly, ExecutionContexts.parasitic)
      .map(_.exists(identity))(ExecutionContexts.parasitic)

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = journals.enumerateAllNodeIds()

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = snapshots.enumerateAllNodeIds()

  override def persistSnapshotPart(
    id: QuineId,
    atTime: EventTime,
    part: MultipartSnapshotPart
  ): Future[Unit] = {
    val MultipartSnapshotPart(bytes, index, count) = part
    snapshots.persistSnapshotPart(id, atTime, bytes, index, count)
  }
  override def deleteSnapshots(qid: QuineId): Future[Unit] = snapshots.deleteAllByQid(qid)

  override def getLatestMultipartSnapshot(
    id: QuineId,
    upToTime: EventTime
  ): Future[Option[MultipartSnapshot]] =
    snapshots
      .getLatestSnapshotTime(id, upToTime)
      .flatMap(
        _.traverse(time =>
          snapshots
            .getSnapshotParts(id, time)
            .map(MultipartSnapshot(time, _))(ExecutionContexts.parasitic)
        )
      )(multipartSnapshotExecutionContext)

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

  override def getMultipleValuesStandingQueryStates(
    id: QuineId
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    standingQueryStates.getMultipleValuesStandingQueryStates(id)

  override def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]]
  ): Future[Unit] = standingQueryStates.setStandingQueryState(
    standingQuery,
    id,
    standingQueryId,
    state
  )
  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] =
    standingQueryStates.deleteStandingQueryStates(id)
  override def getMetaData(key: String): Future[Option[Array[Byte]]] = metaData.getMetaData(key)

  override def getAllMetaData(): Future[Map[String, Array[Byte]]] = metaData.getAllMetaData()

  override def setMetaData(key: String, newValue: Option[Array[Byte]]): Future[Unit] =
    metaData.setMetaData(key, newValue)

  override def persistDomainGraphNodes(domainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] =
    this.domainGraphNodes.persistDomainGraphNodes(domainGraphNodes)

  override def removeDomainGraphNodes(domainGraphNodeIds: Set[DomainGraphNodeId]): Future[Unit] =
    this.domainGraphNodes.removeDomainGraphNodes(domainGraphNodeIds)

  override def getDomainGraphNodes(): Future[Map[DomainGraphNodeId, DomainGraphNode]] =
    this.domainGraphNodes.getDomainGraphNodes()

  override def shutdown(): Future[Unit] = session.closeAsync().toScala.void

  override def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = journals.getJournalWithTime(id, startingAt, endingAt)
  override def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] =
    domainIndexEvents.getJournalWithTime(id, startingAt, endingAt)

  override def persistNodeChangeEvents(id: QuineId, events: Seq[NodeEvent.WithTime[NodeChangeEvent]]): Future[Unit] =
    journals.persistEvents(id, events)

  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = journals.deleteEvents(qid)
  override def persistDomainIndexEvents(id: QuineId, events: Seq[NodeEvent.WithTime[DomainIndexEvent]]): Future[Unit] =
    domainIndexEvents.persistEvents(id, events)
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = domainIndexEvents.deleteEvents(qid)

  override def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    domainIndexEvents.deleteByDgnId(dgnId)
}
