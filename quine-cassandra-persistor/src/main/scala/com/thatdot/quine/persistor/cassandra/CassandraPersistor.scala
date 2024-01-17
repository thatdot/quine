package com.thatdot.quine.persistor.cassandra

import scala.collection.immutable
import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import cats.Applicative
import cats.data.NonEmptyList
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.all._
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace

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
import com.thatdot.quine.model.{DomainGraphNode, QuineId}
import com.thatdot.quine.persistor.cassandra.support.{CassandraStatementSettings, CassandraTable}
import com.thatdot.quine.persistor.{MultipartSnapshotPersistenceAgent, PersistenceAgent, PersistenceConfig}

/** This class exists because Scala 2 doesn't (natively) support polymorphic function values (lambdas),
  * like Scala 3 does.
  * Consider it an alias for the function signature it wraps.
  */
abstract class Chunker {
  def apply[A](things: immutable.Seq[A])(f: immutable.Seq[A] => Future[Unit]): Future[Unit]
}

/** Persistence implementation backed by Cassandra.
  *
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
abstract class CassandraPersistor(
  val persistenceConfig: PersistenceConfig,
  keyspace: String,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  shouldCreateTables: Boolean,
  shouldCreateKeyspace: Boolean,
  val snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer
) extends PersistenceAgent
    with MultipartSnapshotPersistenceAgent {

  import MultipartSnapshotPersistenceAgent._

  val multipartSnapshotExecutionContext: ExecutionContext = materializer.executionContext

  // This is so we can have syntax like .mapN and .tupled, without making the parasitic ExecutionContext implicit.
  implicit protected val futureInstance: Applicative[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)

  protected def session: CqlSession
  protected def journalsTableDef: JournalsTableDefinition
  protected def snapshotsTableDef: SnapshotsTableDefinition
  protected def chunker: Chunker

  /** An action verify the table before use. Use will be delayed until this Future completes
    * @param session
    * @param tableName
    * @return
    */
  protected def verifyTable(session: CqlSession)(tableName: String): Future[Unit]

  // TODO: this is getting a bit cursed. Come up with a way to create this class without blocking on Future
  // to get the table definitions (prepared statements).
  private lazy val (
    journals,
    snapshots,
    standingQueries,
    standingQueryStates,
    metaData,
    domainGraphNodes,
    domainIndexEvents
  ) = Await.result(
    (
      journalsTableDef.create(session, verifyTable(session), chunker, readSettings, writeSettings, shouldCreateTables),
      snapshotsTableDef.create(session, verifyTable(session), readSettings, writeSettings, shouldCreateTables),
      StandingQueries.create(session, verifyTable(session), readSettings, writeSettings, shouldCreateTables),
      StandingQueryStates.create(session, verifyTable(session), readSettings, writeSettings, shouldCreateTables),
      MetaData.create(session, verifyTable(session), readSettings, writeSettings, shouldCreateTables),
      DomainGraphNodes.create(session, verifyTable(session), chunker, readSettings, writeSettings, shouldCreateTables),
      DomainIndexEvents.create(session, verifyTable(session), chunker, readSettings, writeSettings, shouldCreateTables)
    ).tupled,
    35.seconds
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

  override def persistNodeChangeEvents(
    id: QuineId,
    events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]]
  ): Future[Unit] =
    journals.persistEvents(id, events)

  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = journals.deleteEvents(qid)
  override def persistDomainIndexEvents(
    id: QuineId,
    events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]]
  ): Future[Unit] =
    domainIndexEvents.persistEvents(id, events)
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = domainIndexEvents.deleteEvents(qid)

  override def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    domainIndexEvents.deleteByDgnId(dgnId)
  def delete(): Future[Unit] = session.executeAsync(dropKeyspace(keyspace).build).thenApply[Unit](_ => ()).toScala
}
