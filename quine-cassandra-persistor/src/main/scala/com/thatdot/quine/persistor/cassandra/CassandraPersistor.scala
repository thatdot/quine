package com.thatdot.quine.persistor.cassandra

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import cats.data.NonEmptyList
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace
import shapeless.poly._

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.quinepattern.{QueryPlan, QuinePatternUnimplementedException}
import com.thatdot.quine.graph.{
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQueryId,
  StandingQueryInfo,
  namespaceToString,
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.persistor.cassandra.support.{CassandraStatementSettings, CassandraTable, TableDefinition}
import com.thatdot.quine.persistor.{MultipartSnapshotPersistenceAgent, NamespacedPersistenceAgent, PersistenceConfig}
import com.thatdot.quine.util.Log.implicits._

/** Used to break up large batch queries on AWS Keyspaces - which doesn't support batches of over 30 elements
  * This class exists because Scala 2 doesn't (natively) support polymorphic function values (lambdas),
  * like Scala 3 does.
  * Consider it an alias for the function signature it wraps.
  */
abstract class Chunker {
  def apply[A](things: Seq[A])(f: Seq[A] => Future[Unit]): Future[Unit]
}

object NoOpChunker extends Chunker {
  def apply[A](things: Seq[A])(f: Seq[A] => Future[Unit]): Future[Unit] = f(things)
}

class SizeBoundedChunker(maxBatchSize: Int, parallelism: Int, materializer: Materializer) extends Chunker {
  def apply[A](things: Seq[A])(f: Seq[A] => Future[Unit]): Future[Unit] =
    if (things.lengthIs <= maxBatchSize) // If it can be done as a single batch, just run it w/out Pekko Streams
      f(things)
    else
      Source(things)
        .grouped(maxBatchSize)
        .runWith(Sink.foreachAsync(parallelism)(f))(materializer)
        .map(_ => ())(ExecutionContext.parasitic)
}

abstract class CassandraPersistorDefinition {
  protected def journalsTableDef(namespace: NamespaceId): JournalsTableDefinition
  protected def snapshotsTableDef(namespace: NamespaceId): SnapshotsTableDefinition
  def tablesForNamespace(namespace: NamespaceId)(implicit logConfig: LogConfig): (
    TableDefinition.DefaultType[Journals],
    TableDefinition.DefaultType[Snapshots],
    TableDefinition.DefaultType[StandingQueries],
    TableDefinition.DefaultType[StandingQueryStates],
    //TableDefinition.DefaultType[QuinePatterns],
    TableDefinition.DefaultType[DomainIndexEvents],
  ) = (
    journalsTableDef(namespace),
    snapshotsTableDef(namespace),
    new StandingQueriesDefinition(namespace),
    new StandingQueryStatesDefinition(namespace),
    //new QuinePatternsDefinition(namespace),
    new DomainIndexEventsDefinition(namespace),
  )

  def createTables(
    namespace: NamespaceId,
    session: CqlSession,
    verifyTable: CqlSession => CqlIdentifier => Future[Unit],
  )(implicit ec: ExecutionContext, logConfig: LogConfig): Future[Unit] =
    Future
      .traverse(tablesForNamespace(namespace).productIterator)(
        // TODO: This cast is perfectly safe, but to get rid of it:
        // 1) Replace `.productIterator` above with `.toList` provided by shapeless on tuples
        // which correctly returns List[TableDefinition], however, incorrectly only returns
        // the last element of the tuple, DominIndexEvents. It works fine if you call .toList
        // on the output of .tablesForNamespace().
        // 2) Extract a stand-alone reproduction
        // 3) Open a bug against shapeless with that reproduction
        _.asInstanceOf[TableDefinition.DefaultType[_]].executeCreateTable(session, verifyTable(session)),
      )
      .map(_ => ())(ExecutionContext.parasitic)
}

class PrepareStatements(
  session: CqlSession,
  chunker: Chunker,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
)(implicit materializer: Materializer, futureInstance: Applicative[Future], val logConfig: LogConfig)
    extends (TableDefinition.DefaultType ~> Future) {
  def apply[A](f: TableDefinition.DefaultType[A]): Future[A] =
    f.create(TableDefinition.DefaultCreateConfig(session, chunker, readSettings, writeSettings))
}

/** Persistence implementation backed by Cassandra.
  *
  * @param writeTimeout How long to wait for a response when running an INSERT statement.
  * @param readTimeout How long to wait for a response when running a SELECT statement.
  */
abstract class CassandraPersistor(
  val persistenceConfig: PersistenceConfig,
  session: CqlSession,
  namespace: NamespaceId,
  protected val snapshotPartMaxSizeBytes: Int,
)(implicit
  materializer: Materializer,
  logConfig: LogConfig,
) extends NamespacedPersistenceAgent
    with MultipartSnapshotPersistenceAgent {

  /** The current keyspace to which this persistor is connected, or None if not connected.
    */
  def keyspace: Option[String] = session.getKeyspace.toScala.map(_.asCql(true))

  import MultipartSnapshotPersistenceAgent._

  protected val multipartSnapshotExecutionContext: ExecutionContext = materializer.executionContext

  // This is so we can have syntax like .mapN and .tupled, without making the parasitic ExecutionContext implicit.
  implicit protected val futureInstance: Monad[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)

  protected def chunker: Chunker

  protected def journals: Journals
  protected def snapshots: Snapshots
  protected val standingQueries: StandingQueries
  protected val standingQueryStates: StandingQueryStates
  //protected val quinePatterns: QuinePatterns
  protected val domainIndexEvents: DomainIndexEvents

  protected def dataTables: List[CassandraTable] =
    List(journals, domainIndexEvents, snapshots, standingQueries, standingQueryStates)

  // then combine them -- if any have results, then the system is not empty of quine data
  override def emptyOfQuineData(): Future[Boolean] = dataTables.forallM(_.isEmpty())
  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = journals.enumerateAllNodeIds()

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = snapshots.enumerateAllNodeIds()

  override def persistSnapshotPart(
    id: QuineId,
    atTime: EventTime,
    part: MultipartSnapshotPart,
  ): Future[Unit] = {
    val MultipartSnapshotPart(bytes, index, count) = part
    snapshots.persistSnapshotPart(id, atTime, bytes, index, count)
  }
  override def deleteSnapshots(qid: QuineId): Future[Unit] = snapshots.deleteAllByQid(qid)

  override def getLatestMultipartSnapshot(
    id: QuineId,
    upToTime: EventTime,
  ): Future[Option[MultipartSnapshot]] =
    snapshots
      .getLatestSnapshotTime(id, upToTime)
      .flatMap(
        _.traverse(time =>
          snapshots
            .getSnapshotParts(id, time)
            .map { parts =>
              val partsWithinCount = parts.map(_.multipartCount).minOption match {
                case Some(minCount) =>
                  // Having more than one part count for a timestamp value is only valid when using singleton
                  // snapshots, as that re-uses a single timestamp value. A successful write of a larger snapshot over
                  // a smaller one will cause all rows to agree on the count. A successful write of a smaller snapshot
                  // over a larger one will leave the parts that go past the count. Those should be ignored.
                  parts.filter(_.multipartIndex < minCount)
                case None => parts
              }
              MultipartSnapshot(time, partsWithinCount)
            }(ExecutionContext.parasitic),
        ),
      )(multipartSnapshotExecutionContext)

  override def persistStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] =
    standingQueries.persistStandingQuery(standingQuery)

  override def removeStandingQuery(standingQuery: StandingQueryInfo): Future[Unit] = {
    // Just do the removal of standing query states as fire-and-forget in the background,
    // as it could take a while.
    standingQueryStates
      .removeStandingQuery(standingQuery.id)
      .onComplete {
        case Success(_) => ()
        case Failure(e) =>
          logger.error(
            log"Error deleting rows in namespace ${Safe(namespaceToString(namespace))} from standing query states table for ${standingQuery}"
            withException e,
          )
      }(materializer.executionContext)
    standingQueries.removeStandingQuery(standingQuery)
  }

  override def getStandingQueries: Future[List[StandingQueryInfo]] =
    standingQueries.getStandingQueries

  override def getMultipleValuesStandingQueryStates(
    id: QuineId,
  ): Future[Map[(StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte]]] =
    standingQueryStates.getMultipleValuesStandingQueryStates(id)

  override def setMultipleValuesStandingQueryState(
    standingQuery: StandingQueryId,
    id: QuineId,
    standingQueryId: MultipleValuesStandingQueryPartId,
    state: Option[Array[Byte]],
  ): Future[Unit] = standingQueryStates.setStandingQueryState(
    standingQuery,
    id,
    standingQueryId,
    state,
  )
  override def deleteMultipleValuesStandingQueryStates(id: QuineId): Future[Unit] =
    standingQueryStates.deleteStandingQueryStates(id)

  def containsMultipleValuesStates(): Future[Boolean] =
    standingQueryStates.isEmpty().map(!_)(ExecutionContext.parasitic)

  override def persistQueryPlan(standingQueryId: StandingQueryId, qp: QueryPlan): Future[Unit] =
    throw new QuinePatternUnimplementedException("Persisting query plans is not supported yet.")

  override def shutdown(): Future[Unit] = session.closeAsync().asScala.void

  override def getNodeChangeEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Iterable[NodeEvent.WithTime[NodeChangeEvent]]] = journals.getJournalWithTime(id, startingAt, endingAt)
  override def getDomainIndexEventsWithTime(
    id: QuineId,
    startingAt: EventTime,
    endingAt: EventTime,
  ): Future[Iterable[NodeEvent.WithTime[DomainIndexEvent]]] =
    domainIndexEvents.getJournalWithTime(id, startingAt, endingAt)

  override def persistNodeChangeEvents(
    id: QuineId,
    events: NonEmptyList[NodeEvent.WithTime[NodeChangeEvent]],
  ): Future[Unit] =
    journals.persistEvents(id, events)

  override def deleteNodeChangeEvents(qid: QuineId): Future[Unit] = journals.deleteEvents(qid)
  override def persistDomainIndexEvents(
    id: QuineId,
    events: NonEmptyList[NodeEvent.WithTime[DomainIndexEvent]],
  ): Future[Unit] =
    domainIndexEvents.persistEvents(id, events)
  override def deleteDomainIndexEvents(qid: QuineId): Future[Unit] = domainIndexEvents.deleteEvents(qid)

  override def deleteDomainIndexEventsByDgnId(dgnId: DomainGraphNodeId): Future[Unit] =
    domainIndexEvents.deleteByDgnId(dgnId)
  def delete(): Future[Unit] =
    Future.traverse(dataTables)(_.delete())(implicitly, materializer.executionContext).void

  def deleteKeyspace(): Future[Unit] = session.getKeyspace.toScala match {
    case Some(keyspace) =>
      session.executeAsync(dropKeyspace(keyspace).build).thenApply[Unit](_ => ()).asScala
    case None =>
      Future.failed(new RuntimeException("Can't drop keyspace when no keyspace set for " + session.getName))
  }

}
