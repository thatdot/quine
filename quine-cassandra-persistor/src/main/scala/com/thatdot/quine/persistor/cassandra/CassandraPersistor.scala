package com.thatdot.quine.persistor.cassandra

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future}
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

import com.thatdot.quine.graph.{
  DefaultNamespaceName,
  DomainIndexEvent,
  EventTime,
  MultipleValuesStandingQueryPartId,
  NamespaceId,
  NodeChangeEvent,
  NodeEvent,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphNode, QuineId}
import com.thatdot.quine.persistor.cassandra.support.{CassandraStatementSettings, CassandraTable, TableDefinition}
import com.thatdot.quine.persistor.{MultipartSnapshotPersistenceAgent, NamespacedPersistenceAgent, PersistenceConfig}

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
  def tablesForNamespace(namespace: NamespaceId): (
    TableDefinition[Journals],
    TableDefinition[Snapshots],
    TableDefinition[StandingQueries],
    TableDefinition[StandingQueryStates],
    TableDefinition[DomainIndexEvents]
  ) = (
    journalsTableDef(namespace),
    snapshotsTableDef(namespace),
    new StandingQueriesDefinition(namespace),
    new StandingQueryStatesDefinition(namespace),
    new DomainIndexEventsDefinition(namespace)
  )

  def createTables(
    namespace: NamespaceId,
    session: CqlSession,
    verifyTable: CqlSession => CqlIdentifier => Future[Unit]
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future
      .traverse(tablesForNamespace(namespace).productIterator)(
        // TODO: This cast is perfectly safe, but to get rid of it:
        // 1) Replace `.productIterator` above with `.toList` provided by shapeless on tuples
        // which correctly returns List[TableDefinition], however, incorrectly only returns
        // the last element of the tuple, DominIndexEvents. It works fine if you call .toList
        // on the output of .tablesForNamespace().
        // 2) Extract a stand-alone reproduction
        // 3) Open a bug against shapeless with that reproduction
        _.asInstanceOf[TableDefinition[_]].executeCreateTable(session, verifyTable(session))
      )
      .map(_ => ())(ExecutionContext.parasitic)
}

class PrepareStatements(
  session: CqlSession,
  chunker: Chunker,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings
)(implicit materializer: Materializer, futureInstance: Applicative[Future])
    extends (TableDefinition ~> Future) {
  def apply[A](f: TableDefinition[A]): Future[A] = f.create(session, chunker, readSettings, writeSettings)
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
  protected val snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer
) extends NamespacedPersistenceAgent
    with MultipartSnapshotPersistenceAgent {

  import MultipartSnapshotPersistenceAgent._

  protected val multipartSnapshotExecutionContext: ExecutionContext = materializer.executionContext

  // This is so we can have syntax like .mapN and .tupled, without making the parasitic ExecutionContext implicit.
  implicit protected val futureInstance: Monad[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)

  protected def chunker: Chunker

  protected def journals: Journals
  protected def snapshots: Snapshots
  protected val standingQueries: StandingQueries
  protected val standingQueryStates: StandingQueryStates
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
            .map(MultipartSnapshot(time, _))(ExecutionContext.parasitic)
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
          logger.error(
            s"Error deleting rows in namespace ${namespace getOrElse DefaultNamespaceName} from standing query states table for $standingQuery",
            e
          )
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
  def delete(): Future[Unit] =
    Future.traverse(dataTables)(_.delete())(implicitly, materializer.executionContext).void

  def deleteKeyspace(): Future[Unit] = session.getKeyspace.asScala match {
    case Some(keyspace) =>
      session.executeAsync(dropKeyspace(keyspace).build).thenApply[Unit](_ => ()).toScala
    case None =>
      Future.failed(new RuntimeException("Can't drop keyspace when no keyspace set for " + session.getName))
  }

}
