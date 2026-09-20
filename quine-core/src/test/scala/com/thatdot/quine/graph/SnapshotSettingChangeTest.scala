package com.thatdot.quine.graph

import java.util.concurrent.{ConcurrentHashMap, ConcurrentNavigableMap}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.{PropertyValue, QuineValue}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** A node slept under one persistence setting and woken under another must come back with the
  * state it had, and must leave storage in the shape the new setting expects, so that changing
  * the setting needs no migration beyond letting nodes wake.
  *
  * Every graph here shares one store, standing in for one persistor across restarts with
  * different configuration.
  */
class SnapshotSettingChangeTest extends AsyncFunSuite with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val namespace: NamespaceId = defaultNamespaceId

  private val snapshots = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]]()
  private val journals = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]]()
  private val domainIndexEvents = new ConcurrentHashMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]]()
  private val metaData = new ConcurrentHashMap[String, Array[Byte]]()

  private def makeGraph(name: String, persistenceConfig: PersistenceConfig): GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        persistenceConfig,
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = journals,
            domainIndexEvents = domainIndexEvents,
            snapshots = snapshots,
            metaData = metaData,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)

    val graph = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      timeout.duration,
    )
    graph.requiredGraphIsReady()
    graph
  }

  /** Journaling on, with a threshold high enough that sleeping never writes a snapshot, so a node's state lives
    * only in its journal.
    */
  private val journaledOnly: GraphService =
    makeGraph("setting-change-journaled", PersistenceConfig(snapshotAfterEvents = 1000))

  private val singleton: GraphService =
    makeGraph("setting-change-singleton", PersistenceConfig(journalEnabled = false, snapshotSingleton = true))

  private val timeKeyed: GraphService =
    makeGraph("setting-change-time-keyed", PersistenceConfig(snapshotAfterEvents = 0))

  implicit val ec: ExecutionContextExecutor = singleton.system.dispatcher

  override def afterAll(): Unit = {
    Await.result(singleton.shutdown(), timeout.duration)
    Await.result(timeKeyed.shutdown(), timeout.duration)
    Await.result(journaledOnly.shutdown(), timeout.duration)
  }

  private def setProp(graph: GraphService, qid: QuineId, name: String, value: Long): Future[Unit] =
    graph.literalOps(namespace).setProp(qid, name, QuineValue.Integer(value))

  private def sleepAndAwait(graph: GraphService, qid: QuineId): Future[Unit] = {
    def stillAwake(): Future[Boolean] =
      graph
        .relayAsk(
          graph.shardFromNode(qid).quineRef,
          ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
        )
        .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))
        .map(_.contains(qid))

    graph.requestNodeSleep(namespace, qid).flatMap { _ =>
      Patterns
        .retry(
          () => stillAwake().filter(_ == false),
          attempts = 100,
          delay = 50.millis,
          graph.system.scheduler,
          graph.system.dispatcher,
        )
        .map(_ => ())
    }
  }

  /** Reading properties wakes the node, so this also stands in for "wake under this graph's setting". */
  private def propNames(graph: GraphService, qid: QuineId): Future[Set[String]] =
    graph.literalOps(namespace).getProps(qid).map((props: Map[Symbol, PropertyValue]) => props.keySet.map(_.name))

  private def storedSnapshotKeys(qid: QuineId): Set[EventTime] =
    Option(snapshots.get(qid)).fold(Set.empty[EventTime])(_.keySet.asScala.toSet)

  test("a singleton snapshot is rekeyed on a time-keyed wake, so later snapshots are the ones read") {
    val qid = idProvider.customIdToQid(1L)
    val rekeyedBefore = timeKeyed.metrics.snapshotsRekeyedOnWake.getCount
    for {
      _ <- setProp(singleton, qid, "first", 1L)
      _ <- sleepAndAwait(singleton, qid)
      keysAfterSingletonSleep = storedSnapshotKeys(qid)
      wokenTimeKeyed <- propNames(timeKeyed, qid)
      keysAfterRekey = storedSnapshotKeys(qid)
      _ <- setProp(timeKeyed, qid, "second", 2L)
      _ <- sleepAndAwait(timeKeyed, qid)
      wokenAgain <- propNames(timeKeyed, qid)
    } yield {
      keysAfterSingletonSleep shouldBe Set(EventTime.MaxValue)
      wokenTimeKeyed shouldBe Set("first")
      keysAfterRekey should not contain EventTime.MaxValue
      keysAfterRekey should have size 1
      timeKeyed.metrics.snapshotsRekeyedOnWake.getCount shouldBe rekeyedBefore + 1
      // Had the singleton row stayed, it would sort after the snapshot just written and be read instead of it.
      wokenAgain shouldBe Set("first", "second")
    }
  }

  /** Puts back a singleton row as a failed delete would have left it. */
  private def restoreSingletonRow(qid: QuineId, bytes: Array[Byte]): Unit = {
    val _ = snapshots.get(qid).put(EventTime.MaxValue, bytes)
  }

  test("a singleton row a failed delete left behind does not shadow the snapshot written since") {
    val qid = idProvider.customIdToQid(5L)
    for {
      _ <- setProp(singleton, qid, "first", 1L)
      _ <- sleepAndAwait(singleton, qid)
      singletonBytes = snapshots.get(qid).get(EventTime.MaxValue)
      _ <- propNames(timeKeyed, qid)
      _ <- setProp(timeKeyed, qid, "second", 2L)
      _ <- sleepAndAwait(timeKeyed, qid)
      _ = restoreSingletonRow(qid, singletonBytes)
      keysWithLeftover = storedSnapshotKeys(qid)
      woken <- propNames(timeKeyed, qid)
      keysAfterWake = storedSnapshotKeys(qid)
      _ <- sleepAndAwait(timeKeyed, qid)
      wokenAgain <- propNames(timeKeyed, qid)
    } yield {
      keysWithLeftover should contain(EventTime.MaxValue)
      // The leftover row sorts last, so it is what the wake reads first; the newer time-keyed row must win.
      woken shouldBe Set("first", "second")
      keysAfterWake should not contain EventTime.MaxValue
      wokenAgain shouldBe Set("first", "second")
    }
  }

  test("a singleton row left by a crash between the move and the delete is removed without a second move") {
    val qid = idProvider.customIdToQid(6L)
    for {
      _ <- setProp(singleton, qid, "first", 1L)
      _ <- sleepAndAwait(singleton, qid)
      singletonBytes = snapshots.get(qid).get(EventTime.MaxValue)
      _ <- propNames(timeKeyed, qid)
      _ <- sleepAndAwait(timeKeyed, qid)
      keysAfterMove = storedSnapshotKeys(qid)
      _ = restoreSingletonRow(qid, singletonBytes)
      rekeyedBefore = timeKeyed.metrics.snapshotsRekeyedOnWake.getCount
      woken <- propNames(timeKeyed, qid)
      keysAfterWake = storedSnapshotKeys(qid)
    } yield {
      keysAfterMove should have size 1
      woken shouldBe Set("first")
      keysAfterWake shouldBe keysAfterMove
      timeKeyed.metrics.snapshotsRekeyedOnWake.getCount shouldBe rekeyedBefore
    }
  }

  test("time-keyed snapshots collapse to the singleton row on a singleton wake") {
    val qid = idProvider.customIdToQid(2L)
    for {
      _ <- setProp(timeKeyed, qid, "first", 1L)
      _ <- sleepAndAwait(timeKeyed, qid)
      keysAfterTimeKeyedSleep = storedSnapshotKeys(qid)
      wokenSingleton <- propNames(singleton, qid)
      keysAfterRekey = storedSnapshotKeys(qid)
      _ <- setProp(singleton, qid, "second", 2L)
      _ <- sleepAndAwait(singleton, qid)
      wokenAgain <- propNames(singleton, qid)
    } yield {
      keysAfterTimeKeyedSleep should have size 1
      keysAfterTimeKeyedSleep should not contain EventTime.MaxValue
      wokenSingleton shouldBe Set("first")
      keysAfterRekey shouldBe Set(EventTime.MaxValue)
      wokenAgain shouldBe Set("first", "second")
    }
  }

  test("a snapshot already keyed for the current setting is left as it is") {
    val qid = idProvider.customIdToQid(3L)
    val rekeyedBefore = timeKeyed.metrics.snapshotsRekeyedOnWake.getCount
    for {
      _ <- setProp(timeKeyed, qid, "first", 1L)
      _ <- sleepAndAwait(timeKeyed, qid)
      keysBefore = storedSnapshotKeys(qid)
      _ <- propNames(timeKeyed, qid)
    } yield {
      storedSnapshotKeys(qid) shouldBe keysBefore
      timeKeyed.metrics.snapshotsRekeyedOnWake.getCount shouldBe rekeyedBefore
    }
  }

  test("state that exists only in the journal survives journaling being turned off") {
    val qid = idProvider.customIdToQid(4L)
    for {
      _ <- setProp(journaledOnly, qid, "first", 1L)
      _ <- setProp(journaledOnly, qid, "second", 2L)
      _ <- sleepAndAwait(journaledOnly, qid)
      keysAfterJournaledSleep = storedSnapshotKeys(qid)
      wokenWithoutJournaling <- propNames(singleton, qid)
      _ <- sleepAndAwait(singleton, qid)
      keysAfterUnjournaledSleep = storedSnapshotKeys(qid)
      wokenAgain <- propNames(singleton, qid)
    } yield {
      keysAfterJournaledSleep shouldBe empty
      wokenWithoutJournaling shouldBe Set("first", "second")
      // The replayed events are durable nowhere the new setting writes, so the sleep that follows snapshots them.
      keysAfterUnjournaledSleep shouldBe Set(EventTime.MaxValue)
      wokenAgain shouldBe Set("first", "second")
    }
  }
}
