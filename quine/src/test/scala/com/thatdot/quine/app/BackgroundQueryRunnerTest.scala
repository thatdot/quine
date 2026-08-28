package com.thatdot.quine.app

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.UUID

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

import org.apache.pekko.stream.scaladsl.Sink

import cats.data.NonEmptyList
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import sttp.ws.WebSocketFrame

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.outputs2.kafka.KafkaExtensionProvider
import com.thatdot.quine.app.model.jobs._
import com.thatdot.quine.app.model.outputs2.query.standing.{LocalTapBus, TapBus}
import com.thatdot.quine.app.v2api.converters.Api2ToOutputs2
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.compiler.cypher.queryCypherValues
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{GraphService, defaultNamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.serialization.ProtobufSchemaCache

/** Exercises [[BackgroundQueryRunner]] end to end: an execution runs locally, streams its result rows
  * to the configured destinations, and records its lifecycle (with an expiry) under its id; a bad
  * query records `Failed`; cancellation stops the run and records the terminal `Cancelled` state.
  */
class BackgroundQueryRunnerTest extends AnyFunSuite with BeforeAndAfterAll with Eventually {

  implicit val logConfig: LogConfig = LogConfig.permissive
  private val graph: GraphService = IngestTestGraph.makeGraph("background-query-runner-test")
  implicit val ec: ExecutionContext = graph.nodeDispatcherEC
  implicit val idProvider: QuineIdProvider = graph.idProvider

  private val protobufSchemaCache: ProtobufSchemaCache = new ProtobufSchemaCache.AsyncLoading(graph.dispatchers)
  private val kafkaExtensions: KafkaExtensionProvider[com.thatdot.api.v2.SaslJaasConfig] = KafkaExtensionProvider.empty

  private val registry = new BackgroundQueryStatusRegistry(graph, hostPresent = _ == "test-host")
  private val tapBus = new LocalTapBus
  private val runner = new BackgroundQueryRunner(
    graph,
    "test-host",
    registry,
    tapBus,
    steps => Api2ToOutputs2.apply(steps)(graph, protobufSchemaCache, kafkaExtensions),
  )

  /** Convenience: stream to `Drop` unless a test needs to observe the destination. */
  private val drop: NonEmptyList[QuineDestinationSteps] = NonEmptyList.one(QuineDestinationSteps.Drop)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(100, Millis))

  override def beforeAll(): Unit =
    while (!graph.isReady) Thread.sleep(10)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  /** Dispatch a run, keeping only its terminal-state Future — these tests await outcomes, not the
    * `Started` write that [[BackgroundQueryRunner.Execution.started]] reports.
    */
  private def runDone(executionId: UUID, jobName: Option[String], backgroundQuery: BackgroundQuery): Future[Unit] =
    runner.run(executionId, jobName, backgroundQuery).done

  /** Run to terminal state and return the record. */
  private def runToTerminal(backgroundQuery: BackgroundQuery): BackgroundQueryRecord = {
    val executionId = UUID.randomUUID()
    Await.result(runDone(executionId, jobName = None, backgroundQuery), 15.seconds)
    Await.result(registry.get(executionId), 5.seconds).getOrElse(fail("no record after terminal run"))
  }

  private def count(label: String): Long = {
    val rows = Await.result(
      queryCypherValues(s"MATCH (n:$label) RETURN count(n)", defaultNamespaceId)(graph).results
        .runWith(Sink.seq)(graph.materializer),
      10.seconds,
    )
    Value.toJson(rows.head.head).asNumber.flatMap(_.toLong).getOrElse(fail("count was not a number"))
  }

  test("an execution runs locally and records Completed with a row count and a future expiry") {
    val before = Milliseconds.currentTime().millis
    val rec = runToTerminal(BackgroundQuery("CREATE (:Person {name: 'a'}) RETURN 1", drop))
    rec.lastAction match {
      case ExecutionAction.Completed(totalRowCount, _) => assert(totalRowCount == 1L)
      case other => fail(s"expected Completed, got $other")
    }
    assert(rec.expiresAtMillis > before, "expiry should be in the future")
    assert(count("Person") == 1L)
  }

  test("result rows are streamed to the destination while the full count is recorded (never truncated)") {
    val tmp = Files.createTempFile("bgq-runner-", ".jsonl")
    val toFile = NonEmptyList.one[QuineDestinationSteps](QuineDestinationSteps.File(tmp.toString))
    val rec = runToTerminal(BackgroundQuery("UNWIND range(1, 5) AS i CREATE (:Foo {i: i}) RETURN i", toFile))
    rec.lastAction match {
      case ExecutionAction.Completed(totalRowCount, columns) =>
        assert(totalRowCount == 5L, "full row count")
        assert(columns == Vector("i"), "column names recorded")
      case other => fail(s"expected Completed, got $other")
    }
    assert(count("Foo") == 5L, "all 5 CREATEs applied — execution was not truncated")
    // The rows reach the file destination (each row is a JSON object keyed by the column name `i`).
    eventually {
      val content = new String(Files.readAllBytes(tmp), UTF_8)
      assert("\"i\"".r.findAllIn(content).length == 5, s"all 5 rows streamed to the file; got: $content")
    }
  }

  test("a query that fails to compile records Failed") {
    val rec = runToTerminal(BackgroundQuery("THIS IS NOT VALID CYPHER", drop))
    assert(rec.lastAction.isInstanceOf[ExecutionAction.Failed], s"expected Failed, got ${rec.lastAction}")
  }

  test("a terminal record's expiry is finite and counted from termination") {
    val expiry = 60000L
    val before = Milliseconds.currentTime().millis
    val rec = runToTerminal(BackgroundQuery("RETURN 1", drop, statusExpiryMillis = expiry))
    val after = Milliseconds.currentTime().millis
    assert(rec.expiresAtMillis != Long.MaxValue, "a completed record must carry a finite expiry")
    assert(
      rec.expiresAtMillis >= before + expiry && rec.expiresAtMillis <= after + expiry,
      "expiry is stamped at termination + statusExpiry, not at start",
    )
  }

  test("a dead run's expired Started record is visible until the owner sweep collects it — never while in flight") {
    val executionId = UUID.randomUUID()
    val expired = BackgroundQueryRecord(
      executionId = executionId,
      jobName = None,
      namespace = defaultNamespaceId.name,
      hostId = "test-host",
      name = None,
      query = "RETURN 1",
      lastAction = ExecutionAction.Started(),
      expiresAtMillis = Milliseconds.currentTime().millis - 1, // stamp already past
    )
    Await.result(registry.put(expired), 5.seconds)
    // Started records are always visible — the stamp is a grace period, not a visibility bound.
    assert(
      Await.result(registry.get(executionId), 5.seconds).isDefined,
      "a Started record is visible past its expiry stamp",
    )
    // While the run is in flight on its owner, the owner sweep must leave the record alone.
    Await.result(registry.sweepOwnedBy("test-host", inFlight = _ == executionId), 5.seconds)
    assert(
      Await.result(registry.get(executionId), 5.seconds).isDefined,
      "an in-flight run's record survives the owner sweep, however stale its stamp",
    )
    // Once the run is no longer in flight (a dead run), the expired record is collected — not immortal.
    Await.result(registry.sweepOwnedBy("test-host", inFlight = _ => false), 5.seconds)
    assert(
      Await
        .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + executionId), 5.seconds)
        .isEmpty,
      "a dead run's expired Started record is physically swept rather than living forever",
    )
  }

  test("the manager sweep collects expired records of departed hosts only") {
    def failedRecord(executionId: UUID, host: String) = BackgroundQueryRecord(
      executionId = executionId,
      jobName = None,
      namespace = defaultNamespaceId.name,
      hostId = host,
      name = None,
      query = "RETURN 1",
      lastAction = ExecutionAction.Failed("stale"),
      expiresAtMillis = Milliseconds.currentTime().millis - 1, // expired
    )
    val orphanId = UUID.randomUUID()
    val ownedId = UUID.randomUUID()
    Await.result(registry.put(failedRecord(orphanId, "gone-host")), 5.seconds)
    Await.result(registry.put(failedRecord(ownedId, "test-host")), 5.seconds)
    Await.result(registry.sweepExpired(Milliseconds.currentTime()), 5.seconds)
    assert(
      Await
        .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + orphanId), 5.seconds)
        .isEmpty,
      "a departed host's expired record is collected by the manager sweep",
    )
    assert(
      Await
        .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + ownedId), 5.seconds)
        .nonEmpty,
      "a present host's expired record is its owner sweep's responsibility, not the manager's",
    )
    Await.result(registry.sweepOwnedBy("test-host", inFlight = _ => false), 5.seconds) // cleanup
  }

  test("startup reconciliation finalizes this host's own leftover Started records to Interrupted") {
    def startedRecord(id: UUID) = BackgroundQueryRecord(
      executionId = id,
      jobName = None,
      namespace = defaultNamespaceId.name,
      hostId = "test-host",
      name = None,
      query = "RETURN 1",
      lastAction = ExecutionAction.Started(),
      expiresAtMillis = Milliseconds.currentTime().millis + 1.hour.toMillis, // still in retention
    )
    val phantomId = UUID.randomUUID()
    val liveId = UUID.randomUUID()
    Await.result(registry.put(startedRecord(phantomId)), 5.seconds)
    Await.result(registry.put(startedRecord(liveId)), 5.seconds)

    // As if just restarted: the run behind `liveId` is (pretend) still in flight; `phantomId`'s is not.
    Await.result(registry.reconcileOwnedStarted("test-host", inFlight = _ == liveId), 5.seconds)

    assert(
      Await.result(registry.get(phantomId), 5.seconds).map(_.lastAction).contains(ExecutionAction.Interrupted()),
      "a leftover Started record with no live run is finalized to Interrupted, not left phantom until expiry",
    )
    assert(
      Await.result(registry.get(liveId), 5.seconds).map(_.lastAction).contains(ExecutionAction.Started()),
      "a Started record whose run is still in flight is left running",
    )
    List(phantomId, liveId).foreach(id =>
      Await.result(graph.namespacePersistor.setMetaData(BackgroundQueryStatusRegistry.KeyPrefix + id, None), 5.seconds),
    )
  }

  test("the manager sweep finalizes a departed host's still-in-retention Started record to Interrupted") {
    val orphanStartedId = UUID.randomUUID()
    Await.result(
      registry.put(
        BackgroundQueryRecord(
          executionId = orphanStartedId,
          jobName = None,
          namespace = defaultNamespaceId.name,
          hostId = "gone-host", // absent: hostPresent = _ == "test-host"
          name = None,
          query = "RETURN 1",
          lastAction = ExecutionAction.Started(),
          expiresAtMillis = Milliseconds.currentTime().millis + 1.hour.toMillis, // not expired
        ),
      ),
      5.seconds,
    )
    Await.result(registry.sweepExpired(Milliseconds.currentTime()), 5.seconds)
    assert(
      Await
        .result(registry.get(orphanStartedId), 5.seconds)
        .map(_.lastAction)
        .contains(ExecutionAction.Interrupted()),
      "a departed host's live-looking Started record is finalized to Interrupted regardless of expiry",
    )
    Await.result(
      graph.namespacePersistor.setMetaData(BackgroundQueryStatusRegistry.KeyPrefix + orphanStartedId, None),
      5.seconds,
    )
  }

  test("the manager sweep deletes an undecodable status record (not immortal)") {
    val badKey = BackgroundQueryStatusRegistry.KeyPrefix + "undecodable-" + UUID.randomUUID()
    Await.result(graph.namespacePersistor.setMetaData(badKey, Some("not a record".getBytes(UTF_8))), 5.seconds)
    assert(Await.result(graph.namespacePersistor.getMetaData(badKey), 5.seconds).nonEmpty, "seeded the garbage blob")
    // The record-driven sweeps can never see it (it has no decodable hostId/expiry), so only the raw
    // manager sweep can collect it.
    Await.result(registry.sweepExpired(Milliseconds.currentTime()), 5.seconds)
    assert(
      Await.result(graph.namespacePersistor.getMetaData(badKey), 5.seconds).isEmpty,
      "an undecodable record under the prefix is physically deleted by the manager sweep",
    )
  }

  test("a run that outlives its statusExpiry stays visible and still records its terminal status") {
    // The Started record's stamp (start + 1ms) is long past by the time this run terminates, but
    // Started records are always visible and the owner sweep never touches an in-flight run — so the
    // record is still there when the run finishes, and the terminal outcome is recorded normally.
    val executionId = UUID.randomUUID()
    Await.result(
      runDone(
        executionId,
        jobName = None,
        BackgroundQuery("UNWIND range(1, 200000) AS i RETURN i", drop, statusExpiryMillis = 1L),
      ),
      15.seconds,
    )
    val stored = Await
      .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + executionId), 5.seconds)
      .map(bytes => io.circe.parser.decode[BackgroundQueryRecord](new String(bytes, UTF_8)))
    stored match {
      case Some(Right(rec)) =>
        assert(
          rec.lastAction.isInstanceOf[ExecutionAction.Completed],
          s"terminal outcome recorded; got ${rec.lastAction}",
        )
      case other => fail(s"expected the Completed record, got $other")
    }
  }

  /** Collect a topic's tap frames up to and including the completion frame. */
  private def tapFramesUntilCompletion(executionId: UUID): Seq[String] = {
    val topic = TapBus.topicForBackgroundQuery(defaultNamespaceId, executionId)
    Await.result(
      tapBus
        .subscriberSource(topic)(graph.materializer)
        .collect { case WebSocketFrame.Text(payload, _, _) => payload }
        .takeWhile(!_.contains(BackgroundQueryTapRelay.CompletionFrameKey), inclusive = true)
        .runWith(Sink.seq)(graph.materializer),
      15.seconds,
    )
  }

  /** Decode the completion frame's inner object from the last collected frame. */
  private def completionOf(frames: Seq[String]): io.circe.ACursor =
    io.circe.parser
      .parse(frames.lastOption.getOrElse(fail("no frames collected")))
      .getOrElse(fail(s"unparseable completion frame: ${frames.last}"))
      .hcursor
      .downField(BackgroundQueryTapRelay.CompletionFrameKey)

  test("tap: rows buffered before any subscriber are delivered on connect, then a completion frame") {
    val executionId = UUID.randomUUID()
    Await.result(
      runDone(executionId, jobName = None, BackgroundQuery("UNWIND range(1, 5) AS i RETURN i", drop)),
      15.seconds,
    )
    // Connect only after the run finished: the buffered head of the results flushes, then the sentinel.
    val frames = tapFramesUntilCompletion(executionId)
    assert(frames.size == 6, s"5 buffered rows + 1 completion frame; got $frames")
    val completion = completionOf(frames)
    assert(completion.get[String]("status").contains("completed"))
    assert(completion.get[Long]("totalRowCount").contains(5L))
    assert(completion.get[Long]("droppedBufferedRows").contains(0L))
  }

  test("tap: the buffer keeps the first MaxBufferedRows rows and reports the overflow in the completion frame") {
    val overflow = 6
    val total = BackgroundQueryTapRelay.MaxBufferedRows + overflow
    val executionId = UUID.randomUUID()
    Await.result(
      runDone(executionId, jobName = None, BackgroundQuery(s"UNWIND range(1, $total) AS i RETURN i", drop)),
      15.seconds,
    )
    // Row-frame *delivery* is best-effort (the bus drops on slow consumers); the completion frame's
    // counts are the contract — they are relay-side facts, independent of bus behavior.
    val completion = completionOf(tapFramesUntilCompletion(executionId))
    assert(completion.get[String]("status").contains("completed"))
    assert(completion.get[Long]("totalRowCount").contains(total.toLong))
    assert(completion.get[Long]("droppedBufferedRows").contains(overflow.toLong))
  }

  test("tap: a run cancelled before any subscriber connects discards its buffer silently") {
    val executionId = UUID.randomUUID()
    val runFut = runDone(
      executionId,
      jobName = None,
      BackgroundQuery("UNWIND range(1, 50000000) AS i RETURN i", drop),
    )
    assert(runner.cancel(executionId), "cancel the in-flight run")
    Await.result(runFut, 15.seconds)
    val topic = TapBus.topicForBackgroundQuery(defaultNamespaceId, executionId)
    val frames = Await.result(
      tapBus
        .subscriberSource(topic)(graph.materializer)
        .takeWithin(2.seconds)
        .runWith(Sink.seq)(graph.materializer),
      10.seconds,
    )
    assert(frames.isEmpty, s"a cancelled, never-observed run publishes nothing; got $frames")
  }

  test("a record removed out from under a live run is not resurrected by its terminal write (gate)") {
    // Simulates the one case the gate exists for: the manager sweeping a partitioned host's records
    // while a run is still in flight there. The late finish must stay silent, not resurrect.
    val executionId = UUID.randomUUID()
    val runFut = runDone(
      executionId,
      jobName = None,
      BackgroundQuery("UNWIND range(1, 50000000) AS i RETURN i", drop),
    )
    eventually(assert(Await.result(registry.get(executionId), 5.seconds).isDefined, "the Started record lands"))
    Await.result(
      graph.namespacePersistor.setMetaData(BackgroundQueryStatusRegistry.KeyPrefix + executionId, None),
      5.seconds,
    )
    assert(runner.cancel(executionId), "finish the run promptly via cancel")
    Await.result(runFut, 15.seconds)
    assert(
      Await
        .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + executionId), 5.seconds)
        .isEmpty,
      "the terminal write is gated: a removed record is not resurrected",
    )
  }

  test("cancel stops an in-flight run and records the terminal Cancelled state") {
    val executionId = UUID.randomUUID()
    // A long-running query so it is still in-flight when we cancel it.
    val runFut = runDone(
      executionId,
      jobName = None,
      BackgroundQuery("UNWIND range(1, 50000000) AS i RETURN i", drop),
    )
    // The kill switch is registered synchronously in `run`, so cancel finds it immediately.
    assert(runner.cancel(executionId), "cancel should find the in-flight execution on this host")
    Await.result(runFut, 15.seconds) // the cancelled run completes

    // The cancelled run reaches the terminal Cancelled state — not Completed, not Failed.
    val rec = Await.result(registry.get(executionId), 5.seconds)
    assert(
      rec.exists(_.lastAction == ExecutionAction.Cancelled()),
      s"a cancelled run records Cancelled; got ${rec.map(_.lastAction)}",
    )
    assert(!runner.cancel(executionId), "a second cancel is a no-op (nothing in-flight)")
  }

  test("a cancel issued immediately after run (before materialization) still records Cancelled") {
    // The kill switch is registered before the stream materializes (a SharedKillSwitch honors a
    // pre-materialization abort), so even a cancel that beats materialization takes effect and the
    // run terminates as Cancelled, never Completed.
    val executionId = UUID.randomUUID()
    val runFut = runDone(
      executionId,
      jobName = None,
      BackgroundQuery("UNWIND range(1, 50000000) AS i RETURN i", drop),
    )
    assert(runner.cancel(executionId), "cancel finds the execution immediately, before the stream materializes")
    Await.result(runFut, 15.seconds)
    val rec = Await.result(registry.get(executionId), 5.seconds)
    assert(
      rec.exists(_.lastAction == ExecutionAction.Cancelled()),
      s"a cancelled run records Cancelled, not Completed; got ${rec.map(_.lastAction)}",
    )
  }

  test("an expired terminal record is invisible to reads and physically deleted by the owner sweep") {
    // Expiry so short the terminal record is already expired by the time we look.
    val executionId = UUID.randomUUID()
    Await.result(
      runDone(executionId, jobName = None, BackgroundQuery("RETURN 1", drop, statusExpiryMillis = 1L)),
      15.seconds,
    )
    eventually(
      assert(Await.result(registry.get(executionId), 5.seconds).isEmpty, "expired terminal record hidden from get"),
    )
    Await.result(registry.sweepOwnedBy("test-host", inFlight = _ => false), 5.seconds)
    assert(
      Await
        .result(graph.namespacePersistor.getMetaData(BackgroundQueryStatusRegistry.KeyPrefix + executionId), 5.seconds)
        .isEmpty,
      "the owner sweep physically deletes the expired record",
    )
  }
}
