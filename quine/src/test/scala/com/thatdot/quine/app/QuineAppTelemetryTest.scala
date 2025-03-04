package com.thatdot.quine.app

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, Future, Promise}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ExplicitlyTriggeredScheduler, TestKit}

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.Assertion
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.{FakeQuineGraph, GraphService}

class QuineAppTelemetryTest
    extends TestKit(
      ActorSystem(
        "telemetry-tests",
        ConfigFactory
          .load()
          .withValue(
            "pekko.scheduler.implementation",
            ConfigValueFactory.fromAnyRef("org.apache.pekko.testkit.ExplicitlyTriggeredScheduler"),
          ),
      ),
    )
    with AnyFunSuiteLike
    with should.Matchers {
  import scala.concurrent.duration.DurationInt

  implicit private class CheckPointRefSafe(cp: Checkpoint) {

    /** Wraps the checkpoint `apply` such that the compiler does not
      * issue a "discarded non-Unit value" warning on an assertion.
      */
    def add(assertion: => Assertion): Unit = cp(assertion: Unit)
  }

  implicit private val lc: LogConfig = LogConfig.permissive

  private val scheduler = system.getScheduler.asInstanceOf[ExplicitlyTriggeredScheduler]

  abstract private class FakeImproveQuine(
    persistor: String = "default-irrelevant",
    getSources: () => Future[Option[List[String]]] = () => Future.successful(None),
    getSinks: () => Future[Option[List[String]]] = () => Future.successful(None),
    recipe: Option[Recipe] = None,
    recipeCanonicalName: Option[String] = None,
    apiKey: Option[String] = None,
  ) extends ImproveQuine(
        service = "QuineTests",
        version = "test",
        persistorSlug = persistor,
        getSources = getSources,
        getSinks = getSinks,
        recipe = recipe,
        recipeCanonicalName = recipeCanonicalName,
        apiKey = apiKey,
      ) {
    val assertionPromise: Promise[Checkpoint]
  }

  private def buildGraphServiceFrom(graph: FakeQuineGraph): GraphService =
    Await.result(
      GraphService(
        persistorMaker = _ => graph.namespacePersistor,
        idProvider = graph.idProvider,
        shardCount = graph.shards.size,
        effectOrder = graph.effectOrder,
        declineSleepWhenWriteWithinMillis = graph.declineSleepWhenWriteWithinMillis,
        declineSleepWhenAccessWithinMillis = graph.declineSleepWhenAccessWithinMillis,
        maxCatchUpSleepMillis = graph.maxCatchUpSleepMillis,
        labelsProperty = graph.labelsProperty,
        edgeCollectionFactory = graph.edgeCollectionFactory,
        metricRegistry = graph.metrics.metricRegistry,
      ),
      10.seconds,
    )

  test("sends telemetry event `instance.startup` upon startup") {
    val graph = new FakeQuineGraph(system)
    val graphService = buildGraphServiceFrom(graph)
    val app = new QuineApp(
      graph = graphService,
      helpMakeQuineBetter = true,
    )
    val expectedSources = Some(List("test-source"))
    val testGetSources = () => Future.successful(expectedSources)
    val expectedSinks = Some(List("test-sink"))
    val testGetSinks = () => Future.successful(expectedSinks)
    val fakeIq: FakeImproveQuine = new FakeImproveQuine(
      getSources = testGetSources,
      getSinks = testGetSinks,
    ) {
      val assertionPromise: Promise[Checkpoint] = Promise()
      override protected val sessionId: UUID = UUID.randomUUID()
      override protected val startTime: Instant = Instant.now()
      override protected def send(
        event: ImproveQuine.Event,
        sources: Option[List[String]],
        sinks: Option[List[String]],
        sessionStartedAt: Instant,
        sessionIdentifier: UUID,
      )(implicit system: ActorSystem, logConfig: LogConfig): Future[Unit] = {
        val cp = new Checkpoint()
        cp.add(event.slug shouldBe "instance.started")
        cp.add(sources shouldBe expectedSources)
        cp.add(sinks shouldBe expectedSinks)
        cp.add(sessionIdentifier shouldBe sessionId)
        cp.add(sessionStartedAt shouldBe startTime)
        assertionPromise.success(cp)
        Future.unit
      }
    }
    app.notifyWebServerStarted(Some(fakeIq))
    val assertionOutcome = Await.result(fakeIq.assertionPromise.future, 2.seconds)
    assertionOutcome.reportAll()
  }

  test("sends `instance.heartbeat` telemetry events on runup intervals") {
    val graph = new FakeQuineGraph(system)
    val graphService = buildGraphServiceFrom(graph)
    val app = new QuineApp(
      graph = graphService,
      helpMakeQuineBetter = true,
    )
    val expectedSources = Some(List("test-source-1", "test-source-2"))
    val testGetSources = () => Future.successful(expectedSources)
    val expectedSinks = Some(List("test-sink-1", "test-sink-2"))
    val testGetSinks = () => Future.successful(expectedSinks)

    val fakeIq: FakeImproveQuine = new FakeImproveQuine(
      getSources = testGetSources,
      getSinks = testGetSinks,
    ) {
      private val assertionsCheckpoint = new Checkpoint()
      private val sendCallCount = new AtomicInteger()
      private val expectedSendInvocations = ImproveQuine.runUpIntervals.size + 1
      val assertionPromise: Promise[Checkpoint] = Promise()
      override protected val sessionId: UUID = UUID.randomUUID()
      override protected val startTime: Instant = Instant.now()
      override protected def send(
        event: ImproveQuine.Event,
        sources: Option[List[String]],
        sinks: Option[List[String]],
        sessionStartedAt: Instant,
        sessionIdentifier: UUID,
      )(implicit system: ActorSystem, logConfig: LogConfig): Future[Unit] = {
        if (sendCallCount.incrementAndGet() == 1) {
          // skip the startup invocation
        } else {
          assertionsCheckpoint.add(event.slug shouldBe "instance.heartbeat")
          assertionsCheckpoint.add(sources shouldBe expectedSources)
          assertionsCheckpoint.add(sinks shouldBe expectedSinks)
          assertionsCheckpoint.add(sessionIdentifier shouldBe sessionId)
          assertionsCheckpoint.add(sessionStartedAt shouldBe startTime)
          if (sendCallCount.get() == expectedSendInvocations) {
            assertionPromise.success(assertionsCheckpoint)
          }
          if (sendCallCount.get() > expectedSendInvocations) {
            // Ideally, this test would also capture if there are too many
            // calls to the `send` method. However, this approach is only
            // slightly likely to work, as the assertion promise will have
            // already completed; this `fail` races the `reportAll()` call.
            // However, I think this is an unlikely enough mistake—and a
            // sufficiently inconsequential one—that putting this attempt
            // here is a reasonable compromise against complicating this
            // test more.
            fail("Too many calls to `send`.")
          }
        }
        Future.unit
      }
    }

    app.notifyWebServerStarted(Some(fakeIq))
    scheduler.timePasses(ImproveQuine.runUpIntervals.last)
    val assertionOutcome = Await.result(fakeIq.assertionPromise.future, 5.seconds)
    assertionOutcome.reportAll()
  }
}
