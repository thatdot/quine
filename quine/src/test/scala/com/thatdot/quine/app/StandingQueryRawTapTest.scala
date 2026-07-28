package com.thatdot.quine.app

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import sttp.ws.WebSocketFrame

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{FileAccessPolicy, ResolutionMode}
import com.thatdot.quine.app.model.outputs2.query.standing.{LocalTapBus, SqTapStage, TapBus}
import com.thatdot.quine.graph.{GraphService, NamespaceId, StandingQueryResult, defaultNamespaceId}
import com.thatdot.quine.routes.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.routes.{StandingQueryPattern => SqPattern, _}

/** Regression tests for the raw standing query tap, which is published from the once-per-result
  * observer upstream of each SQ's broadcast hub (see `TapBus.rawResultObserver`):
  *
  *   - an SQ with no outputs still publishes raw results (only the fallback drain consumes the hub)
  *   - an SQ with several outputs publishes exactly one raw frame per result, not one per output
  *   - an observer that throws does not fail the results stream or cancel the standing query
  */
class StandingQueryRawTapTest extends AnyFunSuite with Matchers {
  val namespace: NamespaceId = defaultNamespaceId

  private val ingestCount = 100
  private val mod = 5 // must divide `ingestCount` equally
  private val expectedResults = ingestCount / mod

  private val ingestConfig = NumberIteratorIngest(
    FileIngestFormat.CypherLine(
      """WITH gen.node.from(toInteger($that)) AS n,
        |     toInteger($that) AS i
        |MATCH (thisNode), (nextNode)
        |WHERE id(thisNode) = id(n)
        |  AND id(nextNode) = idFrom(i + 1)
        |SET thisNode.id = i
        |SET nextNode.id = i + 1
        |CREATE (thisNode)-[:next]->(nextNode)
        |""".stripMargin,
    ),
    ingestLimit = Some(ingestCount.toLong),
    maximumPerSecond = None,
  )

  private val sqPattern = SqPattern.Cypher(
    s"""MATCH (a)-[:next]->(b)
       |WHERE a.id IS NOT NULL
       |  AND b.id IS NOT NULL
       |  AND a.id % $mod = 0
       |RETURN a.id, b.id
       |""".stripMargin,
    StandingQueryMode.MultipleValues,
  )

  private def makeApp(): (GraphService, QuineApp, LocalTapBus) = {
    val graph = IngestTestGraph.makeGraph()
    while (!graph.isReady) Thread.sleep(10)
    val tapBus = new LocalTapBus
    val quineApp =
      new QuineApp(graph, false, FileAccessPolicy(List.empty, ResolutionMode.Dynamic), tapBus)(LogConfig.permissive)
    (graph, quineApp, tapBus)
  }

  /** Subscribe to the raw tap topic for `sqName`, collecting each frame's text payload. */
  private def subscribeRaw(
    tapBus: LocalTapBus,
    graph: GraphService,
    sqName: String,
  ): (ConcurrentLinkedQueue[String], UniqueKillSwitch) = {
    val topic = TapBus.topicForSq(namespace, sqName, "_raw_", SqTapStage.Raw)
    val frames = new ConcurrentLinkedQueue[String]()
    val killSwitch = tapBus
      .subscriberSource(topic)(graph.materializer)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.foreach {
        case WebSocketFrame.Text(payload, _, _) => frames.add(payload); ()
        case _ => ()
      })
      .run()(graph.materializer)
    (frames, killSwitch)
  }

  private def startIngest(quineApp: QuineApp)(implicit timeout: Timeout): Unit = {
    val started = Await.result(
      Future.fromTry(
        quineApp
          .addIngestStream("numbers", ingestConfig, namespace, None, shouldResumeRestoredIngests = false, timeout),
      ),
      3.seconds,
    )
    if (!started) fail("ingest stream was not added")
  }

  test("raw tap publishes results for a standing query with no outputs") {
    val (graph, quineApp, tapBus) = makeApp()
    implicit val timeout: Timeout = Timeout(2.seconds)

    val sqName = "raw-tap-no-outputs"
    Await.result(quineApp.addStandingQuery(sqName, namespace, StandingQueryDefinition(sqPattern, Map.empty)), 3.seconds)
    val (frames, killSwitch) = subscribeRaw(tapBus, graph, sqName)
    startIngest(quineApp)

    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
      assert(frames.size == expectedResults)
    }
    killSwitch.shutdown()
  }

  test("raw tap publishes exactly one frame per result regardless of output count") {
    val (graph, quineApp, tapBus) = makeApp()
    implicit val timeout: Timeout = Timeout(2.seconds)

    val out1 = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val out2 = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val sqDef = StandingQueryDefinition(
      sqPattern,
      Map(
        "queue-1" -> StandingQueryResultOutputUserDef.InternalQueue(out1),
        "queue-2" -> StandingQueryResultOutputUserDef.InternalQueue(out2),
      ),
    )

    val sqName = "raw-tap-two-outputs"
    Await.result(quineApp.addStandingQuery(sqName, namespace, sqDef), 3.seconds)
    val (frames, killSwitch) = subscribeRaw(tapBus, graph, sqName)
    startIngest(quineApp)

    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
      assert(out1.get().length == expectedResults)
      assert(out2.get().length == expectedResults)
      // One frame per result: publishing from per-output workflows would have doubled this
      assert(frames.size == expectedResults)
    }
    killSwitch.shutdown()
  }

  test("a raw result observer that throws does not cancel the standing query") {
    val (graph, quineApp, _) = makeApp()
    implicit val timeout: Timeout = Timeout(2.seconds)

    // Replace the app-registered observer with one that always fails
    graph.setStandingQueryRawResultObserver((_, _, _) => throw new RuntimeException("boom"))

    val sqResultsRef = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val sqDef = StandingQueryDefinition(
      sqPattern,
      Map("internal-queue" -> StandingQueryResultOutputUserDef.InternalQueue(sqResultsRef)),
    )

    val sqName = "raw-tap-throwing-observer"
    Await.result(quineApp.addStandingQuery(sqName, namespace, sqDef), 3.seconds)
    startIngest(quineApp)

    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
      assert(sqResultsRef.get().length == expectedResults)
    }
    // The SQ survived every observer failure: still registered and running
    assert(graph.standingQueries(namespace).map(_.runningStandingQueries.size) == Some(1))
  }
}
