package com.thatdot.quine.app

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Milliseconds, Seconds, Span}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{FileAccessPolicy, ResolutionMode}
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{QuineIngestConfiguration, Transformation}
import com.thatdot.quine.app.model.ingest2.{NumberIteratorIngest, StreamingFormat}
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{GraphService, NamespaceId, defaultNamespaceId}
import com.thatdot.quine.util.GaugeKey

/** A deleted ingest keeps writing to the graph until its stream drains, but its entry leaves the
  * ingest map immediately, so a same-named ingest can start inside that gap. These tests pin that
  * the new ingest's metrics and pressure gauges survive its predecessor finishing its drain.
  *
  * Also covers the metric lifecycle around a create that never produces an ingest.
  */
class IngestNameReuseTest extends AnyFunSuite with BeforeAndAfterAll with ScalaFutures with Eventually {

  implicit val logConfig: LogConfig = LogConfig.permissive
  implicit val timeout: Timeout = Timeout(10.seconds)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Milliseconds))

  private val graph: GraphService = IngestTestGraph.makeGraph("ingest-name-reuse-test")

  private val quineApp = new QuineApp(
    graph,
    helpMakeQuineBetter = false,
    FileAccessPolicy(List.empty, ResolutionMode.Dynamic),
    new LocalTapBus,
  )

  private val namespace: NamespaceId = defaultNamespaceId

  implicit val ec: ExecutionContext = graph.nodeDispatcherEC

  override def beforeAll(): Unit =
    while (!graph.isReady) Thread.sleep(10)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  private def config(name: String, query: String = "RETURN $that"): QuineIngestConfiguration =
    QuineIngestConfiguration(
      name = name,
      source = NumberIteratorIngest(StreamingFormat.RawFormat, startOffset = 0L, limit = None),
      query = query,
      maxPerSecond = Some(1),
      parallelism = 1,
    )

  /** Metric names registered for this ingest, whatever the namespace prefix. */
  private def ingestMetricNames(name: String): Set[String] =
    graph.metrics.metricRegistry.getMetrics.asScala.keys.filter(_.contains(s"ingest.$name.")).toSet

  /** Pressure gauges registered for this ingest. */
  private def ingestGaugeKeys(name: String): Set[GaugeKey] =
    graph.pressureGaugeRegistry.snapshot().keySet.filter(k => k.pipelineType == "ingest" && k.streamName == name)

  test("a same-named ingest created while its predecessor drains keeps its metrics and gauges") {
    val name = "recreated-during-drain"
    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isValid)

    // The v1 removal hands the still-running stream back for the caller to close, rather than
    // closing it itself. That makes the drain window as wide as this test wants it, instead of
    // something to race against.
    val removed = quineApp.removeIngestStream(name, namespace)
    assert(removed.isDefined)

    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isValid)
    val newIngestsMetrics = ingestMetricNames(name)
    val newIngestsGauges = ingestGaugeKeys(name)
    assert(newIngestsMetrics.nonEmpty)
    assert(newIngestsGauges.nonEmpty)

    removed.get.close()
    Await.result(removed.get.terminated().flatten, 10.seconds)

    assert(ingestMetricNames(name) == newIngestsMetrics, "the drained predecessor removed the new ingest's metrics")
    assert(ingestGaugeKeys(name) == newIngestsGauges, "the drained predecessor removed the new ingest's gauges")
    val _ = quineApp.removeV2IngestStream(name, namespace, None).futureValue
  }

  test("deleting an ingest and creating it again in sequence works") {
    val name = "recreated-after-delete"
    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isValid)

    val _ = quineApp.removeV2IngestStream(name, namespace, None).futureValue

    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isValid)
    val _ = quineApp.removeV2IngestStream(name, namespace, None).futureValue
  }

  test("a create rejected as a duplicate leaves the running ingest's metrics alone") {
    val name = "duplicate-create"
    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isValid)
    val whileRunning = ingestMetricNames(name)
    assert(whileRunning.nonEmpty)

    assert(quineApp.createV2IngestStream(name, config(name), namespace, timeout).isInvalid)
    assert(ingestMetricNames(name) == whileRunning, "the rejected duplicate disturbed the live ingest's metrics")

    val _ = quineApp.removeV2IngestStream(name, namespace, None).futureValue
  }

  test("a create rejected as invalid leaves no metrics behind") {
    val name = "invalid-transformation"
    assert(ingestMetricNames(name).isEmpty)

    // An uncompilable transformation fails as a Validated `invalid`, after the meters have already
    // been minted for the ingest.
    val settings = config(name).copy(transformation = Some(Transformation.JavaScript("function ( { (")))
    assert(quineApp.createV2IngestStream(name, settings, namespace, timeout).isInvalid)

    assert(
      ingestMetricNames(name).isEmpty,
      "metrics were left registered for an ingest that never started",
    )
  }

  test("a create that throws leaves no metrics behind") {
    val name = "invalid-query"
    assert(ingestMetricNames(name).isEmpty)

    // Building the ingest query parses the user's Cypher, which throws rather than returning an
    // `invalid` -- a separate path out of the create, and one the meters must survive no better.
    assertThrows[CypherException] {
      quineApp.createV2IngestStream(name, config(name, query = "this is not cypher"), namespace, timeout)
    }

    assert(
      ingestMetricNames(name).isEmpty,
      "metrics were left registered for an ingest whose creation threw",
    )
  }
}
