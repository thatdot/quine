package com.thatdot.quine.graph.metrics

import java.util.UUID

import scala.jdk.CollectionConverters._

import com.codahale.metrics.MetricRegistry
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.metrics.StandingQueryHostMetrics.StandingQueryMetrics
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, defaultNamespaceId}

/** The two halves of a standing query's metric lifecycle: `registerStandingQueryMetrics` publishes them
  * once the query has started, and `removeStandingQueryMetrics` retires them when the query is cancelled.
  *
  * The ingest equivalent is [[HostQuineMetricsTest]]. Standing queries differ in that they name most of
  * their metrics by the query's name but the persisted-state histogram by the query's id, so removal has
  * to be given both.
  */
class HostQuineMetricsStandingQueryTest extends AnyFunSuite {

  private def newMetrics(omitDefaultNamespace: Boolean = false): HostQuineMetrics =
    HostQuineMetrics(enableDebugMetrics = false, new MetricRegistry, omitDefaultNamespace)

  private val ns: NamespaceId = NamespaceId("testns")
  private val sqName = "watcher"
  private val sqId: StandingQueryId = StandingQueryId(UUID.fromString("0-0-0-0-1"))

  /** Every registered metric name belonging to this standing query, whatever the namespace prefix. The
    * two suffixes are the two ways a standing query's metrics are named.
    */
  private def sqMetricNames(metrics: HostQuineMetrics): Set[String] =
    metrics.metricRegistry.getMetrics.asScala.keys
      .filter(_.contains(s"${HostQuineMetrics.StandingQueryMetricComponent}."))
      .filter(n => n.endsWith(s".$sqName") || n.endsWith(s".${sqId.uuid}"))
      .toSet

  /** Create and register a query's metrics, the way a query that starts successfully does. */
  private def register(
    metrics: HostQuineMetrics,
    namespace: NamespaceId = ns,
    name: String = sqName,
  ): StandingQueryMetrics = {
    val sqMetrics = metrics.unregisteredStandingQueryMetrics()
    metrics.registerStandingQueryMetrics(namespace, name, sqMetrics)
    sqMetrics
  }

  test("registration publishes one metric per component that removal knows about") {
    val metrics = newMetrics()
    val _ = register(metrics)

    assert(
      sqMetricNames(metrics).size == 5,
      "two metrics registered the same name, so one is shadowing another",
    )
  }

  test("unregistered metrics are invisible to the registry until registered") {
    val metrics = newMetrics()
    val sqMetrics = metrics.unregisteredStandingQueryMetrics()
    sqMetrics.resultMeter.mark(3)
    assert(sqMetricNames(metrics).isEmpty)

    metrics.registerStandingQueryMetrics(ns, sqName, sqMetrics)
    assert(sqMetricNames(metrics).nonEmpty)
    assert(sqMetrics.resultMeter.getCount == 3)
  }

  test("removeStandingQueryMetrics retires every metric a standing query registers") {
    val metrics = newMetrics()
    val _ = register(metrics)
    // ...and the one a node registers later, as it persists this query's state.
    metrics.standingQueryStateSize(ns, sqId)

    assert(sqMetricNames(metrics).size == 5 + 1)

    assert(metrics.removeStandingQueryMetrics(ns, sqName, sqId))
    assert(sqMetricNames(metrics).isEmpty)
  }

  // The states histogram is registered by nodes, independently of whether the query registered anything
  // else, so removing it in isolation proves its removal is not conditional on the rest being there.
  test("removal retires the states histogram when it is the only metric registered") {
    val metrics = newMetrics()
    metrics.standingQueryStateSize(ns, sqId)

    assert(metrics.removeStandingQueryMetrics(ns, sqName, sqId))
    assert(sqMetricNames(metrics).isEmpty)
  }

  test("removeStandingQueryMetrics reports whether anything was removed") {
    val metrics = newMetrics()
    assert(!metrics.removeStandingQueryMetrics(ns, "never-registered", sqId))

    val _ = register(metrics)
    assert(metrics.removeStandingQueryMetrics(ns, sqName, sqId))
    assert(!metrics.removeStandingQueryMetrics(ns, sqName, sqId), "a second removal has nothing left to remove")
  }

  test("a recreated standing query does not inherit the cancelled query's totals") {
    val metrics = newMetrics()
    val first = register(metrics)
    first.resultMeter.mark(7)
    first.droppedCounter.inc(2)
    first.resultHashCode.add(42)

    metrics.removeStandingQueryMetrics(ns, sqName, sqId)
    val second = register(metrics)

    assert(second.resultMeter.getCount == 0)
    assert(second.droppedCounter.getCount == 0)
    assert(second.resultHashCode.sum == 0)

    // The cancelled query's own final stats are unaffected: RunningStandingQuery holds the metric objects
    // directly, and the cancellation response reads its rates from those rather than from the registry.
    assert(first.resultMeter.getCount == 7)
    assert(first.droppedCounter.getCount == 2)
  }

  test("removal is scoped to one namespace") {
    val metrics = newMetrics()
    val other = NamespaceId("otherns")
    val _ = register(metrics)
    val otherRegistered = register(metrics, namespace = other)
    otherRegistered.resultMeter.mark(5)

    metrics.removeStandingQueryMetrics(ns, sqName, sqId)

    assert(sqMetricNames(metrics).size == 5)
    assert(sqMetricNames(metrics).forall(_.startsWith(other.name)))
    assert(otherRegistered.resultMeter.getCount == 5)
  }

  test("removal is scoped to one query") {
    val metrics = newMetrics()
    val _ = register(metrics)
    val bystander = register(metrics, name = "other-watcher")
    bystander.resultMeter.mark(5)

    metrics.removeStandingQueryMetrics(ns, sqName, sqId)

    assert(bystander.resultMeter.getCount == 5)
    assert(metrics.metricRegistry.getMetrics.asScala.keys.exists(_.endsWith(".other-watcher")))
  }

  test("removal matches creation in the default namespace, whose prefix an enterprise instance omits") {
    for (omitDefaultNamespace <- Seq(true, false)) {
      val metrics = newMetrics(omitDefaultNamespace)
      val _ = register(metrics, namespace = defaultNamespaceId)
      metrics.standingQueryStateSize(defaultNamespaceId, sqId)

      assert(
        metrics.removeStandingQueryMetrics(defaultNamespaceId, sqName, sqId),
        s"removal did not match creation with omitDefaultNamespace = $omitDefaultNamespace",
      )
      assert(sqMetricNames(metrics).isEmpty)
    }
  }
}
