package com.thatdot.quine.app.routes

import scala.jdk.CollectionConverters._

import com.codahale.metrics.MetricRegistry
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.metrics.HostQuineMetrics.IngestMetricComponent

/** A cluster-ingest partition worker meters under the same metric component as a member-local
  * ingest, so the `/metrics` RBAC redaction -- which gates the `ingest` component on `IngestRead`
  * (`V2EnterpriseAdministrationEndpoints.filterMetricsReport`) -- covers it. A separate component
  * escapes that filter and leaks cluster-ingest names and counts to a user without `IngestRead`,
  * which is the regression these guard.
  */
class IngestMeterTest extends AnyFunSuite {

  private def freshMetrics: HostQuineMetrics =
    HostQuineMetrics(enableDebugMetrics = false, new MetricRegistry(), omitDefaultNamespace = false)

  test("a cluster-partition meter registers under the ingest metric component, not a separate one") {
    val metrics = freshMetrics
    val _ = IngestMetered.clusterPartitionMeter(defaultNamespaceId, "orders#2", metrics)
    val names = metrics.metricRegistry.getNames.asScala.toSet
    val expected = Set(
      metrics.metricName(defaultNamespaceId, List(IngestMetricComponent, "orders#2", "count")),
      metrics.metricName(defaultNamespaceId, List(IngestMetricComponent, "orders#2", "bytes")),
    )
    assert(expected.subsetOf(names), s"cluster-partition meter must register under the ingest component; got $names")
    // Not under a separate component the `ingest`-gated redaction would miss. (Scoped to the meter's
    // own names: the global cluster-ingest dispatch counters are a separate surface, tracked apart.)
    val underOwnComponent = Set(
      metrics.metricName(defaultNamespaceId, List("cluster-ingest", "orders#2", "count")),
      metrics.metricName(defaultNamespaceId, List("cluster-ingest", "orders#2", "bytes")),
    )
    assert(
      underOwnComponent.intersect(names).isEmpty,
      "cluster-partition meter must not register under a separate 'cluster-ingest' component that /metrics redaction misses",
    )
  }

  test("a cluster-partition meter is named identically to a member-local ingest of the same name") {
    val cluster = freshMetrics
    val member = freshMetrics
    val _ = IngestMetered.clusterPartitionMeter(defaultNamespaceId, "orders#2", cluster)
    val _ = IngestMetered.ingestMeter(defaultNamespaceId, "orders#2", member)
    assertResult(member.metricRegistry.getNames)(cluster.metricRegistry.getNames)
  }

  test("removing a cluster-partition meter removes the worker's timers too") {
    val metrics = freshMetrics
    val meter = IngestMetered.clusterPartitionMeter(defaultNamespaceId, "orders#2", metrics)
    // The timers a running worker registers: deserialization via its framed source, query via `DecodedSource.stream`
    val _ = meter.unmanagedDeserializationTimer
    val _ = metrics.ingestQueryTimer(defaultNamespaceId, "orders#2")
    def workerMetricNames = metrics.metricRegistry.getNames.asScala.filter(_.contains("ingest.orders#2.")).toSet
    assert(workerMetricNames.size == 4, s"expected count, bytes, deserialization and query; got $workerMetricNames")

    assert(IngestMetered.removeClusterPartitionMeter(defaultNamespaceId, "orders#2", metrics))
    assert(workerMetricNames.isEmpty, s"metrics left behind: $workerMetricNames")
  }
}
