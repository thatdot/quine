package com.thatdot.quine.graph.metrics

import scala.jdk.CollectionConverters._

import com.codahale.metrics.MetricRegistry
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.{NamespaceId, defaultNamespaceId}

/** Covers the lifecycle of the per-ingest metrics: that every one an ingest registers is retired when the
  * ingest is deleted, and that a same-named ingest created afterwards starts from zero rather than
  * inheriting its predecessor's totals.
  */
class HostQuineMetricsTest extends AnyFunSuite {

  private def newMetrics(omitDefaultNamespace: Boolean = false): HostQuineMetrics =
    HostQuineMetrics(enableDebugMetrics = false, new MetricRegistry, omitDefaultNamespace)

  /** Every registered metric name carrying the `ingest.<name>.` infix, whatever the namespace prefix. */
  private def ingestMetricNames(metrics: HostQuineMetrics, ingestName: String): Set[String] =
    metrics.metricRegistry.getMetrics.asScala.keys
      .filter(_.contains(s"${HostQuineMetrics.IngestMetricComponent}.$ingestName."))
      .toSet

  private val ns: NamespaceId = NamespaceId("testns")

  /** Each metric an ingest registers, paired with the accessor that registers it. The tests below drive
    * themselves from this list, registering one metric at a time to pin removal per metric.
    */
  private val registerMetric: List[(String, HostQuineMetrics => Any)] = List(
    IngestHostMetrics.IngestCountComponent -> (m => m.ingestCountMeter(ns, "numbers")),
    IngestHostMetrics.IngestBytesComponent -> (m => m.ingestBytesMeter(ns, "numbers")),
    IngestHostMetrics.IngestQueryComponent -> (m => m.ingestQueryTimer(ns, "numbers")),
    IngestHostMetrics.IngestDeserializationComponent -> (m => m.ingestDeserializationTimer(ns, "numbers")),
  )

  test("removeIngestMetrics retires every metric an ingest registers") {
    val metrics = newMetrics()
    // Touch every accessor, which is what an ingest does as it starts up.
    registerMetric.foreach { case (_, register) => register(metrics) }

    assert(
      ingestMetricNames(metrics, "numbers").size == registerMetric.size,
      "two accessors registered the same name, so one metric is shadowing another",
    )

    assert(metrics.removeIngestMetrics(ns, "numbers"))
    assert(ingestMetricNames(metrics, "numbers").isEmpty)
  }

  // Not every ingest registers every metric -- only sources that time decoding register a deserialization
  // timer, for instance. Removing one metric in isolation proves no metric's removal is conditional on
  // an earlier one having removed something, whatever order removal attempts them in.
  registerMetric.foreach { case (component, register) =>
    test(s"removal retires the $component metric when it is the only one the ingest registered") {
      val metrics = newMetrics()
      register(metrics)

      assert(metrics.removeIngestMetrics(ns, "numbers"))
      assert(ingestMetricNames(metrics, "numbers").isEmpty)
    }
  }

  test("removeIngestMetrics reports whether anything was removed") {
    val metrics = newMetrics()
    assert(!metrics.removeIngestMetrics(ns, "never-registered"))

    metrics.ingestQueryTimer(ns, "numbers")
    assert(metrics.removeIngestMetrics(ns, "numbers"))
    assert(!metrics.removeIngestMetrics(ns, "numbers"), "a second removal has nothing left to remove")
  }

  test("a recreated ingest does not inherit the deleted ingest's totals") {
    val metrics = newMetrics()
    val firstCount = metrics.ingestCountMeter(ns, "numbers")
    val firstTimer = metrics.ingestQueryTimer(ns, "numbers")
    firstCount.mark(7)
    firstTimer.update(java.time.Duration.ofMillis(5))

    metrics.removeIngestMetrics(ns, "numbers")

    val secondCount = metrics.ingestCountMeter(ns, "numbers")
    val secondTimer = metrics.ingestQueryTimer(ns, "numbers")
    assert(secondCount.getCount == 0)
    assert(secondTimer.getCount == 0)

    // The deleted ingest's own final stats are unaffected: it holds the metric objects directly, and
    // the status API reads its rates from those rather than from the registry.
    assert(firstCount.getCount == 7)
    assert(firstTimer.getCount == 1)
  }

  // An ingest whose configuration is still being validated builds its metrics unregistered and publishes
  // them through registerIngestMetric only once it is known to be starting, so these two have to agree on
  // names as closely as the accessors above do.
  test("a separately registered metric lands under the name removal retires") {
    val metrics = newMetrics()
    val countMeter = new com.codahale.metrics.Meter

    metrics.registerIngestMetric(ns, "numbers", IngestHostMetrics.IngestCountComponent, countMeter)

    assert(ingestMetricNames(metrics, "numbers").size == 1)
    // The registered instance is the one reporting, not a fresh one the registry made: the ingest is
    // already marking the instance it handed over.
    countMeter.mark(3)
    assert(metrics.ingestCountMeter(ns, "numbers").getCount == 3)

    assert(metrics.removeIngestMetrics(ns, "numbers"))
    assert(ingestMetricNames(metrics, "numbers").isEmpty)
  }

  test("registering over a name already taken replaces it rather than throwing") {
    val metrics = newMetrics()
    metrics.ingestCountMeter(ns, "numbers").mark(7)

    val replacement = new com.codahale.metrics.Meter
    metrics.registerIngestMetric(ns, "numbers", IngestHostMetrics.IngestCountComponent, replacement)

    assert(metrics.ingestCountMeter(ns, "numbers").getCount == 0)
  }

  test("removal is scoped to one namespace") {
    val metrics = newMetrics()
    val other = NamespaceId("otherns")
    metrics.ingestQueryTimer(ns, "numbers")
    metrics.ingestQueryTimer(other, "numbers")

    metrics.removeIngestMetrics(ns, "numbers")

    assert(ingestMetricNames(metrics, "numbers").size == 1)
    assert(ingestMetricNames(metrics, "numbers").forall(_.startsWith(other.name)))
  }

  test("removal matches creation in the default namespace, whose prefix an enterprise instance omits") {
    for (omitDefaultNamespace <- Seq(true, false)) {
      val metrics = newMetrics(omitDefaultNamespace)
      metrics.ingestCountMeter(defaultNamespaceId, "numbers")
      metrics.ingestQueryTimer(defaultNamespaceId, "numbers")

      assert(
        metrics.removeIngestMetrics(defaultNamespaceId, "numbers"),
        s"removal did not match creation with omitDefaultNamespace = $omitDefaultNamespace",
      )
      assert(ingestMetricNames(metrics, "numbers").isEmpty)
    }
  }
}
