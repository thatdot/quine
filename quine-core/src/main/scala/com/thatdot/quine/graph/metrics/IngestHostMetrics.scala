package com.thatdot.quine.graph.metrics

import com.codahale.metrics.{Meter, Metric, MetricRegistry, Timer}

import com.thatdot.quine.graph.NamespaceId

/** The part of [[HostQuineMetrics]] that manages each ingest stream's metrics on this host.
  *
  * This trait holds no state of its own; every metric lives in `metricRegistry`, named
  * `<namespace>.ingest.<ingestName>.<component>`. Its public functions look up (creating if absent),
  * register, and remove those metrics.
  */
trait IngestHostMetrics {
  import IngestHostMetrics._

  def metricRegistry: MetricRegistry
  def metricName(namespaceId: NamespaceId, components: List[String]): String

  /** Meter of records ingested by a named ingest on this host (`<namespace>.ingest.<ingestName>.count`). */
  def ingestCountMeter(namespace: NamespaceId, ingestName: String): Meter =
    metricRegistry.meter(ingestMetricName(namespace, ingestName, IngestCountComponent))

  /** Meter of bytes ingested by a named ingest on this host (`<namespace>.ingest.<ingestName>.bytes`) */
  def ingestBytesMeter(namespaceId: NamespaceId, ingestName: String): Meter =
    metricRegistry.meter(ingestMetricName(namespaceId, ingestName, IngestBytesComponent))

  /** A timer tracking ingest query executions.
    * CAUTION: Unlike the other ingest-related metrics, this timer is not paused when the ingest is completed/failed.
    * This means its `Metered`-implementing metrics (pretty much anything with "rate" or "count" in the name) will
    * stale once ingest has stopped: the rate decays toward zero rather than holding the stopped stream's final
    * value the way [[com.thatdot.quine.app.routes.IngestMetered.freeze]] makes an IngestMeter's rates hold.
    * This metric also uses the naming scheme consistent with other metrics, NOT the naming scheme used for ingest
    * metrics.
    *
    * Resolve this once when building the stream and hold the reference, the way `IngestMeter` holds its meters:
    * since [[removeIngestMetrics]] retires the timer when the ingest is deleted, a per-record lookup would
    * re-register it from records still draining and leave the deleted ingest's timer behind.
    * @see [[com.thatdot.quine.app.routes.IngestMeter]]
    */
  def ingestQueryTimer(namespaceId: NamespaceId, ingestName: String): Timer =
    metricRegistry.timer(ingestMetricName(namespaceId, ingestName, IngestQueryComponent))

  /** A timer tracking ingest record deserialization time.
    * CAUTION: Unlike the other ingest-related metrics, this timer is not paused when the ingest is completed/failed.
    * This means its `Metered`-implementing metrics (pretty much anything with "rate" or "count" in the name) will
    * stale once ingest has stopped. See [[ingestQueryTimer]] for that lifecycle and for why the timer must be
    * resolved once per stream rather than per record.
    * This metric also uses the naming scheme consistent with other metrics, NOT the naming scheme used for ingest
    * metrics.
    * @see [[com.thatdot.quine.app.routes.IngestMeter.unmanagedDeserializationTimer]]
    */
  def ingestDeserializationTimer(namespaceId: NamespaceId, ingestName: String): Timer =
    metricRegistry.timer(ingestMetricName(namespaceId, ingestName, IngestDeserializationComponent))

  /** Publish an already-built ingest metric under the name the accessor above would have given it, and
    * that [[removeIngestMetrics]] retires. For an ingest that built its metrics unregistered so that it
    * could thread them through configuration validation: registering only once that validation has
    * passed means a rejected configuration leaves nothing behind to retire.
    *
    * The query timer takes no part in this: it is resolved from the registry at materialization (see
    * [[ingestQueryTimer]]), which is long past the point where creation can still fail.
    * @see [[com.thatdot.quine.app.routes.IngestMeter.register]]
    */
  def registerIngestMetric(namespaceId: NamespaceId, ingestName: String, component: String, metric: Metric): Unit = {
    val name = ingestMetricName(namespaceId, ingestName, component)
    // `register` throws on a name already taken, where the get-or-create accessors above tolerate one;
    // clearing first keeps this as forgiving as they are. Registration happens under the same lock that
    // guards the ingest map, so no concurrent creation can slip into the gap between the two calls.
    metricRegistry.remove(name)
    metricRegistry.register(name, metric)
    ()
  }

  /** Retire every metric registered for a named ingest. This is called when that ingest is deleted.
    * @return whether any metric was removed
    */
  def removeIngestMetrics(namespaceId: NamespaceId, ingestName: String): Boolean =
    List(IngestCountComponent, IngestBytesComponent, IngestQueryComponent, IngestDeserializationComponent)
      .map(ingestMetricName(namespaceId, ingestName, _))
      .map(metricRegistry.remove)
      .exists(identity)

  /** Name of one of an ingest stream's metrics: `<namespace>.ingest.<ingestName>.<component>`. */
  private def ingestMetricName(namespace: NamespaceId, ingestName: String, component: String): String =
    metricName(namespace, List(HostQuineMetrics.IngestMetricComponent, ingestName, component))
}

object IngestHostMetrics {

  /** The last name segment of each metric an ingest can register. Naming them once is what keeps
    * registration and removal in step: a metric registered under a name [[IngestHostMetrics.removeIngestMetrics]]
    * does not know about would outlive the ingest that made it.
    */
  val IngestCountComponent = "count"
  val IngestBytesComponent = "bytes"
  val IngestQueryComponent = "query"
  val IngestDeserializationComponent = "deserialization"
}
