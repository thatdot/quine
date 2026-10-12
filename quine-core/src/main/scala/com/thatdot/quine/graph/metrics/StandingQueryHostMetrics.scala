package com.thatdot.quine.graph.metrics

import java.util.concurrent.atomic.LongAdder

import com.codahale.metrics.{Counter, Histogram, Meter, Metric, MetricRegistry, Timer}

import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}

/** The part of [[HostQuineMetrics]] that manages each standing query's metrics on this host.
  *
  * This trait holds no state of its own; a standing query's metrics live in `metricRegistry`, named
  * `<namespace>.standing-queries.<component>.<sqName>`. Its public functions create a query's metrics,
  * register them once the query has started, and remove them when the query is cancelled.
  */
trait StandingQueryHostMetrics {
  import StandingQueryHostMetrics._

  def metricRegistry: MetricRegistry
  def metricName(namespaceId: NamespaceId, components: List[String]): String

  /** Name of one of a standing query's metrics: `<namespace>.standing-queries.<component>.<sqName>`.
    * Note that the component precedes the query's name here, the opposite of an ingest's metric names.
    */
  private def standingQueryMetricName(namespaceId: NamespaceId, component: String, sqName: String): String =
    metricName(namespaceId, List(HostQuineMetrics.StandingQueryMetricComponent, component, sqName))

  /** Histogram of size (in bytes) of persisted standing query states.
    * Named by the query's id rather than its name, unlike the query's other metrics.
    */
  private[graph] def standingQueryStateSize(namespaceId: NamespaceId, sqId: StandingQueryId): Histogram =
    metricRegistry.histogram(standingQueryStateSizeName(namespaceId, sqId))

  private def standingQueryStateSizeName(namespaceId: NamespaceId, sqId: StandingQueryId): String =
    standingQueryMetricName(namespaceId, StandingQueryStatesComponent, sqId.uuid.toString)

  /** Returns fresh metrics for a standing query that is about to start. They are not visible to metrics
    * reporting until [[registerStandingQueryMetrics]] is called, so a query that fails to start can simply
    * drop them.
    */
  private[graph] def unregisteredStandingQueryMetrics(): StandingQueryMetrics =
    StandingQueryMetrics(
      resultMeter = new Meter(),
      cancellationMeter = new Meter(),
      droppedCounter = new Counter(),
      queueTimer = new Timer(),
      consumptionMeter = new Meter(),
      resultHashCode = new LongAdder(),
    )

  /** Publishes a standing query's metrics under that query's name. Called once the query has started. */
  private[graph] def registerStandingQueryMetrics(
    namespaceId: NamespaceId,
    sqName: String,
    sqMetrics: StandingQueryMetrics,
  ): Unit = {
    def register(component: String, metric: Metric): Unit = {
      val name = standingQueryMetricName(namespaceId, component, sqName)
      // `register` throws on a name already taken, so clear it first.
      metricRegistry.remove(name)
      metricRegistry.register(name, metric)
      ()
    }
    register(StandingQueryResultsComponent, sqMetrics.resultMeter)
    register(StandingQueryCancellationsComponent, sqMetrics.cancellationMeter)
    register(StandingQueryDroppedComponent, sqMetrics.droppedCounter)
    register(StandingQueryQueueTimeComponent, sqMetrics.queueTimer)
    register(StandingQueryConsumptionComponent, sqMetrics.consumptionMeter)
  }

  /** Retire every metric registered for a standing query. This is called when that query is cancelled.
    *
    * CAUTION the `states` histogram can come back: node actors register it as they persist standing
    * query state (see [[standingQueryStateSize]]), so a node that sleeps after the cancellation
    * re-creates it under the retired name.
    *
    * @return whether any registered metric was removed
    */
  private[graph] def removeStandingQueryMetrics(
    namespaceId: NamespaceId,
    sqName: String,
    sqId: StandingQueryId,
  ): Boolean = {
    val removed = List(
      StandingQueryResultsComponent,
      StandingQueryDroppedComponent,
      StandingQueryCancellationsComponent,
      StandingQueryConsumptionComponent,
      StandingQueryQueueTimeComponent,
    ).map(standingQueryMetricName(namespaceId, _, sqName))
      .map(metricRegistry.remove)
    // Not folded into the list above: this one is named by id, where the rest are named by query name.
    val removedStates = metricRegistry.remove(standingQueryStateSizeName(namespaceId, sqId))
    (removedStates :: removed).exists(identity)
  }
}

object StandingQueryHostMetrics {

  /** The name segment identifying each metric a standing query registers under its own name. Naming
    * them once is what keeps registration and removal in step: a metric registered under a name
    * [[StandingQueryHostMetrics.removeStandingQueryMetrics]] does not know about would outlive the standing
    * query that made it.
    */
  val StandingQueryResultsComponent = "results"
  val StandingQueryDroppedComponent = "dropped"
  val StandingQueryCancellationsComponent = "cancellations"
  val StandingQueryConsumptionComponent = "consumption"
  val StandingQueryQueueTimeComponent = "queue-time"

  /** Deliberately not among the components above: the persisted-state histogram is named by the query's id
    * rather than by its name, so anything walking a query's metrics needs both to address it.
    */
  val StandingQueryStatesComponent = "states"

  /** Everything one standing query on this host measures itself with, created once when the query starts
    * and held by the query from then on.
    *
    * The persisted-state histogram is not here: node actors register it themselves as they write a
    * query's state (see [[StandingQueryHostMetrics.standingQueryStateSize]]).
    *
    * @param resultMeter results produced by the query
    * @param cancellationMeter cancellation (negative match) results enqueued
    * @param droppedCounter results dropped (should be zero unless something has gone wrong)
    * @param queueTimer how long results spend in the result queue before each output accepts them. A single
    *        result may be measured once per sink on the results hub (declared outputs, SSE, wiretaps).
    * @param consumptionMeter results leaving the queue, counted once each before being fanned out to outputs
    * @param resultHashCode running total of result hash codes (stays at zero unless the query asked for
    *        one), kept out of the metric registry so no reporter publishes it
    */
  final case class StandingQueryMetrics(
    resultMeter: Meter,
    cancellationMeter: Meter,
    droppedCounter: Counter,
    queueTimer: Timer,
    consumptionMeter: Meter,
    resultHashCode: LongAdder,
  )
}
