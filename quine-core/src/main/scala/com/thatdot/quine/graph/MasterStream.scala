package com.thatdot.quine.graph

import scala.annotation.unused
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Materializer, UniqueKillSwitch}
import org.apache.pekko.{Done, NotUsed}

import org.apache.pekko

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.util.PekkoStreams.errorSuppressingMergeHub

class MasterStream(implicit val mat: Materializer, val logConfig: LogConfig) extends LazySafeLogging {
  import MasterStream._

  private val (_ingestHub, ingestSource) =
    errorSuppressingMergeHub[IngestSrcExecToken]("master-stream-ingest-mergehub").preMaterialize()
  private val (_sqResultsHub, sqResultsSource) =
    errorSuppressingMergeHub[SqResultsExecToken]("master-stream-sq-results-mergehub").preMaterialize()
  private val (_nodeSleepHub, nodeSleepSource) =
    errorSuppressingMergeHub[NodeSleepExecToken]("master-stream-node-sleeps-mergehub").preMaterialize()
  private val (_persistorHub, persistorSource) =
    errorSuppressingMergeHub[PersistorExecToken]("master-stream-persistor-mergehub").preMaterialize()

  // Pekko docs are misleading. `false` means that the new source is being added via mergePreferred is preferred
  // over the original source / receiver. E.g, Source.repeat(IdleToken) will have the lowest preference, followed
  // by ingestSource, etc.
  private val preferNewHubOverUpstream = false
  @unused
  private[this] def demonstratePekkoDocsMisleadingness(): Unit = {
    val left = Source.repeat("Left")
    val right = Source.repeat("Right")

    var preferRight = true // Pekko docs issue here!
    val _ = left
      .mergePreferred(right, preferRight)
      .runForeach(println(_)) // Logs "Left" forever, even though "preferRight = true"

    preferRight = false
    val _ = left
      .mergePreferred(right, preferRight)
      .runForeach(println(_)) // Logs "Right" forever, even though "preferRight = false"
  }

  // These sinks are the main interface to the master stream -- each accepts completion tokens for a single kind of work
  val ingestCompletionsSink = _ingestHub
  val standingOutputsCompletionSink = _sqResultsHub
  val nodeSleepCompletionsSink = _nodeSleepHub
  val persistorCompletionsSink = _persistorHub

  // Sink to give the stream an overall outlet (and thus allow it to actually run).
  // NB this never backpressures, so all the upstreams will be allowed to run as quickly as the `mergePreferred`
  // overhead allows.
  private val loggingSink: Sink[ExecutionToken, Future[Done]] =
    Sink.foreach[ExecutionToken](x => logger.trace(safe"${Safe(x.name)}")).named("master-stream-logging-sink")

  /** Cached throttle cost for ingest streams.
    *
    * Will be set from [[QuineEnterpriseApp]] after the [[ClusterManager]] communicates
    * with the license server and each host.
    */
  @volatile private var cachedThrottleCost: Int = 1

  /** This is the max allowed rate for the bucket size included in Flow[_].throttle */
  private val bucketSize = 1.second.toNanos.toInt

  /** To fit `elementsPer` in a bucket of size `bucketSize` using Pekko's cost function throttle the cost
    *
    * In other words given a limit, `bucketSize`, how big of a slice of that limit should each element `cost`` so that only
    * `elementsPer` fit.
    *
    * If the elements per second is greater than the bucket size make their cost one to avoid any
    * wonky-ness that could exist in that case.
    */
  private def elementsPerToCost(elementsPer: Long): Int =
    math.max(1, (bucketSize.toLong / elementsPer).toInt)

  /** Returns the cached throttle cost. This avoids expensive atomic reads on every element.
    *
    *  A cost of 1 means that elements per time will be equal to the bucket size.
    */
  private def throttleCostFunction[A]: A => Int = _ => cachedThrottleCost

  /** Turns off the throttling of the ingest portion of the [[MasterStream]] */
  def disableIngestThrottle(): Unit =
    cachedThrottleCost = 1

  /** Throttles the ingest portion of the [[MasterStream]] `elementsPerSecond` number of elements per second */
  def enableIngestThrottle(elementsPerSecond: Long): Unit = {
    val newCost = elementsPerToCost(elementsPerSecond)
    cachedThrottleCost = newCost
  }

  Source
    .repeat(IdleToken)
    .throttle(1, 1.second)
    .mergePreferred(
      ingestSource.throttle(bucketSize, 1.second, throttleCostFunction),
      preferNewHubOverUpstream,
    )
    .mergePreferred(sqResultsSource, preferNewHubOverUpstream)
    .mergePreferred(nodeSleepSource, preferNewHubOverUpstream)
    .mergePreferred(persistorSource, preferNewHubOverUpstream)
    .runWith(loggingSink)(mat)
}

case object MasterStream {

  sealed trait ExecutionToken { val name: String }
  case object IdleToken extends ExecutionToken { val name: String = this.toString }
  final case class IngestSrcExecToken(name: String) extends ExecutionToken
  final case class SqResultsExecToken(name: String) extends ExecutionToken
  final case class NodeSleepExecToken(name: String) extends ExecutionToken
  final case class PersistorExecToken(name: String) extends ExecutionToken

  type IngestSrcType = Source[IngestSrcExecToken, NotUsed]
  type SqResultsSrcType = Source[SqResultsExecToken, UniqueKillSwitch]
  type NodeSleepSrcType = Source[NodeSleepExecToken, UniqueKillSwitch]
  type PersistorSrcType = Source[PersistorExecToken, UniqueKillSwitch]
}

trait IngestControl {
  def pause(): Future[Boolean]
  def unpause(): Future[Boolean]
  def terminate(): Future[pekko.Done]
}
