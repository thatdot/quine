package com.thatdot.quine.graph

import java.time.Instant

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{BoundedSourceQueue, QueueOfferResult}
import org.apache.pekko.{Done, NotUsed}

import com.codahale.metrics.{Counter, Meter, Timer}

import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, QuinePattern}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

/** Information about a standing query that gets persisted and reloaded on startup
  *
  * ==Queue size and backpressuring==
  *
  * Standing query results get buffered up into a Pekko source queue. There are two somewhat
  * arbitrary parameters we must choose in relation to this queue:
  *
  *   1. at what queue size do we start backpressuring ingest (see [[BaseGraph.ingestValve]])?
  *   2. at what queue size do we start dropping results?
  *
  * @param name standing query name
  * @param id unique ID of the standing query
  * @param queryPattern the pattern being looked for
  * @param queueBackpressureThreshold buffer size at which ingest starts being backpressured
  * @param queueMaxSize buffer size at which SQ results start being dropped
  */
final case class StandingQueryInfo(
  name: String,
  id: StandingQueryId,
  queryPattern: StandingQueryPattern,
  queueBackpressureThreshold: Int,
  queueMaxSize: Int,
  shouldCalculateResultHashCode: Boolean,
)

object StandingQueryInfo {

  /** @see [[StandingQueryInfo.queueMaxSize]]
    *
    * Beyond this size, the queue of SQ results will begin dropping results. We almost don't need
    * this limit since we should be backpressuring long before the limit is reached. However:
    *
    *  - we'd like to guard against pathological cases where processing 1 SQ result produces 2 more
    *    SQ results (ideally this is before we OOM)...
    *
    *  - BUT we don't want to drop results just because a user ran "propagate" and they have large
    *    in-memory shard limits resulting a sudden burst of results
    */
  val DefaultQueueMaxSize = 1048576 // 2^20

  /** @see [[StandingQueryInfo.queueBackpressureThreshold]]
    *
    * The queue backpressure threshold is similar in function to the small internal buffers Pekko
    * adds at async boundaries: a value of 1 is the most natural choice, but larger values may lead
    * to increased throughput. Pekko's default for `pekko.stream.materializer.max-input-buffer-size`
    * is 16.
    *
    * Experimentally, we've found we get optimal throughput around 32 (larger than that leads to
    * variability in throughput and very large leads to needless memory pressure).
    */
  val DefaultQueueBackpressureThreshold = 32
}

/** How did the user specify their standing query?
  *
  * This is kept around for re-creating the initial user-issued queries and for debugging.
  */
sealed abstract class PatternOrigin
object PatternOrigin {
  sealed trait DgbOrigin extends PatternOrigin
  sealed trait SqV4Origin extends PatternOrigin

  case object QuinePatternOrigin extends PatternOrigin

  case object DirectDgb extends DgbOrigin
  case object DirectSqV4 extends SqV4Origin
  final case class GraphPattern(
    pattern: GraphQueryPattern,
    cypherOriginal: Option[String],
  ) extends DgbOrigin
      with SqV4Origin
}

sealed abstract class StandingQueryPattern {

  def includeCancellation: Boolean
  def origin: PatternOrigin
}
object StandingQueryPattern {

  /** A DomainGraphNode standing query
    *
    * @param dgnId node to "execute"
    * @param formatReturnAsStr return `strId(n)` (as opposed to `id(n)`)
    * @param aliasReturnAs name given to the returned value
    * @param includeCancellation should results about negative matches be included?
    * @param origin how did the user specify this query?
    */
  final case class DomainGraphNodeStandingQueryPattern(
    dgnId: DomainGraphNodeId,
    formatReturnAsStr: Boolean,
    aliasReturnAs: Symbol,
    includeCancellation: Boolean,
    origin: PatternOrigin.DgbOrigin,
  ) extends StandingQueryPattern

  /** An SQv4 standing query (also referred to as a Cypher standing query)
    *
    * @param compiledQuery compiled query to execute
    * @param includeCancellation should result cancellations be reported? (currently always treated as false)
    * @param origin how did the user specify this query?
    */
  final case class MultipleValuesQueryPattern(
    compiledQuery: MultipleValuesStandingQuery,
    includeCancellation: Boolean,
    origin: PatternOrigin.SqV4Origin,
  ) extends StandingQueryPattern

  final case class QuinePatternQueryPattern(
    quinePattern: QuinePattern,
    includeCancellation: Boolean,
    origin: PatternOrigin,
  ) extends StandingQueryPattern
}

/** Information kept around about a standing query (on this host) while the query is running
  *
  * TODO: should `startTime` be the initial registration time?
  *
  * @param resultsQueue the queue into which new results should be offered
  * @param query static information about the query (this is what is persisted)
  * @param resultsHub a source that lets you listen in on current results
  * @param outputTermination completes when [[resultsQueue]] completes OR when [[resultsHub]] cancels
  * @param resultMeter metric of results coming out of the query
  * @param droppedCounter counter of rsults dropped (should be zero unless something has gone wrong)
  * @param startTime when the query was started (or restarted) running
  */
final class RunningStandingQuery(
  private val resultsQueue: BoundedSourceQueue[StandingQueryResult.WithQueueTimer],
  val query: StandingQueryInfo,
  val resultsHub: Source[StandingQueryResult, NotUsed],
  outputTermination: Future[Done],
  val queueTimer: Timer,
  val resultMeter: Meter,
  val droppedCounter: Counter,
  val startTime: Instant,
) extends LazySafeLogging {

  def this(
    resultsQueue: BoundedSourceQueue[StandingQueryResult.WithQueueTimer],
    query: StandingQueryInfo,
    inNamespace: NamespaceId,
    resultsHub: Source[StandingQueryResult, NotUsed],
    outputTermination: Future[Done],
    metrics: HostQuineMetrics,
  ) =
    this(
      resultsQueue,
      query,
      resultsHub,
      outputTermination,
      resultMeter = metrics.standingQueryResultMeter(inNamespace, query.name),
      droppedCounter = metrics.standingQueryDroppedCounter(inNamespace, query.name),
      queueTimer = metrics.standingQueryResultQueueTimer(inNamespace, query.name),
      startTime = Instant.now(),
    )

  def terminateOutputQueue(): Future[Unit] = {
    resultsQueue.complete()
    /* Using outputTermination instead of resultsQueue.watchCompletion, because a watchCompletion future may not
     * complete if the termination is caused by a sink cancellation rather than a source completion. Note that since
     * [[resultsHub]] is (so far) always a BroadcastHub, it shouldn't ever cancel, so this is probably unnecessary
     */
    outputTermination.map(_ => ())(ExecutionContext.parasitic)
  }

  /** How many results are currently accumulated in the buffer */
  def bufferCount: Int = resultsQueue.size()

  /** Enqueue a result, returning true if the result was successfully enqueued, false otherwise
    */
  def offerResult(result: StandingQueryResult)(implicit logConfig: LogConfig): Boolean = {
    val timerCtx = queueTimer.time()

    val success = resultsQueue.offer(result.withQueueTimer(timerCtx)) match {
      case QueueOfferResult.Enqueued =>
        true
      case QueueOfferResult.Failure(err) =>
        logger.warn(
          log"onResult: failed to enqueue Standing Query result for: ${Safe(query.name)}. Result: $result"
          withException err,
        )
        false
      case QueueOfferResult.QueueClosed =>
        logger.warn(
          log"""onResult: Standing Query Result arrived but result queue already closed for:
               |${Safe(query.name)}. Dropped result: $result""".cleanLines,
        )
        false
      case QueueOfferResult.Dropped =>
        logger.warn(
          log"onResult: dropped Standing Query result for: ${Safe(query.name)}. Result: $result",
        )
        false
    }
    // On results (but not cancellations) update the relevant metrics
    if (result.meta.isPositiveMatch) {
      if (success) resultMeter.mark()
      else droppedCounter.inc()
    }
    success
  }
}
