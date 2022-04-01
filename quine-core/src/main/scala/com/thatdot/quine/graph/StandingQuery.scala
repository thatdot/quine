package com.thatdot.quine.graph

import java.time.Instant

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import com.codahale.metrics.{Counter, Meter}
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.model._

/** Information about a standing query that gets persisted and reloaded on startup
  *
  * ==Queue size and backpressuring==
  *
  * Standing query results get buffered up into an Akka source queue. There are two somewhat
  * arbitrary parameters we must choose in relation to this queue:
  *
  *   1. at what queue size do we start backpressuring ingest (see [[BaseGraph.ingestValve]])?
  *   2. at what queue size do we start dropping results?
  *
  * @param name standing query name
  * @param id unique ID of the standing query
  * @param query the pattern being looked for
  * @param queueBackpressureThreshold buffer size at which ingest starts being backpressured
  * @param queueMaxSize buffer size at which SQ results start being dropped
  */
final case class StandingQuery(
  name: String,
  id: StandingQueryId,
  query: StandingQueryPattern,
  queueBackpressureThreshold: Int,
  queueMaxSize: Int
)

object StandingQuery {

  /** @see [[StandingQuery.queueMaxSize]]
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

  /** @see [[StandingQuery.queueBackpressureThreshold]]
    *
    * The queue backpressure threshold is similar in function to the small internal buffers Akka
    * adds at async boundaries: a value of 1 is the most natural choice, but larger values may lead
    * to increased throughput. Akka's default for `akka.stream.materializer.max-input-buffer-size`
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

  case object DirectDgb extends DgbOrigin
  case object DirectSqV4 extends SqV4Origin
  final case class GraphPattern(
    pattern: GraphQueryPattern,
    cypherOriginal: Option[String]
  ) extends DgbOrigin
      with SqV4Origin
}

sealed abstract class StandingQueryPattern {

  def includeCancellation: Boolean

  def origin: PatternOrigin
}
object StandingQueryPattern extends LazyLogging {

  /** Create a standing query pattern from a graph pattern
    *
    * @param pattern graph pattern
    * @param cypherOriginal original Cypher query (if the pattern came from a Cypher query)
    * @param includeCancellation should results about negative matches be included?
    * @param useDomainGraphBranch should this be compiled into a DGB (or SQv4) query
    * @param labelsProperty which property is used for labels
    * @param idProvider which ID provider does the graph operate with
    * @return the compiled pattern
    */
  @throws[InvalidQueryPattern]("when the pattern cannot be turned into a SQ of the desired sort")
  def fromGraphPattern(
    pattern: GraphQueryPattern,
    cypherOriginal: Option[String],
    includeCancellations: Boolean,
    useDomainGraphBranch: Boolean,
    labelsProperty: Symbol,
    idProvider: QuineIdProvider
  ): StandingQueryPattern = {
    val origin = PatternOrigin.GraphPattern(pattern, cypherOriginal)
    if (useDomainGraphBranch) {
      if (!pattern.distinct) {
        logger.warn(
          cypherOriginal match {
            case Some(cypherQuery) =>
              s"""DistinctId Standing Queries that do not specify a `DISTINCT` clause are deprecated.
                  |DistinctId queries without `DISTINCT` are deprecated and will be removed in the future.
                  |Query was: '$cypherQuery'""".stripMargin.replace('\n', ' ')
            case None =>
              s"""DistinctId Standing Queries that do not specify `distinct` are deprecated and a future release will
                  |require that DistinctId Standing Queries use patterns with `distinct`.
                  |Query pattern was: $pattern""".stripMargin.replace('\n', ' ')
          }
        )
      }
      val (branch, returnColumn) = pattern.compiledDomainGraphBranch(labelsProperty)
      Branch(branch, returnColumn.formatAsString, returnColumn.aliasedAs, includeCancellations, origin)
    } else {
      if (pattern.distinct) {
        // QU-568
        throw InvalidQueryPattern("MultipleValues Standing Queries do not yet support `DISTINCT`")
      }
      val compiledQuery = pattern.compiledCypherStandingQuery(labelsProperty, idProvider)
      SqV4(compiledQuery, includeCancellations, origin)
    }
  }

  /** A DomainGraphBranch standing query
    *
    * @param branch branch to execute
    * @param formatReturnAsStr return `strId(n)` (as opposed to `id(n)`)
    * @param aliasReturnAs name given to the returned value
    * @param includeCancellation should results about negative matches be included?
    * @param origin how did the user specify this query?
    */
  final case class Branch(
    branch: DomainGraphBranch[Test],
    formatReturnAsStr: Boolean,
    aliasReturnAs: Symbol,
    includeCancellation: Boolean,
    origin: PatternOrigin.DgbOrigin
  ) extends StandingQueryPattern

  /** An SQv4 standing query (also referred to as a Cypher standing query)
    *
    * @param compiledQuery compiled query to execute
    * @param includeCancellations should result cancellations be reported?
    * @param origin how did the user specify this query?
    */
  final case class SqV4(
    compiledQuery: cypher.StandingQuery,
    includeCancellation: Boolean,
    origin: PatternOrigin.SqV4Origin
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
  val resultsQueue: BoundedSourceQueue[StandingQueryResult],
  val query: StandingQuery,
  val resultsHub: Source[StandingQueryResult, NotUsed],
  outputTermination: Future[Done],
  val resultMeter: Meter,
  val droppedCounter: Counter,
  val startTime: Instant
) {

  def this(
    resultsQueue: BoundedSourceQueue[StandingQueryResult],
    query: StandingQuery,
    resultsHub: Source[StandingQueryResult, NotUsed],
    outputTermination: Future[Done],
    metrics: HostQuineMetrics
  ) =
    this(
      resultsQueue,
      query,
      resultsHub,
      outputTermination,
      resultMeter = metrics.standingQueryResultMeter(query.name),
      droppedCounter = metrics.standingQueryDroppedCounter(query.name),
      startTime = Instant.now()
    )

  def cancel(): Future[Unit] = {
    resultsQueue.complete()
    /* Using outputTermination instead of resultsQueue.watchCompletion, because a watchCompletion future may not
     * complete if the termination is caused by a sink cancellation rather than a source completion. Note that since
     * [[resultsHub]] is (so far) always a BroadcastHub, it shouldn't ever cancel, so this is probably unnecessary
     */
    outputTermination.map(_ => ())(ExecutionContexts.parasitic)
  }

  /** How many results are currently accumulated in the buffer */
  def bufferCount: Int = resultsQueue.size()
}
