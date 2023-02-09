package com.thatdot.quine.app.util

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.util.AtLeastOnceCypherQuery.RetriableQueryFailure
import com.thatdot.quine.graph.cypher.Location
import com.thatdot.quine.graph.messaging.ExactlyOnceTimeoutException
import com.thatdot.quine.graph.{CypherOpsGraph, GraphNotReadyException, ShardNotAvailableException, cypher}
import com.thatdot.quine.persistor.WrappedPersistorException

/** A Cypher query that will be retried against the graph until the entire query succeeds
  *
  * @param query               the compiled Cypher query to run at least once
  * @param cypherParameterName the name of the Cypher parameter left free for values in [[query]]
  * @param debugName           a name attributed to this specific AtLeastOnceCypherQuery for use in debug logging.
  *                            For example, "ingest-stream-wikipediaAuthorsIngest"
  * @param startupRetryDelay   how long to wait before retrying a failed query when the failure occurred before the
  *                            query interpreter started
  */
final case class AtLeastOnceCypherQuery(
  query: cypher.CompiledQuery[Location.External],
  cypherParameterName: String,
  debugName: String = "unnamed",
  startupRetryDelay: FiniteDuration = 100.millis
) extends LazyLogging {

  /** Runs a compiled Cypher query with simple retry logic, ensuring that ephemeral failures such as temporary network
    * outages (@see [[RetriableQueryFailure]]) don't cause the query to fail entirely. However, side effects as a
    * result of running [[query]] may have happened multiple times, such as creation of nodes. Use this with caution for
    * non-idempotent queries.
    *
    * @param value the query input to be passed to the Cypher interpreter as a parameter (as [[cypherParameterName]])
    * @return a Source that will yield a stream ending with one full set of results for [[query]] given [[value]] bound
    *         as [[cypherParameterName]]. This can be thought of as returning a weaker version of a
    *          [[com.thatdot.quine.graph.cypher.QueryResults]]
    */
  def stream(value: cypher.Value)(implicit
    graph: CypherOpsGraph
  ): Source[Vector[cypher.Value], NotUsed] = {
    // this Source represents the work that would be needed to query over one specific `value`
    // Work does not begin until the source is `run` (after the recovery strategy is hooked up below)
    // If a recoverable error occurs, instead return a Source that will fail after a small delay
    // so that recoverWithRetries (below) can retry the query
    def bestEffortSource: Source[Vector[cypher.Value], NotUsed] =
      try graph.cypherOps.query(query, atTime = None, parameters = Map(cypherParameterName -> value)).results
      catch {
        case RetriableQueryFailure(e) =>
          // TODO arbitrary timeout delays repeated failing calls to requiredGraphIsReady in implementation of .run above
          Source.future(akka.pattern.after(startupRetryDelay)(Future.failed(e))(graph.system))
      }

    bestEffortSource
      .recoverWithRetries(
        attempts = -1, // retry forever, relying on the relayAsk (used in the Cypher interpreter) to slow down attempts
        { case RetriableQueryFailure(e) =>
          logger.debug(
            s"""Suppressed ${e.getClass.getSimpleName} during execution of query: $debugName, retrying now.
               |Ingested item: $value. Query: "${query.queryText}". Suppressed exception:
               |${e.getMessage}"""".stripMargin.replace('\n', ' ')
          )
          bestEffortSource
        }
      )
  }.named(s"at-least-once-cypher-query-$debugName")
}
object AtLeastOnceCypherQuery {

  /** Helper to recognize errors that can be caught and retried during query execution (for example, errors that could
    * occur as a result of graph topology changing, or GC pauses)
    *
    * These exceptions should include any that can occur as the result of network latency or temporary network
    * failures, but should not include any exceptions that will always get thrown on subsequent retries (e.g.
    * deserialization errors)
    *
    * Inspired by [[scala.util.control.NonFatal]]
    */
  object RetriableQueryFailure {
    def unapply(e: Throwable): Option[Throwable] = e match {
      // A relayAsk-based protocol timed out, but might succeed when retried
      case _: ExactlyOnceTimeoutException => Some(e)
      // Graph is not currently ready, but may be in the future
      case _: GraphNotReadyException => Some(e)
      // Shard has dropped out (unavailable) but might be replaced
      case _: ShardNotAvailableException => Some(e)
      // Some problem from the persistor. This can include ephemeral errors like timeouts, so conservatively retry
      case _: WrappedPersistorException => Some(e)
      // Retriable failures related to StreamRefs:
      case _: akka.stream.RemoteStreamRefActorTerminatedException => Some(e)
      case _: akka.stream.StreamRefSubscriptionTimeoutException => Some(e)
      case _: akka.stream.InvalidSequenceNumberException => Some(e)
      case _ => None
    }
  }
}
