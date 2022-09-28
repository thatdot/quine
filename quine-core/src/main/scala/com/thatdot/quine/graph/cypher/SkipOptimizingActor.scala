package com.thatdot.quine.graph.cypher

import scala.compat.ExecutionContexts

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.stream.scaladsl.{BroadcastHub, Source}

import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.cypher.Query.Return
import com.thatdot.quine.model.Milliseconds

/** Manages SKIP optimizations for a [family of] queries, eg (SKIP n LIMIT m, SKIP n+m LIMIT o, ...)
  *
  * This main service this actor provides is maintaining a [[BroadcastHub]] ([[queryHub]]) that allows "resuming"
  * a source of query results for [[QueryFamily]]. This means that queries in the same "family", ie, differing only in
  * their SKIP/LIMIT clauses, can re-use the same materialized state. This can have substantial impact when dealing with
  * query families involving waking many nodes, such as pagination over a supernode's edges.
  *
  * NB This actor is only used from within a JVM, so it does not need (or have) the additional infrastructure to
  * work as a QuineRef. Accordingly, its messages also do not extend QuineMessage
  *
  * INV: queryFamily must have no [[Expr.Parameter]]s
  *
  * @param graph       the graph over which [[QueryFamily]] will be run
  * @param QueryFamily the canonical query representing the family of queries this actor should manage (eg, if this
  *                    actor optimizes queries like "MATCH (n) RETURN n SKIP $x LIMIT $y", the QueryFamily will be
  *                    (the compiled form of) "MATCH (n) RETURN n". Capitalized for implementation (pattern-matching)
  *                    convenience.
  * @param atTime      the timestamp at which [[QueryFamily]] will be run against the graph
  */
class SkipOptimizingActor(graph: CypherOpsGraph, QueryFamily: Query[Location.Anywhere], atTime: Option[Milliseconds])
    extends Actor
    with ActorLogging {
  import SkipOptimizingActor._

  /** Decommissions this actor by removing it from the skipOptimizerCache
    * INV: [[skipOptimizerCache]]'s removalListener will ensure this actor gets stopped
    *
    * This will be called off the actor thread and must not use local state (in particular,
    * this must not close over the actor's `context`)
    */
  private def decommission(): Unit =
    graph.cypherOps.skipOptimizerCache.invalidate(QueryFamily -> atTime)

  private def startQuery() = {
    log.debug(s"SkipOptimizingActor is beginning execution of query. AtTime: ${atTime}; query: $QueryFamily")
    graph.cypherOps
      .query(QueryFamily, parameters = Parameters.empty, atTime = atTime)
      .watchTermination() { case (mat, completesWithStream) =>
        /** Register a termination hook. This can be read roughly as "when the last element of the queryFamily query is
          * produced, shut down this SkipOptimizingActor". What we really want is "when the last element of the
          * queryFamily query is _consumed_ (via a Source constructed in response to a ResumeQuery message), shut down
          * this SkipOptimizingActor". Unfortunately, our main signal for which element is "last" is the upstream producer
          * (`graph.cypherOps.query`) completing (specifically, not anything _within_ the data flowing through the stream)
          *
          * Therefore, we aim to make the semantics of the completion signal as uniform as possible from
          * `graph.cypherOps.query` through to the `ResumeQuery`-generated Sources. To do so, is important that the stream
          * is structured such that there are few or no opportunities for a QueryContext to be produced without being
          * consumed. In particular, this means that the BroadcastHub used to  connect the Source to various Sinks (the
          * consumers) should buffer as little as possible.
          */
        completesWithStream.onComplete { status =>
          log.debug(
            s"SkipOptimizingActor finished execution of query (cleanly: ${status.isSuccess}) and will terminate: ${QueryFamily}"
          )
          decommission()
        }(ExecutionContexts.parasitic)
        mat
      }
      .runWith(BroadcastHub.sink(bufferSize = 1))(
        graph.materializer
      )
      .named("skip-optimizing-query-hub")
  }
  private var queryHub: Source[QueryContext, NotUsed] = startQuery()

  /** the last index produced by this actor: used to check whether a query can be resumed as requested
    */
  private var lastProducedIdx = -1L

  /** whether a consumer is currently attached to the [[queryHub]]. If so, additional [[ResumeQuery]] requests must be
    * rejected until the [[queryHub]] is available again
    */
  private var isCurrentlyStreaming = false

  /** extracts the 'skip' and 'limit', defaulting to 0 and None, respectively, from a [[Query.Return]].
    * @return Left(an error due to an invalid SKIP or LIMIT value) or Right(skip, limit) where skip is the number of
    *         rows to drop, and limit is Some(number of rows to keep) or None to keep all rows
    */
  private def extractSkipAndLimit(
    query: Return[_],
    context: QueryContext,
    parameters: Parameters
  ): Either[SkipOptimizationError.InvalidSkipLimit, (Long, Option[Long])] = {
    val skipVal = query.drop.map(_.eval(context)(graph.idProvider, parameters))
    val limitVal = query.take.map(_.eval(context)(graph.idProvider, parameters))
    for {
      skip <- skipVal match {
        case Some(Expr.Integer(skipCount)) => Right(skipCount)
        case None => Right(0L)
        case Some(expr) =>
          Left(
            SkipOptimizationError.InvalidSkipLimit(
              s"The query's SKIP clause had type ${expr.typ.pretty}, required ${Type.Integer.pretty}"
            )
          )
      }
      limit <- limitVal match {
        case Some(Expr.Integer(limitCount)) => Right(Some(limitCount))
        case None => Right(None)
        case Some(expr) =>
          Left(
            SkipOptimizationError.InvalidSkipLimit(
              s"The query's LIMIT clause had type ${expr.typ.pretty}, required ${Type.Integer.pretty}"
            )
          )
      }
    } yield skip -> limit
  }

  override def receive: Receive = {
    case ResumeQuery(
          query @ Return(QueryFamily, None, None, dropRule, takeRule, columns @ _),
          context,
          parameters,
          restartIfAppropriate,
          replyTo
        ) if dropRule.isDefined || takeRule.isDefined =>
      val responseMessage: Either[SkipOptimizationError, Source[QueryContext, NotUsed]] =
        extractSkipAndLimit(query, context, parameters).flatMap { case (skip, limit) =>
          // how many elements will need to be skipped to satisfy the provided SKIP predicate
          // eg: if lastProducedIdx is -1 (a fresh stream), and SKIP is 10: skipOffset = 10
          // if lastProducedIdx is 3 (we have emitted 4 elements: indices 0 thru 3) and SKIP is 4: skipOffset = 0
          val skipOffset = (skip - 1) - lastProducedIdx
          if (!isCurrentlyStreaming && (skipOffset >= 0 || restartIfAppropriate)) {
            isCurrentlyStreaming = true
            log.debug(
              s"SkipOptimizingActor received query eligible for pagination for atTime: $atTime. computed SKIP: $skip"
            )
            // apply the SKIP rule (resetting the queryHub if appropriate)
            val skippedStream: Source[QueryContext, NotUsed] =
              if (skipOffset > 0) {
                // actor is in a good state to resume query
                queryHub.drop(skipOffset)
              } else if (skipOffset == 0) {
                queryHub
              } else {
                // actor is in the wrong state to resume query, but has been allowed to restart the query
                log.info(
                  """Processing a ResumeQuery that requires resetting the
                     |SkipOptimizingActor's state. As restartIfAppropriate = true, the query will be restarted,
                     |replaying and dropping results up to the SKIP value provided in the latest query: {}""".stripMargin
                    .replace('\n', ' '),
                  query
                )
                queryHub = startQuery()
                queryHub.drop(skip)
              }
            // apply the LIMIT rule (updating lastProducedIdx and registering an unlock callback if appropriate)
            val skippedAndLimitedStream: Source[QueryContext, NotUsed] =
              limit match {
                case Some(limitNum) =>
                  lastProducedIdx = skip + limitNum - 1
                  skippedStream.take(limitNum).watchTermination() { (mat, completesWithStream) =>
                    completesWithStream.onComplete(_ => self ! UnlockStreaming)(ExecutionContexts.parasitic)
                    mat
                  }
                case None =>
                  /** there is no LIMIT clause on this query, so completing `skippedStream` will complete the `queryHub`
                    * for the whole [[QueryFamily]]. We could decommission() this actor right now, but the completion of
                    * `queryHub` will do so anyways. Rather than introduce a potential double-free / NPE type problem,
                    * we'll rely solely on the queryHub completion/cleanup callback as the single point-in-time decision
                    * to terminate the actor
                    */
                  skippedStream
              }
            Right(
              skippedAndLimitedStream
            )
          } else if (isCurrentlyStreaming) {
            Left(SkipOptimizationError.ReplayInProgress)
          } else {
            // Not currently streaming, not allowed to restart, and the offset indicates this actor's state is such that
            // it can't resume the query without restarting
            Left(SkipOptimizationError.SkipLimitMismatch)
          }
        }
      replyTo ! responseMessage
    case ResumeQuery(Return(QueryFamily, orderBy @ _, distinctBy @ _, None, None, _), _, _, _, replyTo) =>
      // This should be unreachable: the cypher runtime should only use SkipOptimizingActor with SKIP/LIMIT
      replyTo ! Left(SkipOptimizationError.UnsupportedProjection("no SKIP or LIMIT was specified"))
    case ResumeQuery(Return(QueryFamily, Some(orderBy @ _), distinctBy @ _, drop @ _, take @ _, _), _, _, _, replyTo) =>
      // This should be unreachable: the cypher runtime should only use SkipOptimizingActor with no ORDER BY
      replyTo ! Left(SkipOptimizationError.UnsupportedProjection("ORDER BY was specified"))
    case ResumeQuery(Return(QueryFamily, orderBy @ _, Some(distinctBy @ _), drop @ _, take @ _, _), _, _, _, replyTo) =>
      // This should be unreachable: the cypher runtime should only use SkipOptimizingActor with no DISTINCT
      replyTo ! Left(SkipOptimizationError.UnsupportedProjection("DISTINCT was specified"))
    case ResumeQuery(mismatchedReturn, _, _, _, replyTo) if mismatchedReturn.toReturn != QueryFamily =>
      // This should be unreachable: the cypher runtime should decide which SkipOptimizingActor to use by its queryfamily
      replyTo ! Left(SkipOptimizationError.QueryMismatch)
    case UnlockStreaming =>
      isCurrentlyStreaming = false

  }
}
object SkipOptimizingActor {

  /** Requests that the queryFamily be resumed, according to the projection rules in [[query]]. replyTo will receive in
    * response either a:
    *
    * Left(SkipOptimizationError) explaining why this actor was unable to process the query as requested
    * Right(Source[QueryContext, NotUsed]) which will produce results as requested by the provided [[query]].
    *
    * i.e., the type sent to `replyTo` is `Either[SkipOptimizationError, Source[QueryContext, NotUsed]]`
    *
    * @param query                 the query to run using this SkipOptimizingActor. This must match this actor's
    *                              QueryFamily, and must specify at least one of {SKIP, LIMIT}, but must not specify
    *                              either of {ORDER BY, DISTINCT}
    * @param context               the QueryContext under which to evaluate the provided SKIP/LIMIT rules
    * @param parameters            the parameters with which to evaluate the provided SKIP/LIMIT rules
    * @param restartIfAppropriate  if true, requests that the actor restart its query in case the current state of the
    *                              QueryFamily results stream is incompatible with the requested query. If false,
    *                              the actor will reject the query instead. For example, if the last row this actor
    *                              facilitated was the 40th row, and this [[ResumeQuery]] requests rows 35-45, if
    *                              `restartIsAppropriate == true`, this actor will replay the query to row 34, then
    *                              forward the query for rows 35-45. If `restartIsAppropraite == false`, instead the
    *                              actor will reply with a [[SkipOptimizationError.SkipLimitMismatch]]
    * @param replyTo               the ActorRef to which results and/or errors will be `tell`-ed
    */
  case class ResumeQuery(
    query: Query.Return[Location.Anywhere],
    context: QueryContext,
    parameters: Parameters,
    restartIfAppropriate: Boolean,
    replyTo: ActorRef
  )

  /** Message instructing the [[SkipOptimizingActor]] to unlock streaming, allowing new [[ResumeQuery]] requests.
    * This should only be sent from code in [[SkipOptimizingActor]] (eg, on the completion hook for
    * [[ResumeQuery]]-generated Sources)
    */
  case object UnlockStreaming

  /** Reasons a [[SkipOptimizingActor]] might not produce a results stream
    * @param msg a message explaining the cause of the error
    * @param retriable true when the error was a result of actor state (ie, the same request performed at a different
    *                  time might succeed), false when the error was a result of the request itself (eg, the request
    *                  was for a query this actor was not constructed to handle). Errors with retriable = false
    *                  can be thought of as "the caller's fault" (like an HTTP 400). Errors with retriable = true
    *                  can be thought of as either "the protocol's fault" (like an HTTP 403)
    */
  sealed abstract class SkipOptimizationError(val msg: String, val retriable: Boolean)
  object SkipOptimizationError {
    case object ReplayInProgress
        extends SkipOptimizationError(
          "Requested query is currently streaming results to another, try again later.",
          retriable = true
        )

    case object SkipLimitMismatch
        extends SkipOptimizationError(
          "Requested query's SKIP and/or LIMIT does not match the current state of this SkipOptimizingActor",
          retriable = true
        )

    case object QueryMismatch
        extends SkipOptimizationError(
          "Requested query does not match the query this SkipOptimizingActor tracks",
          retriable = false
        )

    case class UnsupportedProjection(invalidRuleDescription: String)
        extends SkipOptimizationError(
          s"Requested query specifies a projection not supported by a SkipOptimizingActor: ${invalidRuleDescription}",
          retriable = false
        )

    case class InvalidSkipLimit(override val msg: String) extends SkipOptimizationError(msg, retriable = false)
  }
}
