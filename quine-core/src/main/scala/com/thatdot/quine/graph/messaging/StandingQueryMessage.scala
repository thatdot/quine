package com.thatdot.quine.graph.messaging

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.{
  MultipleValuesStandingQueryPartId,
  StandingQueryId,
  StandingQueryInfo,
  StandingQueryPattern,
  StandingQueryResult,
  cypher,
}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.MonadHelpers._

/** Top-level type of all SQ-related messages relayed through the graph
  *
  * Used in [[DomainNodeIndexBehavior]] and [[CypherStandingBehavior]].
  */
sealed abstract class StandingQueryMessage extends QuineMessage

object StandingQueryMessage {

  /** == Cypher standing queries == */
  sealed abstract class MultipleValuesStandingQueryCommand extends StandingQueryMessage

  sealed abstract class MultipleValuesStandingQuerySubscriber {
    val globalId: StandingQueryId
    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String
  }
  object MultipleValuesStandingQuerySubscriber {

    /** The subscriber is another node -- results will be sent as complete sets via direct message
      */
    final case class NodeSubscriber(
      subscribingNode: QuineId,
      globalId: StandingQueryId,
      queryId: MultipleValuesStandingQueryPartId,
    ) extends MultipleValuesStandingQuerySubscriber {
      def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String =
        s"${this.getClass.getSimpleName}(${subscribingNode.pretty}, $globalId, $queryId)"
    }

    /** The subscriber is the end-user (or, if you prefer, the SQ's results queue). Results will be
      * statefully accumulated and sent as diffs (cancellations or new results) to the result queue via a
      * [[com.thatdot.quine.graph.cypher.MultipleValuesResultsReporter]]
      */
    final case class GlobalSubscriber(
      globalId: StandingQueryId,
    ) extends MultipleValuesStandingQuerySubscriber {
      def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = this.toString
    }
  }

  /** @param subscriber node to which results are sent
    * @param query what to match
    */
  final case class CreateMultipleValuesStandingQuerySubscription(
    subscriber: MultipleValuesStandingQuerySubscriber,
    query: MultipleValuesStandingQuery,
  ) extends MultipleValuesStandingQueryCommand

  /** @param originalSubscriber node which had created a subscription
    * @param queryId ID of the query passed in when the subscription was made
    */
  final case class CancelMultipleValuesSubscription(
    originalSubscriber: MultipleValuesStandingQuerySubscriber,
    queryId: MultipleValuesStandingQueryPartId,
  ) extends MultipleValuesStandingQueryCommand

  /** Internal (node to node) representation of a standing query result group
    * @param from node delivering the result
    * @param queryPartId ID of the query passed in when the subscription was made
    * @param globalId ID of the original entire standing query issued by the user
    * @param forQueryPartId when delivering results to another query, what is that query's ID
    *                       TODO consider splitting this type based on forQueryPartIds's Some/None to avoid a `get`
    * @param resultGroup The accumulated rows that represent the results of one stage of a standing query state
    *                    performing it's operations.
    */
  final case class NewMultipleValuesStateResult(
    from: QuineId,
    queryPartId: MultipleValuesStandingQueryPartId,
    globalId: StandingQueryId,
    forQueryPartId: Option[MultipleValuesStandingQueryPartId],
    resultGroup: Seq[cypher.QueryContext],
  ) extends MultipleValuesStandingQueryCommand {
    def isPositive = true

    def standingQueryResults(sq: StandingQueryInfo, idProvider: QuineIdProvider): Seq[StandingQueryResult] =
      resultGroup.map { r =>
        val qvResult = r.environment.map { case (col, value) =>
          col.name -> cypher.Expr.toQuineValue(value).getOrThrow
        }
        StandingQueryResult(isPositiveMatch = isPositive, data = qvResult)
      }

    def pretty(implicit idProvider: QuineIdProvider): String =
      s"${this.getClass.getSimpleName}(${from.pretty}, $queryPartId, $globalId, $resultGroup"
  }

  /** == DomainNodeIndexBehavior  == */
  sealed abstract class DomainNodeSubscriptionCommand extends StandingQueryMessage {
    val dgnId: DomainGraphNodeId
  }

  final case class CreateDomainNodeSubscription(
    dgnId: DomainGraphNodeId,
    replyTo: Either[QuineId, StandingQueryId],
    relatedQueries: Set[StandingQueryId],
  ) extends DomainNodeSubscriptionCommand

  final case class DomainNodeSubscriptionResult(
    from: QuineId,
    dgnId: DomainGraphNodeId,
    result: Boolean,
  ) extends DomainNodeSubscriptionCommand
      with SqResultLike {

    def isPositive: Boolean = result

    def standingQueryResults(sq: StandingQueryInfo, idProvider: QuineIdProvider)(implicit
      logConfig: LogConfig,
    ): Seq[StandingQueryResult] = {
      val (formatAsString, aliasedAs) = sq.queryPattern match {
        case pat: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
          pat.formatReturnAsStr -> pat.aliasReturnAs.name
        case _: StandingQueryPattern.MultipleValuesQueryPattern =>
          throw new RuntimeException(s"Received branch result $this for MultipleValues query $sq")
        case _: StandingQueryPattern.QuinePatternQueryPattern => ???
      }
      StandingQueryResult(isPositive, from, formatAsString, aliasedAs)(idProvider) :: Nil
    }
  }

  final case class CancelDomainNodeSubscription(
    dgnId: DomainGraphNodeId,
    alreadyCancelledSubscriber: QuineId,
  ) extends DomainNodeSubscriptionCommand

  sealed abstract class UpdateStandingQueriesCommand extends StandingQueryMessage

  /** Sent to a node to request that it refresh its list of universal standing
    *
    * @note nodes will _not_ be woken up to process this message
    */
  case object UpdateStandingQueriesNoWake extends UpdateStandingQueriesCommand

  /** Sent to a node to request that it refresh its list of universal standing
    *
    * @note nodes will be woken up to process this message
    */
  final case class UpdateStandingQueriesWake(replyTo: QuineRef)
      extends UpdateStandingQueriesCommand
      with AskableQuineMessage[BaseMessage.Done.type]

  // messages that can be mapped to a standing query result/cancellation
  // TODO remove this, it's no longer used
  sealed trait SqResultLike extends StandingQueryMessage {

    /** Is this result reporting a new match (as opposed to a cancellation)? */
    def isPositive: Boolean

    def standingQueryResults(sq: StandingQueryInfo, idProvider: QuineIdProvider)(implicit
      logConfig: LogConfig,
    ): Seq[StandingQueryResult]
  }
}
