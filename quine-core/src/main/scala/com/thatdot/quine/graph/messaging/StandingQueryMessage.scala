package com.thatdot.quine.graph.messaging

import com.google.common.hash.Hashing.murmur3_128

import com.thatdot.quine.graph.{
  StandingQuery,
  StandingQueryId,
  StandingQueryPartId,
  StandingQueryPattern,
  StandingQueryResult,
  cypher
}
import com.thatdot.quine.model.{DomainGraphBranch, GenericEdge, QuineId, QuineIdProvider, QuineValue}
import com.thatdot.quine.util.Hashable

/** Top-level type of all SQ-related messages relayed through the graph
  *
  * Used in [[DomainNodeIndexBehavior]] and [[CypherStandingBehavior]].
  */
sealed abstract class StandingQueryMessage extends QuineMessage

object StandingQueryMessage {

  /** == Cypher standing queries == */
  sealed abstract class CypherStandingQueryCommand extends StandingQueryMessage

  final case class ResultId(uuid: java.util.UUID) extends AnyVal
  object ResultId {
    def fresh(): ResultId = ResultId(java.util.UUID.randomUUID())
    def fromQuineId(qid: QuineId): ResultId =
      ResultId(Hashable[Array[Byte]].hashToUuid(murmur3_128(), qid.array))
  }

  sealed abstract class CypherSubscriber {
    val globalId: StandingQueryId
  }
  object CypherSubscriber {
    final case class QuerySubscriber(
      onNode: QuineId,
      globalId: StandingQueryId,
      queryId: StandingQueryPartId
    ) extends CypherSubscriber

    final case class GlobalSubscriber(
      globalId: StandingQueryId
    ) extends CypherSubscriber
  }

  /** @param subscriber node to which results are sent
    * @param query what to match
    */
  final case class CreateCypherSubscription(
    subscriber: CypherSubscriber,
    query: cypher.StandingQuery
  ) extends CypherStandingQueryCommand

  /** @param originalSubscriber node which had created a subscription
    * @param queryId ID of the query passed in when the subscription was made
    */
  final case class CancelCypherSubscription(
    originalSubscriber: CypherSubscriber,
    queryId: StandingQueryPartId
  ) extends CypherStandingQueryCommand

  /** @param from node delivering the result
    * @param queryId ID of the query passed in when the subscription was made
    * @param forQueryId when delivering results to another query, what is that query's ID
    * @param resultId fresh ID that can be used to invalidate the results
    * @param result assumulated value
    */
  final case class NewCypherResult(
    from: QuineId,
    queryId: StandingQueryPartId,
    globalId: StandingQueryId,
    forQueryId: Option[StandingQueryPartId],
    resultId: ResultId,
    result: cypher.QueryContext
  ) extends CypherStandingQueryCommand
      with SqResultLike {
    def isPositive = true

    def standingQueryResult(sq: StandingQuery, idProvider: QuineIdProvider): StandingQueryResult = {
      val qvResult: Map[String, QuineValue] =
        result.environment.map { case (col, value) =>
          col.name -> cypher.Expr.toQuineValue(value)
        }.toMap
      StandingQueryResult(isPositiveMatch = isPositive, resultId = resultId, data = qvResult)
    }
  }

  /** @param from node delivering the result
    * @param queryId ID of the query passed in when the subscription was made
    * @param forQueryId when delivering results to another query, what is that query's ID
    * @param resultId ID that was passed with the original results
    */
  final case class CancelCypherResult(
    from: QuineId,
    queryId: StandingQueryPartId,
    globalId: StandingQueryId,
    forQueryId: Option[StandingQueryPartId],
    resultId: ResultId
  ) extends CypherStandingQueryCommand
      with SqResultLike {
    def isPositive = false

    def standingQueryResult(sq: StandingQuery, idProvider: QuineIdProvider): StandingQueryResult =
      StandingQueryResult(isPositiveMatch = isPositive, resultId = resultId, data = Map.empty)
  }

  /** == DomainNodeIndexBehavior  == */
  sealed abstract class DomainNodeSubscriptionCommand extends StandingQueryMessage

  final case class CreateDomainNodeSubscription(
    testBranch: DomainGraphBranch,
    assumedEdge: Option[(GenericEdge, DomainGraphBranch)],
    replyTo: Either[QuineId, StandingQueryId],
    relatedQueries: Set[StandingQueryId]
  ) extends DomainNodeSubscriptionCommand

  final case class DomainNodeSubscriptionResult(
    from: QuineId,
    testBranch: DomainGraphBranch,
    assumedEdge: Option[(GenericEdge, DomainGraphBranch)],
    result: Boolean
  ) extends DomainNodeSubscriptionCommand
      with SqResultLike {
    def isPositive = result

    def standingQueryResult(sq: StandingQuery, idProvider: QuineIdProvider): StandingQueryResult = {
      val (formatAsString, aliasedAs) = sq.query match {
        case pat: StandingQueryPattern.Branch =>
          pat.formatReturnAsStr -> pat.aliasReturnAs.name
        case _: StandingQueryPattern.SqV4 =>
          throw new RuntimeException(s"Received branch result $this for SQv4 query $sq")
      }
      StandingQueryResult(isPositive, from, formatAsString, aliasedAs)(idProvider)
    }
  }

  final case class CancelDomainNodeSubscription(
    testBranch: DomainGraphBranch,
    assumedEdge: Option[(GenericEdge, DomainGraphBranch)],
    alreadyCancelledSubscriber: QuineId
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
  sealed trait SqResultLike extends StandingQueryMessage {

    /** Is this result reporting a new match (as opposed to a cancellation)? */
    def isPositive: Boolean

    def standingQueryResult(sq: StandingQuery, idProvider: QuineIdProvider): StandingQueryResult
  }
}
