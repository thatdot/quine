package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.LiteralMessage.AddToAtomicResult.Aux
import com.thatdot.quine.graph.{EventTime, GraphNodeHashCode, NodeEvent, StandingQueryId}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, Milliseconds, PropertyValue, QuineValue}

/** Top-level type of all literal-related messages relayed through the graph
  *
  * Used in [[LiteralBehavior]].
  */
sealed abstract class LiteralMessage extends QuineMessage

object LiteralMessage {
  sealed abstract class LiteralCommand extends LiteralMessage

  final case class GetHalfEdgesCommand(
    withType: Option[Symbol],
    withDirection: Option[EdgeDirection],
    withId: Option[QuineId],
    withLimit: Option[Int],
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Source[HalfEdgeMessage, NotUsed]]

  final case class GetHalfEdgesFilteredCommand(
    edgeTypes: Set[Symbol],
    directions: Set[EdgeDirection],
    otherIds: Set[QuineId],
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Source[HalfEdgeMessage, NotUsed]]

  final case class ValidateAndReturnMissingHalfEdgesCommand(
    expectedEdges: Set[HalfEdge],
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[MissingHalfEdgesResponse]

  final case class MissingHalfEdgesResponse(missingEdges: Set[HalfEdge]) extends LiteralMessage

  final case class HalfEdgeMessage(halfEdge: HalfEdge) extends LiteralMessage

  final case class AddHalfEdgeCommand(
    halfEdge: HalfEdge,
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class RemoveHalfEdgeCommand(
    halfEdge: HalfEdge,
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class GetPropertiesCommand(replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Source[PropertyMessage, NotUsed]]
  case class PropertyMessage(value: Either[(Symbol, PropertyValue), Symbol]) extends LiteralMessage

  final case class GetPropertiesAndEdges(replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Source[PropertyOrEdgeMessage, NotUsed]]

  final case class PropertyOrEdgeMessage(
    value: Either[(Symbol, PropertyValue), HalfEdge],
  ) extends LiteralMessage

  final case class SetPropertyCommand(
    key: Symbol,
    value: PropertyValue,
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class RemovePropertyCommand(
    key: Symbol,
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class DeleteNodeCommand(deleteEdges: Boolean, replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Future[DeleteNodeCommand.Result]]
  case object DeleteNodeCommand {

    sealed abstract class Result extends LiteralMessage

    final case class Failed(edgeCount: Int) extends Result
    case object Success extends Result
  }

  final case class QuineIdResponse(qid: QuineId) extends LiteralMessage

  final case class LogInternalState(replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Future[NodeInternalState]]

  final case class GetNodeHashCode(replyTo: QuineRef) extends LiteralCommand with AskableQuineMessage[GraphNodeHashCode]

  /** Request the journal of events for this node.
    *
    * The reply streams: a node's journal has no bound on its length, so it is delivered one event at
    * a time rather than assembled into a single message.
    *
    * Both bounds are read straight into the persistor query. They say which slice of the journal to
    * return and nothing about which moment the node is being asked about, so a bound may name a
    * moment the node has not reached: there is simply nothing recorded after the present to return.
    *
    * @param startingAt earliest millisecond to report, inclusive, or `None` for the node's whole history
    * @param endingAt latest millisecond to report, inclusive, or `None` for everything recorded
    */
  final case class GetJournal(
    startingAt: Option[Milliseconds],
    endingAt: Option[Milliseconds],
    replyTo: QuineRef,
  ) extends LiteralCommand
      with AskableQuineMessage[Source[JournalEntry, NotUsed]]

  /** Check if a node is "interesting" (has at least one property (including labels) or edge).
    * Used to filter out empty nodes from scan results.
    */
  final case class CheckNodeIsInteresting(replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[NodeIsInteresting]

  final case class NodeIsInteresting(isInteresting: Boolean) extends LiteralMessage

  /** Request the current results of the standing query matches on this node. */
  final case class GetSqState(replyTo: QuineRef) extends LiteralCommand with AskableQuineMessage[SqStateResults]

  /** A single result. Could be for an incoming subscriber or an outgoing subscription. */
  /** One subscriber to one DistinctId pattern rooted on this node.
    *
    * @param dgnId the pattern, rooted here, that the subscriber asked about
    * @param subscriberNode set when the subscriber is another node; then `subscriberQuery` is empty
    * @param subscriberQuery set when the subscriber is the standing query itself; then `subscriberNode` is empty
    * @param forQueries the standing queries this subscriber depends on this node for. What decides whether the
    *                   subscription is still wanted, so it is reported rather than summarised away.
    * @param lastResult the answer last reported for `dgnId`
    */
  final case class DistinctIdSubscriberState(
    dgnId: DomainGraphNodeId,
    subscriberNode: Option[QuineId],
    subscriberQuery: Option[StandingQueryId],
    forQueries: List[StandingQueryId],
    lastResult: Option[Boolean],
  )

  /** One answer a peer gave this node about one child pattern.
    *
    * @param dgnId the child pattern the peer was asked about
    * @param peer the node that was asked
    * @param forQueries the standing queries the ask was made on behalf of. The peer holds the same set, and both
    *                   ends retire the subscription by the same test, so this is state in its own right.
    * @param answer the peer's last answer, or None while the question is out
    */
  final case class DistinctIdIndexState(
    dgnId: DomainGraphNodeId,
    peer: QuineId,
    forQueries: List[StandingQueryId],
    answer: Option[Boolean],
  )

  /** One child-to-parent link in the index used to route a peer's answer to the patterns that care about it. */
  final case class DistinctIdParentLink(childDgnId: DomainGraphNodeId, parentDgnId: DomainGraphNodeId)

  /** Every piece of DistinctId standing query state a node holds.
    *
    * @param subscribers who is subscribed to patterns rooted here, and what they were told
    * @param subscriptions what peers have told this node about the children of those patterns
    * @param parentIndex child-to-parent links. Unlike the two above this is derived: it is rebuilt at every wake
    *                    from the registry and the index, and the rebuild may legitimately be more complete than
    *                    what was there before. Reported for inspection; not something a restore must reproduce.
    */
  final case class SqStateResults(
    subscribers: List[DistinctIdSubscriberState],
    subscriptions: List[DistinctIdIndexState],
    parentIndex: List[DistinctIdParentLink],
  ) extends QuineMessage

  /** One event from a node's journal, as streamed in reply to [[GetJournal]]. */
  final case class JournalEntry(event: NodeEvent.WithTime[NodeEvent]) extends LiteralMessage

  /** IncrementCounter Procedure */
  @deprecated("Use AddToAtomic variants instead for consistency across types", "Feb 2023")
  final case class IncrementProperty(propertyKey: Symbol, incrementAmount: Long, replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[IncrementProperty.Result]
  case object IncrementProperty {

    sealed abstract class Result extends LiteralMessage

    final case class Failed(valueFound: QuineValue) extends Result
    final case class Success(newCount: Long) extends Result
  }

  /** @tparam T a QuineValue type that is "addable"
    *           TODO we can add a constraint that there is a `Monoid[T.jvmType]` and use that instance to implement
    *             the behavior for these messages, rather than copy/pasting in LiteralCommandBehavior
    *           TODO add instances for String, Map, etc. See [[AddToFloat]] and [[AddToInt]] for the pattern of cypher
    *             procedures making this functionality available to users
    *
    * Invariant: AddToAtomic[T] will always respond with either AddToAtomicResult.Failed or an AddToAtomicResult.Aux[T]
    */
  sealed trait AddToAtomic[T <: QuineValue] extends LiteralCommand with AskableQuineMessage[AddToAtomicResult] {
    def propertyKey: Symbol
    def addThis: T

    def success(result: T): AddToAtomicResult.Aux[T]
    def failure(currentVal: QuineValue): AddToAtomicResult.Failed = AddToAtomicResult.Failed(currentVal)
  }
  object AddToAtomic {
    final case class Int(propertyKey: Symbol, addThis: QuineValue.Integer, replyTo: QuineRef)
        extends AddToAtomic[QuineValue.Integer] {
      def success(result: QuineValue.Integer): AddToAtomicResult.SuccessInt = AddToAtomicResult.SuccessInt(result)
    }

    final case class Float(propertyKey: Symbol, addThis: QuineValue.Floating, replyTo: QuineRef)
        extends AddToAtomic[QuineValue.Floating] {
      def success(result: QuineValue.Floating): AddToAtomicResult.SuccessFloat = AddToAtomicResult.SuccessFloat(result)
    }

    final case class Set(propertyKey: Symbol, addThis: QuineValue.List, replyTo: QuineRef)
        extends AddToAtomic[QuineValue.List] {
      def success(result: QuineValue.List): Aux[QuineValue.List] = AddToAtomicResult.SuccessList(result)
    }

  }
  sealed trait AddToAtomicResult extends LiteralMessage {
    type T <: QuineValue
    def valueFound: T
  }
  object AddToAtomicResult {
    type Aux[A <: QuineValue] = AddToAtomicResult { type T = A }
    final case class Failed(override val valueFound: QuineValue) extends AddToAtomicResult { type T = QuineValue }
    final case class SuccessInt(override val valueFound: QuineValue.Integer) extends AddToAtomicResult {
      type T = QuineValue.Integer
    }
    final case class SuccessFloat(override val valueFound: QuineValue.Floating) extends AddToAtomicResult {
      type T = QuineValue.Floating
    }
    final case class SuccessList(override val valueFound: QuineValue.List) extends AddToAtomicResult {
      type T = QuineValue.List
    }
  }

  final case class SetLabels(labels: Set[Symbol], replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  /** Debug (non-authoritative) summary of the WatchableEventIndex entries that relate to DGN queries
    *
    * List is used for its relatively nice default toString
    *
    * INV: [[anyEdgeIdx]] must be distinct
    */
  final case class DgnWatchableEventIndexSummary(
    propIdx: Map[String, List[DomainGraphNodeId]],
    edgeIdx: Map[String, List[DomainGraphNodeId]],
    anyEdgeIdx: List[DomainGraphNodeId],
  ) extends QuineMessage

  /** Relays a complete, non-authoritative snapshot of node-internal state, eg, for logging.
    * ONLY FOR DEBUGGING!
    */
  final case class NodeInternalState(
    atTime: Option[Milliseconds],
    properties: Map[Symbol, String],
    edges: Set[HalfEdge],
    latestUpdateMillisAfterSnapshot: Option[EventTime],
    subscribers: List[String], // TODO make this string more informative
    subscriptions: List[String], // TODO: make this string more informative
    sqStateResults: SqStateResults,
    dgnWatchableEventIndex: DgnWatchableEventIndexSummary,
    multipleValuesStandingQueryStates: Vector[LocallyRegisteredStandingQuery],
    journal: Set[NodeEvent.WithTime[NodeEvent]],
    graphNodeHashCode: Long,
  ) extends LiteralMessage

  final case class LocallyRegisteredStandingQuery(
    id: String,
    globalId: String,
    subscribers: Set[String],
    state: String,
  )
}
