package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{EventTime, GraphNodeHashCode, NodeEvent}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, Milliseconds, PropertyValue, QuineId, QuineValue}

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
    replyTo: QuineRef
  ) extends LiteralCommand
      with AskableQuineMessage[Source[HalfEdgeMessage, NotUsed]]
  final case class HalfEdgeMessage(halfEdge: HalfEdge) extends LiteralMessage

  final case class AddHalfEdgeCommand(
    halfEdge: HalfEdge,
    replyTo: QuineRef
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class RemoveHalfEdgeCommand(
    halfEdge: HalfEdge,
    replyTo: QuineRef
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
    value: Either[(Symbol, PropertyValue), HalfEdge]
  ) extends LiteralMessage

  final case class SetPropertyCommand(
    key: Symbol,
    value: PropertyValue,
    replyTo: QuineRef
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class RemovePropertyCommand(
    key: Symbol,
    replyTo: QuineRef
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

  /** Request the current results of the standing query matches on this node. */
  final case class GetSqState(replyTo: QuineRef) extends LiteralCommand with AskableQuineMessage[SqStateResults]

  /** A single result. Could be for an incoming subscriber or an outgoing subscription. */
  final case class SqStateResult(dgnId: DomainGraphNodeId, qid: QuineId, lastResult: Option[Boolean])

  /** Payload to report on the current results of the standing query matches on this node. */
  final case class SqStateResults(subscribers: List[SqStateResult], subscriptions: List[SqStateResult])
      extends QuineMessage

  /** IncrementCounter Procedure */
  final case class IncrementProperty(propertyKey: Symbol, incrementAmount: Long, replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[IncrementProperty.Result]
  case object IncrementProperty {

    sealed abstract class Result extends LiteralMessage

    final case class Failed(valueFound: QuineValue) extends Result
    final case class Success(newCount: Long) extends Result
  }

  final case class SetLabels(labels: Set[Symbol], replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  final case class DgnLocalEventIndexSummary(
    propIdx: Map[String, DomainGraphNodeId],
    edgeIdx: Map[String, DomainGraphNodeId],
    anyEdgeIdx: List[DomainGraphNodeId]
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
    dgnLocalEventIndex: DgnLocalEventIndexSummary,
    multipleValuesStandingQueryStates: Vector[LocallyRegisteredStandingQuery],
    journal: Set[NodeEvent.WithTime],
    graphNodeHashCode: Long
  ) extends LiteralMessage

  final case class LocallyRegisteredStandingQuery(
    id: String,
    globalId: String,
    subscribers: Set[String],
    state: String
  )
}
