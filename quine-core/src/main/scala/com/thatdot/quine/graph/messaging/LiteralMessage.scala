package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.{EventTime, NodeChangeEvent}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineId, QuineValue}

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

  final case class MergeIntoNodeCommand(
    otherQid: QuineId,
    replyTo: QuineRef
  ) extends LiteralCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  /** Return the ID of the node queried. Used to resolve merged nodes. */
  final case class GetNodeId(replyTo: QuineRef) extends LiteralCommand with AskableQuineMessage[QuineIdResponse]

  final case class QuineIdResponse(qid: QuineId) extends LiteralMessage

  final case class LogInternalState(replyTo: QuineRef)
      extends LiteralCommand
      with AskableQuineMessage[Future[NodeInternalState]]

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

  /** Relays a complete, non-authoritative snapshot of node-internal state, eg, for logging.
    * ONLY FOR DEBUGGING!
    */
  final case class NodeInternalState(
    properties: Map[Symbol, String],
    edges: Set[HalfEdge],
    forwardTo: Option[QuineId],
    mergedIntoHere: Set[QuineId],
    latestUpdateMillisAfterSnapshot: Option[EventTime],
    subscribers: Option[String], // TODO make this string more informative
    subscriptions: Option[String], // TODO: make this string more informative
    cypherStandingQueryStates: Vector[LocallyRegisteredStandingQuery],
    journal: Vector[NodeChangeEvent]
  ) extends LiteralMessage

  final case class LocallyRegisteredStandingQuery(
    id: String,
    globalId: String,
    subscribers: Set[String],
    state: String
  )
}
