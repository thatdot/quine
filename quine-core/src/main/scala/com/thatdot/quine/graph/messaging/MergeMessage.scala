package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import com.thatdot.quine.model.{HalfEdge, Properties, QuineId}

/** Top-level type of all merge-related messages relayed through the graph
  *
  * Used in [[MergeBehavior]].
  */
sealed abstract class MergeMessage extends QuineMessage
object MergeMessage {
  sealed abstract class MergeCommand extends MergeMessage

  final case class MergeIntoNode(other: QuineId, replyTo: QuineRef)
      extends MergeCommand
      with AskableQuineMessage[Future[BaseMessage.Done.type]]
  final case class MergeAdd(props: Properties, edges: Set[HalfEdge], context: MergeAddContext) extends MergeCommand
  final case class MergeAddContext(mergedFrom: QuineId, mergedInto: QuineId)
  final case class RedirectEdges(from: QuineId, to: QuineId) extends MergeCommand
  final case class DoNotForward(msg: QuineMessage) extends MergeMessage
}
