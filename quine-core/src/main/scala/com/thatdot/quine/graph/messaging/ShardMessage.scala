package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.{InMemoryNodeLimit, NamespaceId}
import com.thatdot.quine.model.Milliseconds

/** Top-level type of all shard-related messages relayed through the graph
  *
  * Used mostly in graph-shard protocols.
  */
sealed abstract class ShardMessage extends QuineMessage

object ShardMessage {

  /** Transition a shard to shutting down all of its nodes.
    *
    * It is OK to send this multiple times - the shard will just reply with its
    * updated shutdown progress.
    *
    * @param replyTo where to send statistics about shutdown progress
    */
  final case class InitiateShardShutdown(replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[ShardShutdownProgress]

  /** Result of calling shutdown on GraphShardActors */
  final case class ShardShutdownProgress(remainingNodeActorCount: Int) extends ShardMessage

  /** Cancel any in-progress shard shutdowns started by InitiateShardShutdown. */
  final case class CancelShardShutdown(replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[BaseMessage.Done.type]

  /** Instruct the shard to forcibly stop all of its nodes */
  case object RemoveNodes extends ShardMessage

  case class PurgeNode(namespace: NamespaceId, qid: QuineId, replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[Future[BaseMessage.Done.type]]

  /** Instruct the shard to forcibly remove some of its nodes
    *
    * @param predicate how to pick the nodes to remove
    * @param replyTo where to send a signal that the operation is done
    */
  final case class RemoveNodesIf(namespace: NamespaceId, predicate: LocalPredicate, replyTo: QuineRef)
      extends QuineMessage
      with AskableQuineMessage[BaseMessage.Done.type]

  final case class LocalPredicate(predicate: SpaceTimeQuineId => Boolean)

  /** Request the shard sleep a node. No guarantees. For testing. */
  final case class RequestNodeSleep(idToSleep: SpaceTimeQuineId, replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[BaseMessage.Done.type]

  /** Send to a shard to ask for some sample of awake nodes
    *
    * @param limit max number of nodes to send back (none means no maximum, so all awake nodes)
    * @param atTime historical moment to sample
    * @param replyTo where to send the result nodes
    */
  final case class SampleAwakeNodes(
    namespace: NamespaceId,
    limit: Option[Int],
    atTime: Option[Milliseconds],
    replyTo: QuineRef,
  ) extends ShardMessage
      with AskableQuineMessage[Source[AwakeNode, NotUsed]]

  final case class AwakeNode(quineId: QuineId) extends ShardMessage

  /** Report stats about nodes managed by a shard
    *
    * @param awake nodes with active actors backing them
    * @param askedToSleep nodes asked to sleep, but who haven't confirmed
    * @param sleeping nodes asked to sleep, who have confirmed
    */
  final case class ShardStats(
    awake: Int,
    askedToSleep: Int,
    sleeping: Int,
  ) extends ShardMessage {
    def goingToSleep: Int = askedToSleep + sleeping
    def total: Int = awake + goingToSleep
  }

  /** Query a shard's in-memory limits
    *
    * @param replyTo where to deliver the response
    */
  final case class GetInMemoryLimits(replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[CurrentInMemoryLimits]

  /** Try to adjust the in-memory limits of a shard, returning whether the resize was successful.
    *
    * TODO: A resize can currently fail if the shard did not previously have any in-memory limit.
    * We could loosen this constraint, but it requires choosing an arbitrary order to expiry the
    * existing shard elements.
    *
    * @param newLimits updated in-memory soft/hard limits
    * @param replyTo where to deliver the result
    */
  final case class UpdateInMemoryLimits(
    newLimits: InMemoryNodeLimit,
    replyTo: QuineRef,
  ) extends ShardMessage
      with AskableQuineMessage[CurrentInMemoryLimits]

  /** Shard's in-memory limits
    *
    * @param limits in-memory soft/hard limits
    */
  final case class CurrentInMemoryLimits(limits: Option[InMemoryNodeLimit]) extends ShardMessage

  final case class CreateNamespace(namespaceId: NamespaceId, replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[NamespaceChangeResult]

  final case class DeleteNamespace(namespaceId: NamespaceId, replyTo: QuineRef)
      extends ShardMessage
      with AskableQuineMessage[NamespaceChangeResult]

  final case class NamespaceChangeResult(didHaveEffect: Boolean) extends ShardMessage
}
