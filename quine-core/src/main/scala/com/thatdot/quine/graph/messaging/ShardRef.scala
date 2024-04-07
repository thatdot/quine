package com.thatdot.quine.graph.messaging

/** Reference to a [[GraphShardActor]]
  *
  * Similar to [[ActorRef]], but carries along some information used for relaying messages to the
  * nodes for which the shard is responsible.
  */
abstract class ShardRef {

  /** Reference that can be used to send a message to the shard actor */
  def quineRef: WrappedActorRef

  /** ID of the shard (unique within the logical graph) */
  def shardId: Int

  /** Whether this is a local (true) or remote (false) ShardRef */
  def isLocal: Boolean
}
