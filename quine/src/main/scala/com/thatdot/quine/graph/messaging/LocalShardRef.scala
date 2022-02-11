package com.thatdot.quine.graph.messaging

import scala.collection.concurrent

import akka.actor.ActorRef

import com.thatdot.quine.graph.GraphShardActor.NodeState

/** Actor reference to a local [[GraphShardActor]]
  *
  * @param localShard the shard actor reference to the [[GraphShardActor]]
  * @param shardId index of the shard in the graph
  * @param nodesMap nodes awake on the shard (which only the shard should modify - keep `private`!!)
  */
final class LocalShardRef(
  val localRef: ActorRef,
  val shardId: Int,
  nodesMap: concurrent.Map[QuineIdAtTime, NodeState]
) extends ShardRef {
  val quineRef: WrappedActorRef = WrappedActorRef(localRef)

  override def toString: String = s"LocalShardRef($localRef)"

  /** Apply an action with an [[ActorRef]] if/while the node is awake
    *
    * It is tempting to think of sending a message to a node backed by a local actor as being as
    * simple as using the [[QuineIdAtTime]] to lookup an [[ActorRef]] and telling that actor. Things
    * are more complicated due to the potential for a race between the actor being shutdown and a
    * message being sent to it at the same time. Specifically, we might lookup an [[ActorRef]] but,
    * before we have time to use it, the node shuts down. The only way to solve this is with a
    * lock that guarantees that the node is "alive". This function handles the locking under the
    * hood - if the callback is called, the actor is guaranteed to be awake and will remain awake
    * at least until the callback returns.
    *
    * @param id which node
    * @param withActorRef if the node is awake, apply this action and ensuring the node stays awake
    * @return if the action could be executed (else the node is sleeping - see the node lifecycle)
    */
  def withLiveActorRef(id: QuineIdAtTime, withActorRef: ActorRef => Unit): Boolean =
    nodesMap.get(id) match {
      case Some(NodeState(_, actorRef, actorRefLock, _)) =>
        val stamp = actorRefLock.tryReadLock()
        val gotReadLock = stamp != 0L
        if (gotReadLock) {
          try withActorRef(actorRef)
          finally actorRefLock.unlockRead(stamp)
        }
        gotReadLock
      case None => false
    }
}
