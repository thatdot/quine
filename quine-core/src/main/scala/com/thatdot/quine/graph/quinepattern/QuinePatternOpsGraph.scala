package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.{ActorRef, Props}

import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.{BaseGraph, NamespaceId, StandingQueryId, StandingQueryOpsGraph, behavior}

/** Hook interface for node wakeup notifications.
  *
  * Thread-safety note: `getNodeWakeInfo` is called to retrieve info for sending
  * a message to the host actor, rather than directly calling methods on this object.
  * This ensures state modifications happen on the correct actor thread.
  */
trait NodeWakeHook {

  /** Get info needed to send NodeWake message to the host actor */
  def getNodeWakeInfo: (StandingQueryId, NamespaceId, Map[Symbol, com.thatdot.quine.language.ast.Value])
}

/** Trait representing operations related to Quine pattern handling within a graph.
  * This trait extends the `BaseGraph` trait and provides functionality for managing
  * pattern registries and loaders specific to Quine patterns.
  *
  * Responsibilities include:
  * - Ensuring compatibility with the required node type behavior.
  * - Providing access to the registry actor for managing pattern registries.
  * - Providing access to the loader actor for handling loading operations related to Quine patterns.
  */
trait QuinePatternOpsGraph extends BaseGraph { this: StandingQueryOpsGraph =>

  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[QuinePatternOpsGraph, behavior.QuinePatternQueryBehavior]

  private[this] val registryActor: ActorRef = system.actorOf(Props(classOf[QuinePatternRegistry], namespacePersistor))

  private[this] val loaderActor: ActorRef = system.actorOf(Props(classOf[QuinePatternLoader], this))

  // Node wake hooks - stores (hook, hostActorRef) pairs
  private[this] val nodeHooks = collection.concurrent.TrieMap.empty[NodeWakeHook, ActorRef]

  def getRegistry: ActorRef = {
    requireCompatibleNodeType()
    registryActor
  }

  def getLoader: ActorRef = {
    requireCompatibleNodeType()
    loaderActor
  }

  // Hook registration - includes host ActorRef for thread-safe messaging
  def registerNodeHook(hook: NodeWakeHook, hostActorRef: ActorRef): Unit =
    nodeHooks += (hook -> hostActorRef)

  def unregisterNodeHook(hook: NodeWakeHook): Unit =
    nodeHooks -= hook

  def onNodeCreated(actorRef: ActorRef, nodeId: SpaceTimeQuineId): Unit =
    // Notify hooks via message passing (thread-safe)
    nodeHooks.foreach { case (hook, hostActorRef) =>
      val (anchorId, ns, ctx) = hook.getNodeWakeInfo
      if (nodeId.namespace == ns) {
        hostActorRef ! behavior.QuinePatternCommand.NodeWake(anchorId, nodeId.id, ns, ctx)
      }
    }
}
