package com.thatdot.quine.graph.cypher

import org.apache.pekko.actor.ActorRef

import com.thatdot.quine.graph.messaging.QuineIdOps
import com.thatdot.quine.graph.{BaseNodeActor, CypherOpsGraph, NamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}

/** Information available to a procedure when it is executing.
  *
  * Unless otherwise stated, methods here are thread-safe, so the procedure may
  * use them in asynchronous code.
  */
trait ProcedureExecutionLocation extends QuineIdOps {

  /** Graph on which the query is executing
    *
    * This can be casted to a more specific graph type
    */
  def graph: CypherOpsGraph

  /** Namespace of node being queried. */
  def namespace: NamespaceId

  /** Historical state being queried, or None for the moving present */
  def atTime: Option[Milliseconds]

  /** The node the query is currently on, or None if the query isn't on a node
    *
    * @note not thread-safe - understand the node actor model before using this
    */
  def node: Option[BaseNodeActor]

  /** ID provider */
  implicit def idProvider: QuineIdProvider

  /** If executing on a node, the actor reference of the node.
    *
    * @note this is for debugging purposes see [[QuineIdOps]] for sending messages
    */
  implicit def self: ActorRef
}
