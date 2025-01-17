package com.thatdot.quine.graph.messaging

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.{CompiledQuery, Location}
import com.thatdot.quine.model.HalfEdge

/** Top-level type of all algorithms-related messages relayed through the graph
  *
  * Used in [[com.thatdot.quine.graph.behavior.AlgorithmBehavior]].
  */
sealed abstract class AlgorithmMessage extends QuineMessage

sealed abstract class AlgorithmCommand extends AlgorithmMessage

object AlgorithmMessage {

  /** Begin the protocol to take a random walk starting from the node receiving the message.
    * Continue by using `AccumulateRandomWalk`.
    *
    * @param collectQuery A constrained OnNode Cypher query to fetch results to fold into the random walk results
    * @param length       Maximum length of the walk.
    * @param returnParam  the `p` parameter for biasing random walks back to the previous node.
    * @param inOutParam   the `q` parameter for biasing random walks toward BFS or DFS.
    * @param seedOpt      optional string to set the random seed before choosing an edge
    * @param replyTo      Where to send the final result.
    */
  final case class GetRandomWalk(
    collectQuery: CompiledQuery[Location.OnNode],
    length: Int,
    returnParam: Double,
    inOutParam: Double,
    seedOpt: Option[String],
    replyTo: QuineRef,
  ) extends AlgorithmCommand
      with AskableQuineMessage[RandomWalkResult]

  /** Primitive message to recursively collect IDs from a random walk through the graph.
    *
    * @param collectQuery     A constrained OnNode Cypher query to fetch results to fold into the random walk results
    * @param remainingLength  how many hops remain before terminating
    * @param neighborhood     neighborhood around the node sending this message. Used to bias walks with inOutParam
    * @param returnParam      the `p` parameter for biasing random walks back to the previous node.
    * @param inOutParam       the `q` parameter for biasing random walks toward BFS or DFS.
    * @param seedOpt          optional string to set the random seed before choosing an edge
    * @param prependAcc       accumulated results are prepended.
    * @param validateHalfEdge if `Some(_)`, then the receiver should validate the half edge first
    * @param excludeOther     nodes to exclude from the walk (sometimes required to ensure termination)
    * @param reportTo         delivery final answer here.
    */
  final case class AccumulateRandomWalk(
    collectQuery: CompiledQuery[Location.OnNode],
    remainingLength: Int,
    neighborhood: Set[QuineId],
    returnParam: Double,
    inOutParam: Double,
    seedOpt: Option[String],
    prependAcc: List[String],
    validateHalfEdge: Option[HalfEdge],
    excludeOther: Set[QuineId],
    reportTo: QuineRef,
  ) extends AlgorithmCommand

  final case class RandomWalkResult(acc: List[String], didComplete: Boolean) extends AlgorithmMessage
}
