package com.thatdot.quine.graph.messaging

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.model.HalfEdge

/** Top-level type of all Cypher-related messages relayed through the graph
  *
  * Used in [[CypherBehavior]].
  */
sealed abstract class CypherMessage extends QuineMessage

object CypherMessage {
  sealed abstract class CypherQueryInstruction extends CypherMessage

  /** The information that gets handed to a node to execute a cypher query
    *
    * @param query the actual query to execute
    * @param parameters the query parameters (these are basically constants)
    * @param context the variables brought into scope by the query up til now
    */
  final case class QueryPackage(
    query: cypher.Query[cypher.Location.OnNode],
    parameters: cypher.Parameters,
    context: cypher.QueryContext,
    replyTo: QuineRef
  ) extends CypherQueryInstruction
      with AskableQuineMessage[Source[QueryContextResult, NotUsed]]

  /** Start by checking a half edge. If that matches, move on to the actual query
    *
    * @param halfEdge half edge to look at
    * @param action `None` means "check", `Some(true)` means add, `Some(false)` means remove
    * @param query the actual query to execute
    * @param parameters the query parameters (these are basically constants)
    * @param context the variables brought into scope by the query up til now
    */
  final case class CheckOtherHalfEdge(
    halfEdge: HalfEdge,
    action: Option[Boolean],
    query: cypher.Query[cypher.Location.OnNode],
    parameters: cypher.Parameters,
    context: cypher.QueryContext,
    replyTo: QuineRef
  ) extends CypherQueryInstruction
      with AskableQuineMessage[Source[QueryContextResult, NotUsed]] {

    def queryPackage: QueryPackage = QueryPackage(query, parameters, context, replyTo)
  }

  final case class QueryContextResult(
    result: cypher.QueryContext
  ) extends CypherMessage
}
