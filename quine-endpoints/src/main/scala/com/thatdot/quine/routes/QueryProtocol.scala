package com.thatdot.quine.routes

import ujson.Value

/** Type of messages sent as part of the WebSocket query protocol
  *
  *   - Every [[ClientRequest]] from the client gets a [[ServerResponse]] reply from the
  *     server, with the replies matching the order of the requests
  *
  *   - The server's responses may be interleaved with query responses. Each query started by
  *     [[RunQuery]] will results in possibly many [[BatchOfResults]] messages and end with either
  *     a [[BatchOfResults]] with `queryFinished = true` or a [[QueryFailed]]
  */
sealed abstract class QueryProtocolMessage[+Id]

/* Possible extensions:
 *
 *   - messages for managing settings
 *      * authorization
 *      * what to do on WS failure (cancel query or let it run to completion?)
 */
object QueryProtocolMessage {

  /** Messages sent by the client to the server */
  sealed abstract class ClientMessage extends QueryProtocolMessage[Nothing]

  /** Messages sent by the client to the server which warrant a response message
    * from the server.
    *
    * @see ServerResponseMessage
    */
  sealed abstract class ClientRequestMessage extends ClientMessage

  /** Messages sent by the server to the client */
  sealed abstract class ServerMessage[+Id] extends QueryProtocolMessage[Id]

  /** Messages sent by the server to the client in direct response to a request
    * message.
    *
    * @see ClientRequestMessage
    */
  sealed abstract class ServerResponseMessage extends ServerMessage[Nothing]

  /** Messages sent by the server to the client corresponding to an asynchronous
    * update to a client query
    */
  sealed abstract class ServerAsyncNotificationMessage[+Id] extends ServerMessage[Id] {

    /** for which query is the notification? */
    val queryId: Int
  }

  /** What sort of results does the query deliver? */
  sealed abstract class QuerySort
  case object NodeSort extends QuerySort
  case object EdgeSort extends QuerySort
  case object TextSort extends QuerySort

  /** Instruct the server to start running a query
    *
    * @param queryId id that will be used to refer to the query in the future
    * @param query raw source of the query
    * @param sort what type of results should the query produce?
    * @param parameters constants in the query
    * @param language what language is the query written in?
    * @param atTime what moment in time should be queried?
    * @param maxResultBatch max number of rows in a single result batches ([[None]] means no limit)
    * @param resultsWithin wait this ms delay between result batches ([[None]] means no delay)
    */
  final case class RunQuery(
    queryId: Int,
    query: String,
    sort: QuerySort,
    parameters: Map[String, ujson.Value],
    language: QueryLanguage,
    atTime: Option[Long],
    maxResultBatch: Option[Int],
    resultsWithinMillis: Option[Int]
  ) extends ClientRequestMessage

  /** Instruct the server to cancel a running query
    *
    * @param id which query to cancel
    */
  final case class CancelQuery(
    queryId: Int
  ) extends ClientRequestMessage

  /** Indicate that there was some error processing the last client message
    *
    * TODO: use error codes
    *
    * @param message error message associated with the failure
    */
  final case class MessageError(
    error: String
  ) extends ServerResponseMessage

  /** Indicate that the client message has been processed
    *
    * This is sent when there is otherwise no other more interesting information to return.
    */
  case object MessageOk extends ServerResponseMessage

  /** Indicate that the client query has been accepted and started
    *
    * @param queryId for which query is the confirmation
    * @param isReadOnly whether the query was definitely read-only (and detectable as such at compile time)
    * @param canContainAllNodeScan whether the query may require an all node scan (a potentially costly operation)
    * @param columns the names of the columns to be returned by the query
    */
  final case class QueryStarted(
    queryId: Int,
    isReadOnly: Boolean,
    canContainAllNodeScan: Boolean,
    columns: Option[Seq[String]]
  ) extends ServerResponseMessage

  /** Batch of tabular results to a query
    *
    * @param queryId for which query are the results
    * @param columns columns of the query
    * @param results result rows
    */
  final case class TabularResults(
    queryId: Int,
    columns: Seq[String],
    results: Seq[Seq[ujson.Value]]
  ) extends ServerAsyncNotificationMessage[Nothing]

  /** Batch of non-tabular results to a query
    *
    * @param queryId for which query are the results
    * @param results result values
    */
  final case class NonTabularResults(
    queryId: Int,
    results: Seq[ujson.Value]
  ) extends ServerAsyncNotificationMessage[Nothing]

  /** Batch of node results to a query
    *
    * @param queryId for which query are the results
    * @param results result values
    */
  final case class NodeResults[Id](
    queryId: Int,
    results: Seq[UiNode[Id]]
  ) extends ServerAsyncNotificationMessage[Id]

  /** Batch of edge results to a query
    *
    * @param queryId for which query are the results
    * @param results result values
    */
  final case class EdgeResults[Id](
    queryId: Int,
    results: Seq[UiEdge[Id]]
  ) extends ServerAsyncNotificationMessage[Id]

  /** Indicate that a query failed
    *
    * TODO: should we include more debug information here?
    *
    * @param queryId for which query is the failure
    * @param message error message associated with the failure
    */
  final case class QueryFailed(
    queryId: Int,
    message: String
  ) extends ServerAsyncNotificationMessage[Nothing]

  /** Indicate that a query finished
    *
    * @param queryId which query is done
    */
  final case class QueryFinished(
    queryId: Int
  ) extends ServerAsyncNotificationMessage[Nothing]

}

trait QueryProtocolMessageSchema extends endpoints4s.generic.JsonSchemas with exts.UjsonAnySchema with QuerySchemas {

  import QueryProtocolMessage._

  implicit val clientMessageSchema: Tagged[ClientMessage] = {
    implicit val anyJson: JsonSchema[Value] = anySchema(None)
    implicit lazy val queryLanguageSchema: Enum[QueryLanguage] =
      stringEnumeration[QueryLanguage](Seq(QueryLanguage.Gremlin, QueryLanguage.Cypher))(_.toString)
    implicit lazy val querySortSchema: Enum[QuerySort] =
      stringEnumeration[QuerySort](Seq(TextSort, NodeSort, EdgeSort))(_.toString)
    genericTagged[ClientMessage]
  }

  implicit def serverMessageSchema: Tagged[ServerMessage[Id]] = {
    implicit val anyJson: JsonSchema[Value] = anySchema(None)
    genericTagged[ServerMessage[Id]]
  }
}
