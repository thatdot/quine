package com.thatdot.quine.routes

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

import cats.data.Validated
import io.circe.parser.decodeAccumulating
import io.circe.{Decoder, Encoder, Json}
import org.scalajs.dom
import org.scalajs.dom.console

import com.thatdot.api.v2.QueryWebSocketProtocol._

/** A query to submit over the V2 WebSocket.
  *
  * @param query raw source of the query
  * @param parameters constants bound in the query
  * @param language the query language
  * @param interpreter which compiler/interpreter backend to use
  * @param atTime optional historical timestamp (epoch millis)
  * @param maxResultBatch max rows per result batch (`None` means no limit)
  * @param resultsWithinMillis max ms delay between result batches (`None` means no delay)
  */
final case class V2StreamingQuery(
  query: String,
  parameters: Map[String, Json] = Map.empty,
  language: QueryLanguage = QueryLanguage.Cypher,
  interpreter: QueryInterpreter = QueryInterpreter.Cypher,
  atTime: Option[Long] = None,
  maxResultBatch: Option[Int] = None,
  resultsWithinMillis: Option[Int] = None,
) {
  private[routes] def makeRunQueryMessage(
    queryId: QueryId,
    sort: QuerySort,
  ): RunQuery =
    RunQuery(
      queryId = queryId.id,
      query = query,
      sort = sort,
      parameters = parameters,
      language = language,
      interpreter = interpreter,
      atTime = atTime,
      maxResultBatch = maxResultBatch,
      resultsWithinMillis = resultsWithinMillis,
    )
}

/** Callbacks invoked as results arrive for a V2 query. */
sealed trait V2QueryCallbacks {
  def querySort: QuerySort
  def onError(message: String): Unit
  def onComplete(): Unit
  def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit
  def onQueryCancelOk(): Unit
  def onQueryCancelError(message: String): Unit

  def onQueryError(message: String): Unit = onError(message)

  def onProtocolError(
    clientMessage: ClientMessage,
    serverMessage: ServerResponseMessage,
  ): Unit = onError(s"Query protocol error: $serverMessage is not a valid response to $clientMessage")

  def onWebsocketError(event: dom.Event, webSocket: dom.WebSocket): Unit =
    onError(s"WebSocket connection to `${webSocket.url}` was lost")

  def onWebsocketClose(event: dom.CloseEvent, webSocket: dom.WebSocket): Unit =
    onError(s"WebSocket connection to `${webSocket.url}` was closed")
}

object V2QueryCallbacks {
  trait NodeCallbacks extends V2QueryCallbacks {
    final def querySort: QuerySort = QuerySort.Node
    def onNodeResults(batchOfNodes: Seq[UiNode]): Unit
  }

  trait EdgeCallbacks extends V2QueryCallbacks {
    final def querySort: QuerySort = QuerySort.Edge
    def onEdgeResults(batchOfEdges: Seq[UiEdge]): Unit
  }

  trait TextCallbacks extends V2QueryCallbacks {
    final def querySort: QuerySort = QuerySort.Text
    def onTabularResults(columns: Seq[String], batchOfRows: Seq[Seq[Json]]): Unit = ()
  }

  class CollectNodesToFuture extends NodeCallbacks {
    private val result = Promise[Option[Seq[UiNode]]]()
    private val buffer = Seq.newBuilder[UiNode]
    private var cancelled = false

    def future: Future[Option[Seq[UiNode]]] = result.future

    def onNodeResults(batchOfNodes: Seq[UiNode]): Unit = buffer ++= batchOfNodes
    def onError(message: String): Unit = result.failure(new Exception(message))
    def onComplete(): Unit = result.success(if (cancelled) None else Some(buffer.result()))
    def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit = ()
    def onQueryCancelOk(): Unit = cancelled = true
    def onQueryCancelError(message: String): Unit = ()
  }

  class CollectEdgesToFuture extends EdgeCallbacks {
    private val result = Promise[Option[Seq[UiEdge]]]()
    private val buffer = Seq.newBuilder[UiEdge]
    private var cancelled = false

    def future: Future[Option[Seq[UiEdge]]] = result.future

    def onEdgeResults(batchOfEdges: Seq[UiEdge]): Unit = buffer ++= batchOfEdges
    def onError(message: String): Unit = result.failure(new Exception(message))
    def onComplete(): Unit = result.success(if (cancelled) None else Some(buffer.result()))
    def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit = ()
    def onQueryCancelOk(): Unit = cancelled = true
    def onQueryCancelError(message: String): Unit = ()
  }
}

/** Client for running queries over the V2 WebSocket protocol.
  *
  * @see [[QueryWebSocketProtocol]] for the wire format
  * @param webSocket raw web socket connected to `/api/v2/query/ws`
  * @param clientName name used in console error messages
  */
class V2WebSocketQueryClient(
  val webSocket: dom.WebSocket,
  val clientName: String = "V2WebSocketQueryClient",
) {

  private val clientMessageEncoder: Encoder[ClientMessage] = ClientMessage.encoder
  implicit private val serverMessageDecoder: Decoder[ServerMessage] = ServerMessage.decoder

  private val queries: mutable.Map[QueryId, (V2StreamingQuery, V2QueryCallbacks)] =
    mutable.Map.empty

  def activeQueries: collection.Map[QueryId, (V2StreamingQuery, V2QueryCallbacks)] = queries

  /** Pending client request messages awaiting a server response (matched by order). */
  private val pendingMessages = mutable.Queue.empty[ClientMessage]

  private var nextQueryId = 0

  webSocket.addEventListener[dom.MessageEvent]("message", onMessage(_))
  webSocket.addEventListener[dom.Event]("error", onError(_))
  webSocket.addEventListener[dom.CloseEvent]("close", onClose(_))

  private def onMessage(event: dom.MessageEvent): Unit = {
    val serverMessage: ServerMessage = event.data match {
      case message: String =>
        decodeAccumulating[ServerMessage](message) match {
          case Validated.Valid(msg) => msg
          case Validated.Invalid(errors) =>
            console.error(s"$clientName: could not decode '$message' (${errors.toList.mkString(" ")})")
            return
        }
      case other =>
        console.error(s"$clientName: received non-text message", other)
        return
    }

    serverMessage match {
      case response: ServerResponseMessage =>
        val clientMessage = if (pendingMessages.nonEmpty) {
          pendingMessages.dequeue()
        } else {
          console.error(s"$clientName: cannot associate $response with any client request")
          return
        }
        val origQueryId = clientMessage match {
          case rq: RunQuery => rq.queryId
          case cq: CancelQuery => cq.queryId
        }
        val handler: V2QueryCallbacks = queries.get(QueryId(origQueryId)) match {
          case Some((_, callbacks)) => callbacks
          case None =>
            console.error(
              s"$clientName: failed to find callbacks for handling $response (reply to $clientMessage)",
            )
            return
        }

        (clientMessage, response) match {
          case (_: RunQuery, QueryStarted(queryId, isReadOnly, canContainAllNodeScan, columns))
              if origQueryId == queryId =>
            handler.onQueryStart(isReadOnly, canContainAllNodeScan, columns)
          case (_: RunQuery, MessageError(error)) =>
            console.error(s"$clientName: failed to run query: $error")
            handler.onQueryError(error)
            queries -= QueryId(origQueryId)
          case (_: CancelQuery, MessageOk) =>
            handler.onQueryCancelOk()
          case (_: CancelQuery, MessageError(error)) =>
            console.error(s"$clientName: failed to cancel query: $error")
            handler.onQueryCancelError(error)
          case _ =>
            console.error(s"$clientName: received invalid response '$response' to '$clientMessage'")
            handler.onProtocolError(clientMessage, response)
        }

      case notification: ServerAsyncNotification =>
        val callbacks: V2QueryCallbacks = queries.get(QueryId(notification.queryId)) match {
          case Some((_, callbacks)) => callbacks
          case None =>
            console.error(s"$clientName: message about unknown query ID $serverMessage")
            return
        }

        (notification, callbacks) match {
          case (QueryFailed(queryId, message), _) =>
            callbacks.onQueryError(message)
            queries -= QueryId(queryId)
          case (QueryFinished(queryId), _) =>
            callbacks.onComplete()
            queries -= QueryId(queryId)
          case (TabularResults(_, cols, rows), handler: V2QueryCallbacks.TextCallbacks) =>
            handler.onTabularResults(cols, rows)
          case (NodeResults(_, results), handler: V2QueryCallbacks.NodeCallbacks) =>
            handler.onNodeResults(results)
          case (EdgeResults(_, results), handler: V2QueryCallbacks.EdgeCallbacks) =>
            handler.onEdgeResults(results)
          case _ =>
            console.error(s"$clientName: notification '$notification' unhandled by '$callbacks'")
        }
    }
  }

  private def onError(error: dom.Event): Unit = {
    console.error(s"$clientName: WebSocket error", error)
    queries.values.foreach(_._2.onWebsocketError(error, webSocket))
    queries.clear()
    pendingMessages.clear()
  }

  private def onClose(close: dom.CloseEvent): Unit = {
    console.warn(s"$clientName: WebSocket closed", close)
    queries.values.foreach(_._2.onWebsocketClose(close, webSocket))
    queries.clear()
    pendingMessages.clear()
  }

  /** Issue a query.
    *
    * @param query query to send to the server
    * @param callbacks what to do when results are returned
    * @return unique identifier for the query, or an error if the socket is not open
    */
  def query(query: V2StreamingQuery, callbacks: V2QueryCallbacks): Either[WebSocketNotOpen, QueryId] = {
    if (webSocket.readyState != dom.WebSocket.OPEN)
      return Left(new WebSocketNotOpen(webSocket.readyState))

    val queryId = QueryId(nextQueryId)
    val runQuery = query.makeRunQueryMessage(queryId, callbacks.querySort)

    queries.put(queryId, query -> callbacks)
    pendingMessages.enqueue(runQuery)

    nextQueryId += 1
    webSocket.send(clientMessageEncoder(runQuery).noSpaces)
    Right(queryId)
  }

  /** Cancel a running query.
    *
    * @param queryId which query to cancel
    */
  def cancelQuery(queryId: QueryId): Either[WebSocketNotOpen, Unit] = {
    if (webSocket.readyState != dom.WebSocket.OPEN)
      return Left(new WebSocketNotOpen(webSocket.readyState))

    val cancel = CancelQuery(queryId.id)
    pendingMessages.enqueue(cancel)

    webSocket.send(clientMessageEncoder(cancel).noSpaces)
    Right(())
  }
}
