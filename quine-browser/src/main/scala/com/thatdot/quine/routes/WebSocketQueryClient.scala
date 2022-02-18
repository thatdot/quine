package com.thatdot.quine.routes

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

import endpoints4s.{Codec, Invalid, Valid}
import org.scalajs.dom
import org.scalajs.dom.console

/** Client to run queries (streaming results, cancellation, concurrently) over a WebSocket
  *
  * @see [[QueryProtocolMessage]] for the protocol - this is just a client implementation
  * @param webSocket raw web socket
  * @param clientName name of the client used in console error messages
  */
class WebSocketQueryClient(
  val webSocket: dom.WebSocket,
  val clientName: String = "WebSocketQueryClient"
) extends QueryProtocolMessageSchema
    with exts.NoopIdSchema {

  import QueryProtocolMessage._

  private val clientMessageCodec: Codec[String, ClientMessage] = clientMessageSchema.stringCodec
  private val serverMessageCodec: Codec[String, ServerMessage[Id]] = serverMessageSchema.stringCodec

  /* Every time we send a query to the backend, we also add it to this map. Then, when we get back
   * results, we use the data in this map to decide what to do with those results. Finally, when
   * the query is either completed or failed, the future is completed and it gets removed from
   * this map.
   */
  private val queries: mutable.Map[QueryId, (StreamingQuery, QueryCallbacks)] =
    mutable.Map.empty[QueryId, (StreamingQuery, QueryCallbacks)]

  /** Read-only view into which queries are active in the client */
  def activeQueries: collection.Map[QueryId, (StreamingQuery, QueryCallbacks)] = queries

  /** What messages have been sent but no response received yet?
    *
    * Since the server replies to client messages in the order they were sent, we need to track
    * this queue explicitly (eg. to map back [[MessageError]] to the right query).
    */
  private val pendingMessages = mutable.Queue.empty[ClientRequestMessage]

  /** Client needs to generates unique query IDs. We do this by just counting up. */
  private var nextQueryId = 0

  webSocket.addEventListener[dom.MessageEvent]("message", onMessage(_))
  webSocket.addEventListener[dom.Event]("error", onError(_))
  webSocket.addEventListener[dom.CloseEvent]("close", onClose(_))

  private def onMessage(event: dom.MessageEvent): Unit = {
    val serverMessage: ServerMessage[Id] = event.data match {
      case message: String =>
        serverMessageCodec.decode(message) match {
          case Valid(serverMessage) => serverMessage
          case Invalid(errors) =>
            console.error(s"$clientName: could not decode '$message' (${errors.mkString(" ")}")
            return
        }
      case other =>
        console.error(s"$clientName: received non-text message", other)
        return
    }

    serverMessage match {
      // Direct response to a client request
      case response: ServerResponseMessage =>
        val clientMessage = if (pendingMessages.nonEmpty) {
          pendingMessages.dequeue()
        } else {
          console.error(s"$clientName: cannot associate $response with any cliient request")
          return
        }
        val origQueryId = clientMessage match {
          case runQuery: RunQuery => runQuery.queryId
          case cancelQuery: CancelQuery => cancelQuery.queryId
        }
        val handler: QueryCallbacks = queries.get(QueryId(origQueryId)) match {
          case Some((_, callbacks)) => callbacks
          case None =>
            console.error(s"$clientName: failed to find callbacks for handling $response (reply to $clientMessage)")
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

      // Async server update for a query
      case notification: ServerAsyncNotificationMessage[Id @unchecked] =>
        val callbacks: QueryCallbacks = queries.get(QueryId(notification.queryId)) match {
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
          case (TabularResults(_, cols, rows), handler: QueryCallbacks.TabularCallbacks) =>
            handler.onTabularResults(cols, rows)
          case (NonTabularResults(_, results), handler: QueryCallbacks.NonTabularCallbacks) =>
            handler.onNonTabularResults(results)
          case (NodeResults(_, results), handler: QueryCallbacks.NodeCallbacks) =>
            handler.onNodeResults(results)
          case (EdgeResults(_, results), handler: QueryCallbacks.EdgeCallbacks) =>
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

  /** Issue a query
    *
    * @note this will fail if the websocket is not open
    * @param query query to send to the server
    * @param callbacks what to do when the results are returned?
    * @return unique identifier for the query
    */
  def query(query: StreamingQuery, callbacks: QueryCallbacks): Either[WebSocketNotOpen, QueryId] = {
    if (webSocket.readyState != dom.WebSocket.OPEN) {
      return Left(new WebSocketNotOpen(webSocket.readyState))
    }

    val queryId = QueryId(nextQueryId)
    val runQuery = query.makeRunQueryMessage(queryId, callbacks.querySort)

    queries.put(queryId, query -> callbacks)
    pendingMessages.enqueue(runQuery)

    nextQueryId += 1
    webSocket.send(clientMessageCodec.encode(runQuery))
    Right(queryId)
  }

  /** Cancel a query
    *
    * @note this will fail if the websocket is not open
    * @param queryId which query to cancel
    */
  def cancelQuery(queryId: QueryId): Either[WebSocketNotOpen, Unit] = {
    if (webSocket.readyState != dom.WebSocket.OPEN) {
      return Left(new WebSocketNotOpen(webSocket.readyState))
    }

    val cancelQuery = CancelQuery(queryId.id)
    pendingMessages.enqueue(cancelQuery)

    webSocket.send(clientMessageCodec.encode(cancelQuery))
    Right(())
  }
}

/** Identifier/handle for a query issued by a query client
  *
  * @param id identifier which uniquely identifies the query within the client
  */
final case class QueryId(id: Int) extends AnyVal

/** A streaming query
  *
  * @param query raw source of the query
  * @param parameters constants in the query
  * @param language what language is the query written in?
  * @param atTime what moment in time should be queried?
  * @param maxResultBatch max number of rows in a single result batches ([[None]] means no limit)
  * @param resultsWithin wait this ms delay between result batches ([[None]] means no delay)
  */
final case class StreamingQuery(
  query: String,
  parameters: Map[String, ujson.Value],
  language: QueryLanguage,
  atTime: Option[Long],
  maxResultBatch: Option[Int],
  resultsWithinMillis: Option[Int]
) {
  def makeRunQueryMessage(queryId: QueryId, sort: QueryProtocolMessage.QuerySort): QueryProtocolMessage.RunQuery =
    QueryProtocolMessage.RunQuery(
      queryId.id,
      query,
      sort,
      parameters,
      language,
      atTime,
      maxResultBatch,
      resultsWithinMillis
    )
}

/** Callbacks that are invoked as new information about a query arrives */
sealed trait QueryCallbacks {

  /** Type of results the query will produce */
  def querySort: QueryProtocolMessage.QuerySort

  /** Generic error handler
    *
    * @note once this is called, no other callback will be called
    * @param message error message
    */
  def onError(message: String): Unit

  /** Completion handler
    *
    * @note once this is called, no other callback will be called
    */
  def onComplete(): Unit

  /** Confirmation from the server that the query has started
    *
    * @param isReadOnly when `true`, the query is definitely free of side-effects
    * @param canContainAllNodeScan when `false`, the query definitely will not cause an all node scan
    * @param columns if they exist a schema about columns
    */
  def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit

  /** Confirmation from the server that the query has been cancelled
    *
    * @note more results might still drain out before some other completion signal is sent
    */
  def onQueryCancelOk(): Unit

  /** Something went wrong when the tried to cancel the query
    *
    * @note this does not mean the query is definitely done running!
    */
  def onQueryCancelError(message: String): Unit

  /** Error handler for query errors
    *
    * @param message error message
    * @note once this is called, no other callback will be called
    */
  def onQueryError(message: String): Unit = onError(message)

  /** Error handler for protocol errors
    *
    * @param clientMessage initial client message
    * @param serverMessage unexpectedd server message
    */
  def onProtocolError(
    clientMessage: QueryProtocolMessage.ClientRequestMessage,
    serverMessage: QueryProtocolMessage.ServerResponseMessage
  ): Unit = onError(s"Query protocol error: $serverMessage is not a valid response to $clientMessage")

  /** Error handler for websocket failures
    *
    * @param event websocket `onerror` event
    * @note once this is called, no other callback will be called
    */
  def onWebsocketError(event: dom.Event, webSocket: dom.WebSocket): Unit =
    onError(s"WebSocket connection to `${webSocket.url}` was lost")

  /** Handler for websocket close
    *
    * @param event websocket `onclose` event
    * @note once this is called, no other callback will be called
    */
  def onWebsocketClose(event: dom.CloseEvent, webSocket: dom.WebSocket): Unit =
    onError(s"WebSocket connection to `${webSocket.url}` was closed")
}

final object QueryCallbacks {
  trait NodeCallbacks extends QueryCallbacks {
    final def querySort = QueryProtocolMessage.NodeSort

    /** Result handler for a fresh batch of results
      *
      * @param batchOfNodes batch of node results
      */
    def onNodeResults(batchOfNodes: Seq[UiNode[String]]): Unit
  }

  trait EdgeCallbacks extends QueryCallbacks {
    final def querySort = QueryProtocolMessage.EdgeSort

    /** Result handler for a fresh batch of results
      *
      * @param batchOfEdges batch of edge results
      */
    def onEdgeResults(batchOfEdges: Seq[UiEdge[String]]): Unit
  }

  trait TabularCallbacks extends QueryCallbacks {
    final def querySort = QueryProtocolMessage.TextSort

    /** Result handler for a fresh batch of results
      *
      * @param columns columns in the result
      * @param batchOfRows batch of rows
      */
    def onTabularResults(columns: Seq[String], batchOfRows: Seq[Seq[ujson.Value]]): Unit
  }

  trait NonTabularCallbacks extends QueryCallbacks {
    final def querySort = QueryProtocolMessage.TextSort

    /** Result handler for a fresh batch of results
      *
      * @param batch batch of results
      */
    def onNonTabularResults(batch: Seq[ujson.Value]): Unit
  }

  /** Aggregate all node results into one [[Future]] */
  class CollectNodesToFuture extends NodeCallbacks {
    private val result = Promise[Option[Seq[UiNode[String]]]]()
    private val buffer = Seq.newBuilder[UiNode[String]]
    private var cancelled = false

    /** Future of results (or [[None]] if the query gets cancelled) */
    def future: Future[Option[Seq[UiNode[String]]]] = result.future

    def onNodeResults(batchOfNodes: Seq[UiNode[String]]): Unit = buffer ++= batchOfNodes
    def onError(message: String): Unit = result.failure(new Exception(message))
    def onComplete(): Unit = result.success(if (cancelled) None else Some(buffer.result()))

    def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit = ()
    def onQueryCancelOk(): Unit = cancelled = true
    def onQueryCancelError(message: String): Unit = ()
  }

  /** Aggregates all edge results into one [[Future]] */
  class CollectEdgesToFuture extends EdgeCallbacks {
    private val result = Promise[Option[Seq[UiEdge[String]]]]()
    private val buffer = Seq.newBuilder[UiEdge[String]]
    private var cancelled = false

    /** Future of results (or [[None]] if the query gets cancelled) */
    def future: Future[Option[Seq[UiEdge[String]]]] = result.future

    def onEdgeResults(batchOfEdges: Seq[UiEdge[String]]): Unit = buffer ++= batchOfEdges
    def onError(message: String): Unit = result.failure(new Exception(message))
    def onComplete(): Unit = result.success(if (cancelled) None else Some(buffer.result()))

    def onQueryStart(isReadOnly: Boolean, canContainAllNodeScan: Boolean, columns: Option[Seq[String]]): Unit = ()
    def onQueryCancelOk(): Unit = cancelled = true
    def onQueryCancelError(message: String): Unit = ()
  }
}

/** Exception due to a WebSocket not being in the `OPEN` state
  *
  * @param webSocketState actual state of the websocket
  */
class WebSocketNotOpen(webSocketState: String)
    extends IllegalStateException(s"WebSocket is not OPEN (current state is $webSocketState)") {

  def this(state: Int) = this(state match {
    case dom.WebSocket.CONNECTING => "CONNECTING"
    case dom.WebSocket.OPEN => "OPEN"
    case dom.WebSocket.CLOSING => "CLOSING"
    case dom.WebSocket.CLOSED => "CLOSED"
  })
}
