package com.thatdot.quine.app.routes

import scala.collection.concurrent
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.pekko.http.scaladsl.model.ws
import org.apache.pekko.http.scaladsl.server.Directives.handleWebSocketMessages
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.{Done, NotUsed}

import cats.syntax.either._
import io.circe
import io.circe.{Decoder, Encoder}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{GraphNotReadyException, defaultNamespaceId}
import com.thatdot.quine.gremlin.QuineGremlinException
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.{
  CypherQuery,
  GremlinQuery,
  QueryLanguage,
  QueryProtocolMessage,
  QueryProtocolMessageSchema,
  UiEdge,
  UiNode,
}

/** Information about the queries that are running under a websocket connection
  *
  * @param configuration initial message that triggered the query
  * @param termination signal that can be used to watch the query terminate
  * @param killSwitch switch that can be flipped to cancel the query
  * @param isReadOnly is the query read-only
  * @param canContainAllNodeScan whether the query may require an all node scan (a potentially costly operation)
  */
final case class RunningQuery(
  configuration: QueryProtocolMessage.RunQuery,
  termination: Future[Done],
  killSwitch: UniqueKillSwitch,
  isReadOnly: Boolean,
  canContainAllNodeScan: Boolean,
)

/** Protocol for running queries (streaming results, cancellation, concurrently) over a WebSocket
  *
  * @see [[QueryProtocolMessage]] for the protocol - this is just a server implementation
  */
trait WebSocketQueryProtocolServer
    extends QueryProtocolMessageSchema
    with exts.ServerQuineEndpoints
    with QueryUiRoutesImpl
    with LazySafeLogging {

  import QueryProtocolMessage._
  implicit protected def logConfig: LogConfig

  implicit private[this] val clientMessageDecoder: Decoder[ClientMessage] = clientMessageSchema.decoder
  private[this] val serverMessageEncoder: Encoder[ServerMessage[Id]] = serverMessageSchema.encoder

  private case class RunningQueriesAndSink(
    runningQueries: concurrent.Map[Int, RunningQuery],
    sink: Sink[ServerMessage[Id], NotUsed],
  )

  /** Protocol flow
    *
    * @return a flow which materializes into the map of running queries
    */
  val queryProtocol: Flow[ws.Message, ws.Message, concurrent.Map[Int, RunningQuery]] = {

    /* The merge hub lets us combine results from dynamically added queries.
     *
     * The materialized value is mapped over to create a fresh concurrent map. If we were to make
     * this map a local variable in the function, it would end up being shared across multiple
     * materializations of the flow.
     */
    val mergeHub = MergeHub
      .source[ServerMessage[Id]]
      .mapMaterializedValue { sink =>
        val runningQueries: concurrent.Map[Int, RunningQuery] = concurrent.TrieMap.empty
        (sink, runningQueries)
      }

    Flow
      .fromGraph(
        GraphDSL.createGraph(mergeHub) { implicit builder => mergedSource =>
          import GraphDSL.Implicits._

          // Receive client messages and deserialize them
          val clientMessages = builder.add(Flow[ws.Message])

          // Do something with client messages and return a response
          val processClientRequests = builder.add(Concat[AnyRef](inputPorts = 2))

          builder.materializedValue ~> processClientRequests.in(0)
          clientMessages.out
            .flatMapConcat {
              case textMessage: ws.TextMessage =>
                textMessage.textStream
                  .fold("")(_ + _)
                  .map(deserializeClientTextMessage)
              case _: ws.BinaryMessage =>
                val msg = "Binary websocket messages are not supported"
                Source.single(Left(QueryProtocolMessage.MessageError(msg)))
            } ~> processClientRequests.in(1)

          // We use a preferred merge to ensure responses aren't delayed due to results
          val responseAndResultMerge = builder.add(
            MergePreferred[ServerMessage[Id]](
              secondaryPorts = 1,
              eagerComplete = false,
            ),
          )

          mergedSource ~> responseAndResultMerge.in(0)
          processClientRequests.out
            .statefulMap[RunningQueriesAndSink, Option[ServerMessage[Id]]](() => RunningQueriesAndSink(null, null))(
              { case (state @ RunningQueriesAndSink(runningQueries, sink), request) =>
                request match {
                  case msg: Either[MessageError @unchecked, ClientMessage @unchecked] =>
                    state -> Some(
                      msg
                        .map(clientMessage =>
                          try processClientMessage(clientMessage, runningQueries, sink)
                          catch {
                            case NonFatal(err) => MessageError(serverExceptionMessage(err))
                          },
                        )
                        .merge,
                    )
                  case (
                        sinkMat: Sink[ServerMessage[Id] @unchecked, NotUsed @unchecked],
                        runningQueriesMat: concurrent.Map[Int @unchecked, RunningQuery @unchecked],
                      ) =>
                    RunningQueriesAndSink(runningQueriesMat, sinkMat) -> None
                  case other => throw new RuntimeException(s"Unexpected value: $other")
                }
              },
              _ => None,
            )
            .collect { case Some(s) => s } ~> responseAndResultMerge.preferred

          FlowShape(
            clientMessages.in,
            responseAndResultMerge.out.map(m => ws.TextMessage(serverMessageEncoder(m).noSpaces)).outlet,
          )
        },
      )
      .mapMaterializedValue(_._2)
  }

  /** Deserialize a single text message into a client message or (an error)
    *
    * @param message serialized client message
    * @return deserialized client message or error
    */
  private[this] def deserializeClientTextMessage(message: String): Either[MessageError, ClientMessage] =
    // TODO: switch back to accumulating decoder?
    circe.parser.decode(message).leftMap { error =>
      val msg = "Failed to deserialize client message:\n" + circe.Error.showError.show(error)
      QueryProtocolMessage.MessageError(msg)
    }

  /** Turn an exception into a string to send back to the client
    *
    * @param throwable exception
    */
  private[this] def serverExceptionMessage(throwable: Throwable): String =
    throwable match {
      case qge: QuineGremlinException => qge.pretty
      case qce: CypherException => qce.pretty
      case gnr: GraphNotReadyException => gnr.getMessage
      case are: ArithmeticException => are.getMessage // known to be thrown by the `round()` built-in function
      case iae: IllegalArgumentException => iae.getMessage
      case other =>
        val message = s"Query failed with log ID: ${Random.alphanumeric.take(10).mkString}"
        logger.error(log"${Safe(message)}" withException other)
        message
    }

  /** Process a client message and return the message with which to reply
    *
    * @param message client message
    * @param queries queries already running (the ones managed by this websocket)
    * @param sink result sink (which can be re-materialized as many times as needed)
    * @return server response message
    */
  private[this] def processClientMessage(
    message: ClientMessage,
    queries: concurrent.Map[Int, RunningQuery],
    sink: Sink[ServerMessage[Id], NotUsed],
  ): ServerResponseMessage = {
    graph.requiredGraphIsReady()
    message match {
      case run: RunQuery =>
        // Batch up results according to the user-specified time and batch size
        def batched[A, M](input: Source[A, M]): Source[Seq[A], M] =
          (run.resultsWithinMillis, run.maxResultBatch) match {
            case (None, None) =>
              input.map(Seq(_))
            case (None, Some(maxBatch)) =>
              input.grouped(maxBatch)
            case (Some(maxMillis), batchOpt) =>
              input.groupedWithin(batchOpt.getOrElse(Int.MaxValue), maxMillis.millis)
          }
        val atTime = run.atTime.map(Milliseconds.apply)
        val namespace = defaultNamespaceId // TODO: allow access to non-default namespaces
        // Depending on the sort of query and query language, build up different server messages
        val (results, isReadOnly, canContainAllNodeScan, columns): (
          Source[ServerMessage[Id], UniqueKillSwitch],
          Boolean,
          Boolean,
          Option[Seq[String]],
        ) =
          // TODO canContainAllNodeScan is true for all Gremlin queries?
          run.sort match {
            case NodeSort =>
              val (results, isReadOnly, canContainAllNodeScan): (Source[UiNode[Id], NotUsed], Boolean, Boolean) =
                run.language match {
                  case QueryLanguage.Gremlin =>
                    (
                      queryGremlinNodes(
                        GremlinQuery(run.query, run.parameters),
                        namespace,
                        atTime,
                      ),
                      true,
                      true,
                    )
                  case QueryLanguage.Cypher =>
                    queryCypherNodes(CypherQuery(run.query, run.parameters), namespace, atTime)
                }
              val batches = batched(results.viaMat(KillSwitches.single)(Keep.right))
              (batches.map(NodeResults(run.queryId, _)), isReadOnly, canContainAllNodeScan, None)

            case EdgeSort =>
              val (results, isReadOnly, canContainAllNodeScan): (Source[UiEdge[Id], NotUsed], Boolean, Boolean) =
                run.language match {
                  case QueryLanguage.Gremlin =>
                    (
                      queryGremlinEdges(
                        GremlinQuery(run.query, run.parameters),
                        namespace,
                        atTime,
                      ),
                      true,
                      true,
                    )
                  case QueryLanguage.Cypher =>
                    queryCypherEdges(CypherQuery(run.query, run.parameters), namespace, atTime)
                }
              val batches = batched(results.viaMat(KillSwitches.single)(Keep.right))
              (batches.map(EdgeResults(run.queryId, _)), isReadOnly, canContainAllNodeScan, None)

            case TextSort =>
              run.language match {
                case QueryLanguage.Gremlin =>
                  val results = queryGremlinGeneric(
                    GremlinQuery(run.query, run.parameters),
                    namespace,
                    atTime,
                  )
                  val batches = batched(results.viaMat(KillSwitches.single)(Keep.right))
                  (batches.map(NonTabularResults(run.queryId, _)), true, true, None)

                case QueryLanguage.Cypher =>
                  val cypherQuery = CypherQuery(run.query, run.parameters)
                  val (columns, results, isReadOnly, canContainAllNodeScan) =
                    queryCypherGeneric(cypherQuery, namespace, atTime)
                  val batches = batched(results.viaMat(KillSwitches.single)(Keep.right))
                  (
                    batches.map(TabularResults(run.queryId, columns, _)),
                    isReadOnly,
                    canContainAllNodeScan,
                    Some(columns),
                  )
              }
          }
        val ((killSwitch, termination), source) = results.watchTermination()(Keep.both).preMaterialize()

        // This is where we atomically decide if the provided query ID works
        queries.putIfAbsent(
          run.queryId,
          RunningQuery(run, termination, killSwitch, isReadOnly, canContainAllNodeScan),
        ) match {
          case None =>
            // Actually start running the query
            // TODO: race condition - query responses could start coming before the `QueryStarted` is sent
            source
              .concat(Source.single(QueryFinished(run.queryId)))
              .recover { case NonFatal(err) => QueryFailed(run.queryId, serverExceptionMessage(err)) }
              .runWith(sink)

            // Schedule its removal from the map
            termination.onComplete(_ => queries.remove(run.queryId))(graph.shardDispatcherEC)

            QueryStarted(run.queryId, isReadOnly, canContainAllNodeScan, columns)

          case Some(existingQuery) =>
            MessageError(
              s"Query ID ${run.queryId} is already being used to track another query: $existingQuery",
            )
        }

      case cancel: CancelQuery =>
        queries.remove(cancel.queryId) match {
          case None =>
            MessageError(s"Query ID ${cancel.queryId} isn't tracking any current query")

          case Some(runningQuery: RunningQuery) =>
            runningQuery.killSwitch.shutdown()
            MessageOk
        }
    }
  }

  final val queryProtocolWS: Route =
    query.directive(_ => handleWebSocketMessages(queryProtocol.named("ui-query-protocol-websocket")))
}
