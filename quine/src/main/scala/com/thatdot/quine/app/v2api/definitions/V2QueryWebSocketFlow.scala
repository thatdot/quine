package com.thatdot.quine.app.v2api.definitions

import scala.collection.concurrent
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.{Done, NotUsed}

import io.circe
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.QueryWebSocketProtocol._
import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.GraphNotReadyException
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.CypherQuery

/** Builds a Pekko Streams flow for the V2 query WebSocket protocol.
  *
  * Both client and server messages use the V2 [[com.thatdot.api.v2.QueryWebSocketProtocol]] types. The `interpreter`
  * field on [[RunQuery]] selects between Cypher and QuinePattern backends.
  *
  * Query execution is delegated to a [[V2QueryExecutor]], separating protocol handling from graph access.
  */
object V2QueryWebSocketFlow extends LazySafeLogging {

  /** Per-message authorization function. Returns `Right(message)` to allow or `Left(errorString)` to deny. */
  type MessageAuthorizer = ClientMessage => Either[String, ClientMessage]

  /** Tracks a running query within a WebSocket session. */
  private case class V2RunningQuery(
    description: String,
    termination: Future[Done],
    killSwitch: UniqueKillSwitch,
    isReadOnly: Boolean,
    canContainAllNodeScan: Boolean,
  )

  /** Input to the stateful processing stage — either materialized infrastructure or a deserialized client message. */
  sealed abstract private class ProcessingInput
  final private case class Materialized(
    runningQueries: concurrent.Map[Int, V2RunningQuery],
    sink: Sink[ServerMessage, NotUsed],
  ) extends ProcessingInput
  final private case class DeserializedMessage(
    result: Either[MessageError, ClientMessage],
  ) extends ProcessingInput

  /** State of the stateful processing stage. `None` until the MergeHub materializes. */
  final private case class ProcessingState(
    ready: Option[Materialized],
  )

  /** Build the WebSocket flow.
    *
    * @param executor query execution backend (binds namespace and converts results to V2 types)
    * @param authorizeMessage optional per-message authorizer; when `None`, all messages are allowed
    * @return a Pekko Streams flow from WebSocketFrame to WebSocketFrame
    */
  def buildFlow(
    executor: V2QueryExecutor,
    authorizeMessage: Option[MessageAuthorizer] = None,
  )(implicit materializer: Materializer, logConfig: LogConfig): Flow[WebSocketFrame, WebSocketFrame, NotUsed] = {

    def serializeServerMessage(msg: ServerMessage): WebSocketFrame =
      WebSocketFrame.Text(ServerMessage.encoder(msg).noSpaces, finalFragment = true, rsv = None)

    def deserializeClientTextMessage(payload: String): Either[MessageError, ClientMessage] =
      circe.parser.decode[ClientMessage](payload)(ClientMessage.decoder).left.map { error =>
        val msg = "Failed to deserialize client message:\n" + circe.Error.showError.show(error)
        MessageError(msg)
      }

    def serverExceptionMessage(throwable: Throwable): String =
      throwable match {
        case qce: CypherException => qce.pretty
        case gnr: GraphNotReadyException => gnr.getMessage
        case are: ArithmeticException => are.getMessage
        case iae: IllegalArgumentException => iae.getMessage
        case other =>
          val message = s"Query failed with log ID: ${Random.alphanumeric.take(10).mkString}"
          logger.error(log"${Safe(message)}" withException other)
          message
      }

    def processClientMessage(
      message: ClientMessage,
      queries: concurrent.Map[Int, V2RunningQuery],
      sink: Sink[ServerMessage, NotUsed],
    ): ServerResponseMessage =
      if (executor.isReady) message match {
        case run: RunQuery =>
          val useQuinePattern = run.interpreter == QueryInterpreter.QuinePattern

          def batched[A, M](input: Source[A, M]): Source[Seq[A], M] =
            (run.resultsWithinMillis, run.maxResultBatch) match {
              case (None, None) => input.map(Seq(_))
              case (None, Some(maxBatch)) => input.grouped(maxBatch)
              case (Some(maxMillis), batchOpt) =>
                input.groupedWithin(batchOpt.getOrElse(Int.MaxValue), maxMillis.millis)
            }

          val atTime = run.atTime.map(Milliseconds.apply)
          val cypherQuery = CypherQuery(run.query, run.parameters)

          val (results, isReadOnly, canContainAllNodeScan, columns): (
            Source[ServerMessage, UniqueKillSwitch],
            Boolean,
            Boolean,
            Option[Seq[String]],
          ) = run.sort match {
            case QuerySort.Node =>
              val (nodeSource, ro, scan) = executor.executeNodeQuery(cypherQuery, atTime, useQuinePattern)
              val batches = batched(nodeSource.viaMat(KillSwitches.single)(Keep.right))
              (batches.map(NodeResults(run.queryId, _)), ro, scan, None)

            case QuerySort.Edge =>
              val (edgeSource, ro, scan) = executor.executeEdgeQuery(cypherQuery, atTime, useQuinePattern)
              val batches = batched(edgeSource.viaMat(KillSwitches.single)(Keep.right))
              (batches.map(EdgeResults(run.queryId, _)), ro, scan, None)

            case QuerySort.Text =>
              val (cols, textSource, ro, scan) = executor.executeTextQuery(cypherQuery, atTime, useQuinePattern)
              val batches = batched(textSource.viaMat(KillSwitches.single)(Keep.right))
              (batches.map(TabularResults(run.queryId, cols, _)), ro, scan, Some(cols))
          }

          val ((killSwitch, termination), source) = results.watchTermination()(Keep.both).preMaterialize()

          queries.putIfAbsent(
            run.queryId,
            V2RunningQuery(run.toString, termination, killSwitch, isReadOnly, canContainAllNodeScan),
          ) match {
            case None =>
              source
                .concat(Source.single(QueryFinished(run.queryId)))
                .recover { case NonFatal(err) => QueryFailed(run.queryId, serverExceptionMessage(err)) }
                .runWith(sink)

              termination.onComplete(_ => queries.remove(run.queryId))(executor.executionContext)
              QueryStarted(run.queryId, isReadOnly, canContainAllNodeScan, columns)

            case Some(existingQuery) =>
              MessageError(s"Query ID ${run.queryId} is already being used to track another query: $existingQuery")
          }

        case cancel: CancelQuery =>
          queries.remove(cancel.queryId) match {
            case None =>
              MessageError(s"Query ID ${cancel.queryId} isn't tracking any current query")
            case Some(runningQuery) =>
              runningQuery.killSwitch.shutdown()
              MessageOk
          }
      }
      else MessageError("Graph not ready for execution")

    val mergeHub = MergeHub
      .source[ServerMessage]
      .mapMaterializedValue { sink =>
        val runningQueries: concurrent.Map[Int, V2RunningQuery] = concurrent.TrieMap.empty
        Materialized(runningQueries, sink): ProcessingInput
      }

    Flow
      .fromGraph(
        GraphDSL.createGraph(mergeHub) { implicit builder => mergedSource =>
          import GraphDSL.Implicits._

          val clientMessages = builder.add(Flow[WebSocketFrame])

          val processClientRequests = builder.add(Concat[ProcessingInput](inputPorts = 2))

          builder.materializedValue ~> processClientRequests.in(0)
          clientMessages.out
            .collect { case WebSocketFrame.Text(payload, _, _) => payload }
            .map { payload =>
              val deserialized = deserializeClientTextMessage(payload)
              val authorized = deserialized.flatMap { msg =>
                authorizeMessage match {
                  case Some(authorize) =>
                    authorize(msg).left.map(MessageError.apply)
                  case None =>
                    Right(msg)
                }
              }
              DeserializedMessage(authorized): ProcessingInput
            } ~> processClientRequests.in(1)

          val responseAndResultMerge = builder.add(
            MergePreferred[ServerMessage](
              secondaryPorts = 1,
              eagerComplete = false,
            ),
          )

          mergedSource ~> responseAndResultMerge.in(0)
          processClientRequests.out
            .statefulMap[ProcessingState, Option[ServerMessage]](() => ProcessingState(ready = None))(
              { case (state, input) =>
                input match {
                  case mat: Materialized =>
                    ProcessingState(ready = Some(mat)) -> None
                  case DeserializedMessage(msg) =>
                    val Materialized(runningQueries, sink) = state.ready.getOrElse(
                      throw new IllegalStateException("Received client message before MergeHub materialized"),
                    )
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
                }
              },
              _ => None,
            )
            .collect { case Some(s) => s } ~> responseAndResultMerge.preferred

          FlowShape(
            clientMessages.in,
            responseAndResultMerge.out.map(serializeServerMessage).outlet,
          )
        },
      )
      .mapMaterializedValue(_ => NotUsed)
  }
}
