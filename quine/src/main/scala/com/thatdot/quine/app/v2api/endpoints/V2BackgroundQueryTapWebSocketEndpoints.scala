package com.thatdot.quine.app.v2api.endpoints

import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, webSocketBodyRaw}
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.ErrorResponse.{NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{notFoundError, serverError}
import com.thatdot.api.v2.V2EndpointDefinitions
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.jobs.BackgroundQueryTapRelay
import com.thatdot.quine.app.model.outputs2.query.standing.TapBus
import com.thatdot.quine.app.v2api.definitions.{CommonParameters, CustomMethod, GraphScopedEndpoints, QuineApiMethods}
import com.thatdot.quine.app.v2api.endpoints.V2BackgroundQueryEndpointEntities.BackgroundQueryStatus
import com.thatdot.quine.graph.NamespaceId

/** V2 Tapir WebSocket endpoint tapping one background-query execution's result rows. Frames are
  * JSON text: each row as an object keyed by column name, then one completion frame
  * ([[BackgroundQueryTapRelay.CompletionFrameKey]]) with the terminal status and counts. The
  * executing host buffers pre-connect rows (see [[BackgroundQueryTapRelay]]), so
  * dispatch-then-connect sees the head of the results, from any cluster node. Best-effort with no
  * delivery guarantees — reconcile against the status record's `totalRowCount`.
  */
trait V2BackgroundQueryTapWebSocketEndpoints
    extends V2EndpointDefinitions
    with CommonParameters
    with GraphScopedEndpoints {
  val appMethods: QuineApiMethods
  implicit protected def logConfig: LogConfig

  /** Fallback timeout when tapping a finished run: if no frame arrives, close with a final frame
    * synthesized from the status record. Covers a tap that connects after the relay is already gone
    * (a late tap, or a re-tap) — nothing will ever be published, so we manufacture the terminal
    * ourselves rather than wait forever. Sized generously so that a cross-node in-grace tap, whose
    * first frame waits on subscriber-slot gossip reaching the executing host, is not cut off before
    * its buffered head arrives; the cost is only a slower close on a genuinely gone producer.
    */
  private val FinalFrameTimeout: FiniteDuration = 15.seconds

  val backgroundQueryTap: Endpoint[
    Unit,
    (NamespaceId, UUID),
    Either[ServerError, NotFound],
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = graphScopedEndpoint("backgroundQueries")
    .tag("Cypher Query Language")
    .errorOut(serverError())
    .errorOutEither(notFoundError("No background query execution with that id in that graph."))
    .name("background-query-tap")
    .summary("Background Query Results Tap")
    .description(
      "WebSocket that streams a background-query execution's result rows as JSON text frames, " +
      "followed by a final frame keyed \"" + BackgroundQueryTapRelay.CompletionFrameKey + "\" once " +
      "the run terminates (carrying the terminal status, total row count, and how many buffered " +
      "rows were dropped). Rows produced before the first subscriber connects are buffered (up to " +
      s"${BackgroundQueryTapRelay.MaxBufferedRows} rows, retained until " +
      s"${BackgroundQueryTapRelay.GraceWindow.toSeconds}s after the run terminates), so connecting " +
      "with the execution id returned by the run endpoint sees the head of the results. " +
      "Best-effort with no delivery guarantees: slow consumers drop frames — reconcile against the " +
      "status record's totalRowCount.",
    )
    .in(CustomMethod.colonVerbPath[UUID]("id", "tap"))
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))

  protected[endpoints] val backgroundQueryTapLogic: ((NamespaceId, UUID)) => Future[
    Either[Either[ServerError, NotFound], PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = { case (namespaceId, executionId) =>
    implicit val mat: Materializer = appMethods.graph.materializer
    appMethods
      .getBackgroundQuery(namespaceId, executionId)
      .map {
        case Left(notFound) => Left(Right(notFound))
        case Right(record) =>
          val topic = TapBus.topicForBackgroundQuery(namespaceId, executionId)
          // The relay publishes rows and (when the run ends) its final frame, then completes the
          // stream — so we just pipe the subscription through and never inspect frame contents.
          val subscribed = appMethods.tapBus.subscriberSource(topic)
          val source: Source[WebSocketFrame, NotUsed] =
            if (isTerminal(record))
              // Finished: a relay still in its grace window will flush the buffered head + final frame
              // and complete us. But if it's already gone (a late tap or re-tap), nothing arrives — so
              // bound the wait and synthesize the final frame from the durable record.
              subscribed
                .idleTimeout(FinalFrameTimeout)
                .recoverWithRetries(
                  1,
                  { case _: TimeoutException => Source.single[WebSocketFrame](syntheticCompletionFrame(record)) },
                )
            else
              // Still running: the relay will stream rows and, when the run ends, its final frame and
              // complete the stream. Unbounded — a long query is not a stalled one.
              subscribed
          Right(Flow.fromSinkAndSourceCoupled(Sink.ignore, source))
      }(appMethods.graph.shardDispatcherEC)
  }

  private def isTerminal(record: BackgroundQueryStatus): Boolean = record.status != "started"

  private def syntheticCompletionFrame(record: BackgroundQueryStatus): WebSocketFrame.Text = {
    val json = BackgroundQueryTapRelay.completionFrame(
      status = record.status,
      totalRowCount = record.totalRowCount,
      droppedBufferedRows = 0L, // no buffered rows at this layer
      error = record.error,
    )
    WebSocketFrame.Text(json.noSpaces, finalFragment = true, rsv = None)
  }

  val backgroundQueryTapWebSocketEndpoints: List[ServerEndpoint[PekkoStreams with WebSockets, Future]] = List(
    backgroundQueryTap.serverLogic(backgroundQueryTapLogic),
  )
}
