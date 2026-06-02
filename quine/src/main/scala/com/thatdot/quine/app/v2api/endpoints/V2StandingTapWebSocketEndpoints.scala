package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{Endpoint, path, webSocketBodyRaw}
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.ErrorResponse.{NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{notFoundError, serverError}
import com.thatdot.api.v2.{ResourceName, V2EndpointDefinitions}
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.outputs2.query.standing.{SqTapStage, TapBus}
import com.thatdot.quine.app.v2api.definitions.{CommonParameters, CustomMethod, GraphScopedEndpoints, QuineApiMethods}
import com.thatdot.quine.graph.NamespaceId

/** V2 Tapir WebSocket endpoints for tapping into the V2 standing query pipeline at three stages.
  *
  * - `sqRawTap`: raw [[com.thatdot.quine.graph.StandingQueryResult]] values before any workflow processing
  * - `sqPreEnrichmentTap`: values after `preEnrichmentTransformation`, before Cypher enrichment
  * - `sqPostEnrichmentTap`: values after all workflow stages (what destinations receive)
  *
  * All three endpoints use Pekko distributed pub/sub so that results produced on any cluster node
  * are visible to a client connected to any node. Messages are dropped (not backpressured) when
  * the client is slow or the buffer is full. There are no delivery guarantees.
  */
trait V2StandingTapWebSocketEndpoints extends V2EndpointDefinitions with CommonParameters with GraphScopedEndpoints {
  val appMethods: QuineApiMethods
  implicit protected def logConfig: LogConfig

  // ── base endpoint ─────────────────────────────────────────────────────────

  private val standingTapBase: Endpoint[Unit, NamespaceId, Either[ServerError, NotFound], Unit, Any] =
    graphScopedEndpoint("standingQueries")
      .tag("Standing Queries")
      .errorOut(serverError())
      .errorOutEither(notFoundError("Standing Query or output not found."))

  // ── raw tap: before any workflow processing ────────────────────────────────

  val sqRawTap: Endpoint[
    Unit,
    (NamespaceId, ResourceName),
    Either[ServerError, NotFound],
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = standingTapBase
    .name("sq-raw-tap")
    .summary("Standing Query Raw Tap")
    .description(
      "WebSocket that streams raw StandingQueryResult values for the named Standing Query, " +
      "before any output workflow processing. Results are aggregated across all cluster nodes. " +
      "This is a tapping/visualization tool with no delivery guarantees.",
    )
    .in(
      CustomMethod
        .colonVerbPath[ResourceName]("standingQueryName", "tap")
        .description("Unique name for a Standing Query."),
    )
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))

  protected[endpoints] val sqRawTapLogic: ((NamespaceId, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = { case (namespaceId, sqName) =>
    implicit val mat: Materializer = appMethods.graph.materializer
    appMethods
      .getSQ(sqName.value, namespaceId)
      .map {
        case Left(notFound) => Left(Right(notFound))
        case Right(_) =>
          val topic = TapBus.topicForSq(namespaceId, sqName.value, "_raw_", SqTapStage.Raw)
          val source = appMethods.tapBus.subscriberSource(topic)
          Right(Flow.fromSinkAndSourceCoupled(Sink.ignore, source))
      }(appMethods.graph.shardDispatcherEC)
  }

  // ── pre-enrichment tap: after preEnrichmentTransformation ─────────────────

  val sqPreEnrichmentTap: Endpoint[
    Unit,
    (NamespaceId, ResourceName, ResourceName),
    Either[ServerError, NotFound],
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = standingTapBase
    .name("sq-pre-enrichment-tap")
    .summary("Standing Query Pre-Enrichment Tap")
    .description(
      "WebSocket that streams values after the preEnrichmentTransformation stage but before " +
      "Cypher enrichment. Results are aggregated across all cluster nodes. " +
      "This is a tapping/visualization tool with no delivery guarantees.",
    )
    .in(path[ResourceName]("standingQueryName").description("Unique name for a Standing Query."))
    .in("outputs")
    .in(
      CustomMethod
        .colonVerbPath[ResourceName]("standingQueryOutputName", "tap_pre_enrichment")
        .description("Unique name for a Standing Query Output."),
    )
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))

  protected[endpoints] val sqPreEnrichmentTapLogic: ((NamespaceId, ResourceName, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = { case (namespaceId, sqName, outputName) =>
    implicit val mat: Materializer = appMethods.graph.materializer
    appMethods
      .getSQ(sqName.value, namespaceId)
      .map {
        case Left(notFound) => Left(Right(notFound))
        case Right(sq) if !sq.outputs.exists(_.name.value == outputName.value) =>
          Left(Right(NotFound(s"No output named '${outputName.value}' on Standing Query '${sqName.value}'.")))
        case Right(_) =>
          val topic = TapBus.topicForSq(namespaceId, sqName.value, outputName.value, SqTapStage.PreEnrichment)
          val source = appMethods.tapBus.subscriberSource(topic)
          Right(Flow.fromSinkAndSourceCoupled(Sink.ignore, source))
      }(appMethods.graph.shardDispatcherEC)
  }

  // ── post-enrichment tap: after full workflow, before destinations ──────────

  val sqPostEnrichmentTap: Endpoint[
    Unit,
    (NamespaceId, ResourceName, ResourceName),
    Either[ServerError, NotFound],
    PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame],
    WebSockets with PekkoStreams,
  ] = standingTapBase
    .name("sq-post-enrichment-tap")
    .summary("Standing Query Post-Enrichment Tap")
    .description(
      "WebSocket that streams values after all workflow stages (the same data that destinations " +
      "receive). Results are aggregated across all cluster nodes. " +
      "This is a tapping/visualization tool with no delivery guarantees.",
    )
    .in(path[ResourceName]("standingQueryName").description("Unique name for a Standing Query."))
    .in("outputs")
    .in(
      CustomMethod
        .colonVerbPath[ResourceName]("standingQueryOutputName", "tap")
        .description("Unique name for a Standing Query Output."),
    )
    .get
    .out(webSocketBodyRaw(PekkoStreams).autoPongOnPing(true))

  protected[endpoints] val sqPostEnrichmentTapLogic: ((NamespaceId, ResourceName, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], PekkoStreams.Pipe[WebSocketFrame, WebSocketFrame]],
  ] = { case (namespaceId, sqName, outputName) =>
    implicit val mat: Materializer = appMethods.graph.materializer
    appMethods
      .getSQ(sqName.value, namespaceId)
      .map {
        case Left(notFound) => Left(Right(notFound))
        case Right(sq) if !sq.outputs.exists(_.name.value == outputName.value) =>
          Left(Right(NotFound(s"No output named '${outputName.value}' on Standing Query '${sqName.value}'.")))
        case Right(_) =>
          val topic = TapBus.topicForSq(namespaceId, sqName.value, outputName.value, SqTapStage.PostEnrichment)
          val source = appMethods.tapBus.subscriberSource(topic)
          Right(Flow.fromSinkAndSourceCoupled(Sink.ignore, source))
      }(appMethods.graph.shardDispatcherEC)
  }

  val standingQueryTapWebSocketEndpoints: List[ServerEndpoint[PekkoStreams with WebSockets, Future]] = List(
    sqRawTap.serverLogic(sqRawTapLogic),
    sqPreEnrichmentTap.serverLogic(sqPreEnrichmentTapLogic),
    sqPostEnrichmentTap.serverLogic(sqPostEnrichmentTapLogic),
  )
}
