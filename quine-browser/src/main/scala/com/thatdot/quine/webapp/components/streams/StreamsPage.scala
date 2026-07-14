package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, UiHintsSource}
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.dataservice.{DataService, IngestStreamService, NamespaceService, StandingQueryService}
import com.thatdot.quine.webapp.queryui.GraphSelector
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.{AuthEvents, QuineUiOptions}

/** Top-level page component for the Streams UI.
  * Fetches the V2 OpenAPI spec, then renders the ingest + standing query panels.
  * Only available when the V2 API is enabled.
  */
object StreamsPage {

  /** @param showNamespaceSelector render the header graph selector. Enterprise sets this; OSS
    *   leaves it off, matching the rest of the OSS UI (single graph, no selector anywhere). The
    *   selector reads and switches the shared current namespace through `dataService`, so the
    *   ingest/standing-query feeds (which key off that same namespace) re-scope automatically.
    */
  def apply(
    options: QuineUiOptions,
    dataService: DataService,
    capabilities: StreamsCapabilities,
    showNamespaceSelector: Boolean = false,
  ): HtmlElement = {
    val specState = Var[Pot[ParsedSpec]](Pot.Empty)

    val specUrl = options.documentationV2Url
    val serverUrl = options.serverUrl.getOrElse("")
    // Empty string means "same origin" (mirrors ClientRoutes.baseUrlOpt)
    val baseUrlOpt = options.serverUrl.toOption.filter(_.nonEmpty)
    // QuinePattern feature flag, threaded down to the embedded Cypher editors so they connect to
    // the language server only when it exists (mirrors the nav-bar query bar's gating).
    val qpEnabled = options.qpEnabled.getOrElse(false)
    // Editor-connection config threaded to the embedded Cypher editors (see EmbeddedEditorConfig).
    val editorConfig = EmbeddedEditorConfig(qpEnabled, baseUrlOpt)

    div(
      cls := "container-fluid px-3",
      onMountCallback { _ =>
        specState.set(Pot.Pending)
        fetchAndParse(specUrl).foreach {
          case Right(spec) => specState.set(Pot.Ready(spec))
          case Left(err) => specState.set(Pot.Failed(err))
        }
      },
      div(
        cls := "d-flex align-items-center justify-content-between",
        height := "var(--cui-sidebar-header-height, 4rem)",
        h2(cls := "h2 mb-0 px-3", "Streams"),
        // Enterprise only; OSS has a single graph and no selector. Wired to the shared
        // namespace spine exactly like the Explorer's selector: it shows the current graph,
        // switches it via `SetNamespace`, and refreshes the list on open. The wrapper class
        // re-colors the button for this light header (the shared selector defaults to the
        // light color meant for the Explorer's dark query bar).
        if (showNamespaceSelector)
          div(
            cls := "streams-namespace-selector",
            GraphSelector(
              selected = dataService.currentNamespaceSignal.map(_.namespaceId),
              onSelect = Observer[String] { name =>
                NamespaceParameter(name)
                  .foreach(ns => dataService.namespaceDispatch.onNext(NamespaceService.SetNamespace(ns)))
              },
              knownNamespaces = dataService.namespacesSignal.map(_.map(_.namespaceId)),
              onOpen = Some(() => dataService.namespaceDispatch.onNext(NamespaceService.RefreshNamespaces)),
              defaultNamespace = Some(NamespaceParameter.defaultNamespaceParameter.namespaceId),
            ),
          )
        else emptyNode,
      ),
      // Content
      child <-- specState.signal.map {
        case Pot.Empty | Pot.Pending =>
          div(
            cls := "text-center py-5",
            div(cls := "spinner-border text-primary", role := "status"),
            p(cls := "mt-3 text-body-secondary", "Loading API specification..."),
          )

        case Pot.Failed(msg) =>
          div(cls := "alert alert-danger", msg)

        case Pot.Ready(spec) =>
          // The client is rebuilt per selected graph so mutations target the same
          // namespace the shared list feeds are scoped to. `.distinct` guards against
          // a namespace-list refresh re-emitting the same selection and remounting both
          // panels (which would wipe in-progress form state).
          div(
            child <-- dataService.currentNamespaceSignal.distinct.map { ns =>
              val client = StreamsApiClient(spec, serverUrl, ns.namespaceId)
              div(
                IngestStreamPanel(
                  client = client,
                  ingests = dataService.ingestStreamsSignal,
                  onRefresh = () => dataService.ingestStreamDispatch.onNext(IngestStreamService.RefreshIngestStreams),
                  memberIndices = dataService.memberIndicesSignal,
                  editorConfig = editorConfig,
                  capabilities = capabilities,
                ),
                div(cls := "mt-4"),
                StandingQueryPanel(
                  client = client,
                  standingQueries = dataService.standingQueriesSignal,
                  onRefresh =
                    () => dataService.standingQueryDispatch.onNext(StandingQueryService.RefreshStandingQueries),
                  wiretap = dataService,
                  editorConfig = editorConfig,
                  capabilities = capabilities,
                ),
              )
            },
          )

        case _ => emptyNode
      },
    )
  }

  private def fetchAndParse(url: String): Future[Either[String, ParsedSpec]] =
    (for {
      response <- dom.fetch(url).toFuture
      text <- response.text().toFuture
    } yield
      if (response.status == 401) {
        AuthEvents.unauthorized.emit(())
        Left(s"HTTP ${response.status}")
      } else if (response.ok) OpenApiParser.parse(text).map(attachUiHints)
      else Left(s"HTTP ${response.status}")).recover { case ex: Throwable =>
      dom.console.error("Failed to load API specification:", ex.getMessage)
      Left("Could not connect to the server.")
    }

  /** Attach the Streams UI overlay to the parsed spec and report any
    * drift (hints referring to schemas/fields not present in the spec) as a
    * console warning. The attached hints drive field ordering, promotion, and
    * hiding inside [[SchemaFormRenderer]].
    */
  private def attachUiHints(spec: ParsedSpec): ParsedSpec = {
    val source = StreamsUiHints.source
    UiHintsSource.checkDrift(source, spec.schemas, org.scalajs.dom.console.warn(_))
    spec.copy(hints = source)
  }
}
