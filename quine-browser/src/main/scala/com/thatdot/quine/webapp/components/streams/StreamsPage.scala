package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.ParsedSpec
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.dataservice.{
  BackgroundQueryService,
  DataService,
  IngestStreamService,
  JobService,
  NamespaceService,
  StandingQueryService,
}
import com.thatdot.quine.webapp.openapi.{ApiSpecCache, FormUiHints}
import com.thatdot.quine.webapp.queryui.GraphSelector
import com.thatdot.quine.webapp.util.Pot

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

    def cancelRun(id: String): Unit =
      dataService.backgroundQueryDispatch.onNext(BackgroundQueryService.CancelBackgroundQuery(id))

    def deleteRun(id: String): Unit =
      dataService.backgroundQueryDispatch.onNext(BackgroundQueryService.DeleteBackgroundQuery(id))

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
          div(
            // The client is rebuilt per selected graph so mutations target the same
            // namespace the shared list feeds are scoped to. `.distinct` guards against
            // a namespace-list refresh re-emitting the same selection and remounting both
            // panels (which would wipe in-progress form state).
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
                div(cls := "mt-4"),
                // Graph-scoped like the two panels above it: executions belong to the graph
                // their query ran against.
                BackgroundQueryPanel(
                  client = client,
                  runs = dataService.backgroundQueriesSignal,
                  namespace = dataService.currentNamespaceSignal.map(_.namespaceId),
                  onRefresh =
                    () => dataService.backgroundQueryDispatch.onNext(BackgroundQueryService.RefreshBackgroundQueries),
                  onCancel = cancelRun,
                  onDelete = deleteRun,
                  wiretap = dataService,
                  editorConfig = editorConfig,
                  capabilities = capabilities,
                ),
              )
            },
            div(cls := "mt-4"),
            // Deliberately outside the per-namespace `child <--` above: scheduled jobs are
            // cluster-wide, so switching graphs changes nothing here — and remounting the panel
            // on a switch would discard a half-filled create form for no reason. Its client is
            // built once, with the default graph, which the job endpoints ignore anyway (they
            // carry no graphName path parameter).
            JobsPanel(
              client = StreamsApiClient(spec, serverUrl),
              jobs = dataService.jobsSignal,
              // Passed as a Signal rather than mounting inside the per-namespace block above:
              // the create form needs the current selection, but remounting the panel on every
              // graph switch would discard a half-filled form for a list that never changes.
              namespace = dataService.currentNamespaceSignal.map(_.namespaceId),
              runs = dataService.backgroundQueriesSignal,
              onRefresh = () => dataService.jobDispatch.onNext(JobService.RefreshJobs),
              onCancelRun = cancelRun,
              onDeleteRun = deleteRun,
              wiretap = dataService,
              editorConfig = editorConfig,
              capabilities = capabilities,
            ),
          )

        case _ => emptyNode
      },
    )
  }

  // Fetching and hint-attaching moved to `ApiSpecCache`, so this page and the query bar's
  // run-in-background dialog share one request for a document neither of them changes.
  private def fetchAndParse(url: String): Future[Either[String, ParsedSpec]] =
    ApiSpecCache.load(url, FormUiHints.source)
}
