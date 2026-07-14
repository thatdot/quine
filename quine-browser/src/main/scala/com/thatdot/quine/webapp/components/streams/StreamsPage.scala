package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, UiHintsSource}
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.queryui.{GraphSelector, WiretapStore}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.SystemStatusApi
import com.thatdot.quine.webapp.{AuthEvents, QuineUiOptions}

/** Top-level page component for the Streams UI.
  * Fetches the V2 OpenAPI spec, then renders the ingest + standing query panels.
  * Only available when the V2 API is enabled.
  */
object StreamsPage {

  /** The graph the streams page falls back to when no namespace is selected
    * (e.g. OSS, which only ever has the default graph).
    */
  private val DefaultNamespace: String = NamespaceParameter.defaultNamespaceParameter.namespaceId

  /** @param clusterStatusEnabled fetch the cluster member list and pass it down to the "create
    *   ingest at a position" selector. Only useful when the server exposes
    *   `/api/v2/system/status` (multi-node Enterprise deployments).
    * @param showNamespaceSelector render the header graph selector. Enterprise sets this; OSS leaves
    *   it off, matching the rest of the OSS UI (single graph, no selector anywhere).
    * @param selectedNamespaceVar the graph the page is scoped to. Ingest streams and standing
    *   queries are per-graph, so switching this re-scopes (and rebuilds) both panels. Defaults to
    *   `None`, which pins the page to the default graph.
    * @param knownNamespaces the graphs offered by the header selector.
    * @param onRefreshNamespaces invoked when the selector dropdown opens, to refresh the graph list.
    */
  def apply(
    options: QuineUiOptions,
    wiretapStore: WiretapStore,
    capabilities: StreamsCapabilities,
    clusterStatusEnabled: Boolean = false,
    showNamespaceSelector: Boolean = false,
    selectedNamespaceVar: Var[Option[String]] = Var(None),
    knownNamespaces: Signal[Seq[String]] = Val(Seq.empty),
    onRefreshNamespaces: Option[() => Unit] = None,
  ): HtmlElement = {
    val specState = Var[Pot[ParsedSpec]](Pot.Empty)
    // Cluster member positions, for the "create ingest at a position" selector. Empty
    // when the status endpoint is absent (single node) or reports no members.
    val memberIndices = Var[Seq[Int]](Seq.empty)

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
        if (clusterStatusEnabled) SystemStatusApi.memberIndices(baseUrlOpt).foreach(memberIndices.set)
      },
      div(
        cls := "d-flex align-items-center justify-content-between",
        height := "var(--cui-sidebar-header-height, 4rem)",
        h2(cls := "h2 mb-0 px-3", "Streams"),
        // Rendered unconditionally when enabled (like the Explorer's selector): it
        // shows the currently-scoped graph and refreshes the graph list on open, so
        // it is useful even before the first namespace-list fetch lands. Enterprise
        // only; OSS has a single graph and no selector. The wrapper class re-colors
        // the button for this light header — the shared selector defaults to the
        // light color meant for the Explorer's dark query bar.
        if (showNamespaceSelector)
          div(
            cls := "streams-namespace-selector",
            GraphSelector(
              selectedNamespaceVar = selectedNamespaceVar,
              knownNamespaces = knownNamespaces,
              onOpen = onRefreshNamespaces,
              defaultNamespace = Some(DefaultNamespace),
            ),
          )
        else emptyNode,
      ),
      // Content — rebuilt whenever the spec or the resolved graph changes. Resolve
      // the selection to a graph name and `.distinct` it: Airstream Vars emit even
      // when set to their current value, so a namespace-list refresh (e.g. opening
      // the selector) that re-sets the same selection would otherwise remount both
      // panels and wipe in-progress form state.
      child <-- specState.signal
        .combineWith(selectedNamespaceVar.signal.map(_.getOrElse(DefaultNamespace)).distinct)
        .map {
          case (Pot.Empty | Pot.Pending, _) =>
            div(
              cls := "text-center py-5",
              div(cls := "spinner-border text-primary", role := "status"),
              p(cls := "mt-3 text-body-secondary", "Loading API specification..."),
            )

          case (Pot.Failed(msg), _) =>
            div(cls := "alert alert-danger", msg)

          case (Pot.Ready(spec), graphName) =>
            val client = StreamsApiClient(spec, serverUrl, graphName)
            div(
              IngestStreamPanel(client, memberIndices.signal, editorConfig, capabilities),
              div(cls := "mt-4"),
              StandingQueryPanel(client, wiretapStore, editorConfig, capabilities),
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
