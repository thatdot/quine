package com.thatdot.quine.webapp.queryui

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent._
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{literal => jsObj}
import scala.util.{Failure, Random, Success}

import cats.data.Validated
import com.raquo.laminar.api.L._
import endpoints4s.Invalid
import io.circe.Json
import org.scalajs.dom
import org.scalajs.dom.{document, window}
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.api.v2.QueryWebSocketProtocol.QueryInterpreter
import com.thatdot.quine.Util.escapeHtml
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.v2api.routes.{V2QuerySort, V2QuickQuery, V2UiNodeQuickQuery}
import com.thatdot.quine.webapp.components.streams.EmbeddedEditorConfig
import com.thatdot.quine.webapp.components.{
  ContextMenu,
  ContextMenuItem,
  ContextMenuModel,
  Loader,
  MenuAction,
  MenuSection,
  Toast,
  ToastAction,
  ToastMessage,
  ToastVariant,
  ToolbarButton,
  VisData,
  VisIndirectMouseEvent,
  VisNetwork,
}
import com.thatdot.quine.webapp.dataservice.{
  DataService,
  QueryUiConfigService,
  SaveFailed,
  SaveResult,
  SaveSucceeded,
  WiretapHandler,
  WiretapOwner,
}
import com.thatdot.quine.webapp.resultspanel.cards.{
  CardDefaults,
  CardId,
  CardKind,
  CardPopup,
  CardSnapshot,
  CardsStore,
  MinimizedDrawer,
  TapCardQuery,
}
import com.thatdot.quine.webapp.resultspanel.streaming.LiveStream
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModal
import com.thatdot.quine.webapp.resultspanel.{
  HistoryEntry,
  HistoryState,
  LiveSource,
  ResultOutcome,
  ResultsContent,
  SourceStatus,
  TapCatalogEntry,
  TapEntry,
  TapOutput,
  TapTarget,
}
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.{QuickQueryConversions, V2ApiTypes}
import com.thatdot.quine.webapp.{History, QueryUiOptions, Styles}
import com.thatdot.{visnetwork => vis}

object QueryUi {

  case class Props(
    routes: ClientRoutes,
    dataService: DataService,
    graphData: VisData,
    initialQuery: String = "",
    nodeResultSizeLimit: Long = 100,
    hostColors: Vector[String] = Vector("#97c2fc", "green", "purple", "blue", "red", "orange", "yellow", "black"),
    onNetworkCreate: Option[js.Function1[vis.Network, js.Any]] = None,
    isQueryBarVisible: Boolean = true,
    showEdgeLabels: Boolean = true,
    showHostInTooltip: Boolean = true,
    initialAtTime: Option[Long] = None,
    initialLayout: NetworkLayout = NetworkLayout.Graph,
    edgeQueryLanguage: QueryLanguage = QueryLanguage.Cypher,
    queryMethod: QueryMethod = QueryMethod.WebSocket,
    qpEnabled: Boolean = false,
    initialNamespace: NamespaceParameter = NamespaceParameter.defaultNamespaceParameter,
    permissions: Option[Set[String]] = None,
    namespaceSignal: Option[Signal[NamespaceParameter]] = None,
    refreshSignal: Option[EventStream[Unit]] = None,
    queryBarTrailing: Option[HtmlElement] = None,
    invalidatedNamespaces: Option[EventStream[NamespaceParameter]] = None,
  )

  case class State(
    query: String,
    pendingTextQueries: Set[QueryId],
    queryBarColor: Option[String],
    sampleQueries: Vector[SampleQuery],
    history: History[QueryUiEvent],
    animating: Boolean,
    foundNodesCount: Option[Int],
    foundEdgesCount: Option[Int],
    runningQueryCount: Long,
    uiNodeQuickQueries: Vector[V2UiNodeQuickQuery],
    uiNodeAppearances: Vector[UiNodeAppearance],
    atTime: Option[Long],
    namespace: NamespaceParameter,
  )

  private case class ContextMenuState(x: Double, y: Double, model: ContextMenuModel)

  def apply(props: Props): HtmlElement = {

    val stateVar = Var(
      State(
        query = props.initialQuery,
        pendingTextQueries = Set.empty,
        queryBarColor = None,
        sampleQueries = Vector.empty,
        history = History.empty,
        animating = false,
        foundNodesCount = None,
        foundEdgesCount = None,
        runningQueryCount = 0,
        uiNodeQuickQueries = V2QuickQuery.defaults,
        uiNodeAppearances = Vector.empty,
        atTime = props.initialAtTime,
        namespace = props.initialNamespace,
      ),
    )

    // Separate Vars for state containing Laminar elements (to avoid remounting issues)
    val contextMenuVar = Var(Option.empty[ContextMenuState])
    val nodePopupVar = Var(Option.empty[NodePopupState])
    // How to return from an open node-properties popup to the context menu that spawned it.
    // Takes (x, y) so the context menu reopens where the user dragged the popup, not
    // where the original right-click happened.
    val nodePopupBackVar = Var(Option.empty[(Double, Double) => Unit])
    // Re-fetch properties for the currently open popup at a given position.
    // Set alongside nodePopupBackVar when "View Properties" opens the popup.
    val nodePopupRefreshVar = Var(Option.empty[(Double, Double) => Unit])
    val toastVar = Var(Option.empty[ToastMessage])
    val resultsVar = Var(Option.empty[ResultsContent])
    // Ephemeral result-count overlay (design doc §5): one event per completed query, never
    // for errors. Emitted from submitQuery's Success branches below.
    val resultCountBus = new EventBus[ResultCountEvent]
    val bookmarkDraftVar: Var[Option[SampleQueryDraft]] = Var(None)

    /** True while a namespace snapshot (initial-load or namespace-switch) is being applied to
      * the canvas, driving the loading spinner. Exactly one in-flight restore owns this flag at
      * a time: each restore sets it true before applying, then clears it in a `finally`. Stale,
      * superseded restores (see `applySnapshot`'s `stillCurrent` guard) never touch it, so a
      * slow restore can't clear the flag out from under a newer one still in progress.
      */
    val restoringVar: Var[Boolean] = Var(false)

    val resultsCollapsedVar = Var(false)
    val resultsHistory = HistoryState.empty
    // The Source picker's standing-query catalog: the shared per-namespace feed mapped to
    // picker entries. Permission-gated at construction — without StandingQueryRead the
    // signal is never subscribed, so no poll ever starts.
    val canReadStandingQueries: Boolean = props.permissions.forall(_.contains("StandingQueryRead"))
    // Whether this user may read the explorer's UI configuration (appearances, sample
    // queries, quick queries). Gates the shared-feed bindings — an unbound signal never polls.
    val canViewUiData: Boolean =
      props.permissions.forall(perms => Set("StoredQueryRead", "NodeAppearanceRead").subsetOf(perms))
    // The Explorer Settings modal manages shared configuration (sample queries, quick
    // queries, node appearances), so its junk-drawer entry is keyed on the write
    // permissions rather than the reads that merely using the Explorer requires —
    // the same gate the old dedicated settings page used.
    val canEditExplorerSettings: Boolean =
      props.permissions.forall(perms => Set("StoredQueryWrite", "NodeAppearanceWrite").subsetOf(perms))
    val settingsModalOpenVar: Var[Boolean] = Var(false)
    // Per-browser "show in graph menu" preference for graph feeds: which ones get a pill
    // in the canvas's bottom-left menu (GraphFeedChips). Shared between the settings
    // modal (where the preference is set) and the chips (which it filters).
    val graphMenuPrefs = new GraphMenuPrefs(props.dataService.currentNamespaceSignal.map(_.namespaceId))
    val tapCatalogSignal: Signal[Pot[Vector[TapCatalogEntry]]] =
      if (canReadStandingQueries)
        props.dataService.standingQueriesSignal.map(
          _.map(sqs =>
            sqs
              .map(sq =>
                TapCatalogEntry(
                  sq.name,
                  sq.outputs.map(o =>
                    TapOutput(o.name, o.hasEnrichment, o.hasTransformation, o.enrichmentQuery, o.transformationType),
                  ),
                  sq.pattern.flatMap(_.query),
                ),
              )
              .toVector,
          ),
        )
      else Val(Pot.Empty)
    // One-shot resolution of the query behind a tap target's data (design doc: "render the
    // standing query / enrichment query in the tap card too"), read off the catalog at open
    // time. A resolved query is carried through entry swaps (`CardsStore.replaceTapTableEntry`),
    // so a later catalog edit doesn't retroactively change an already-open card; a card that
    // opened or restored without one (catalog still loading, pre-capture snapshot) is
    // backfilled on its next restart via the same call.
    def resolveTapCardQuery(target: TapTarget): Option[TapCardQuery] = {
      val catalogObs = tapCatalogSignal.observe(unsafeWindowOwner)
      val catalogNow = catalogObs.now().toOption.getOrElse(Vector.empty)
      catalogObs.killOriginalSubscription()
      TapCardQuery.resolve(catalogNow, target)
    }
    // Taps open under the explorer's own owner; the facade scopes the panel's source list to it.
    val tapSubscriptions = new PanelTapSubscriptions(props.dataService, WiretapOwner("explorer"))
    // TODO(lane G cleanup): the old ResultsStore/ResultsPanel/CanvasDoor drawer surface is no
    // longer constructed or mounted — the card system (CardsStore, below) replaced it for adhoc
    // runs and the tap modal replaced AddTapChooser for taps (design doc §3 phase 1/2: "the old
    // drawer no longer opens for adhoc runs"). `resultsHistory` is now vestigial — nothing
    // populates it, and the snapshot restore/persist paths ignore it (its snapshot-format
    // fields are kept for compatibility but persisted empty); `resultsCollapsedVar` still
    // feeds the snapshot format below (see `persistCurrentNamespace`). Once Lane G (G3/G5)
    // deletes ResultsPanel/ResultsHeader/HistoryState, replace these with the card system's
    // own persistence path (A11, not yet implemented — see laneA-integration.md §4).

    // ── Result cards (exploration UI redesign) ──────────────────────────────────────
    // The card system's store: adhoc query results auto-capture into floating cards
    // (design doc §3). Tap cards are created by the tap modal below via
    // addTapTableCard; tap lifecycle effects (close/stop/restart/go-live)
    // are wired to the same PanelTapSubscriptions/WiretapService the old drawer used.
    // `submitQuery` is defined later in this method (it needs many locals declared in between,
    // e.g. wsClients/textQuery/nodeQuery), so `onReRun` can't reference it directly here without
    // a forward-reference compile error — it goes through this mutable forwarding cell instead,
    // assigned once `submitQuery` exists (just before the cardsStore/tapModal wiring is used).
    // Returns whether the run was actually accepted (false on submitQuery's early-outs:
    // blank query, pending text query) so `CardsStore.reRun` only claims the edit
    // association for runs that will really produce content.
    var submitQueryRef: (String, QueryLanguage) => Boolean = (_, _) => false
    // Restarting a stopped tap-table card reopens its tap and, once the fresh LiveSource
    // arrives, swaps a new TapEntry into the existing card (a frozen LiveStream is not
    // revivable — see CardsStore.stopCard). Target keys awaiting that swap are tracked
    // here and resolved by the same sources binder that resolves pending opens below.
    val pendingTabularRestartsVar: Var[Set[String]] = Var(Set.empty)
    // Backstop for opens/restarts whose source never arrives (WS down, SQ deleted since
    // the catalog loaded): a still-pending key is dropped after this long and the failure
    // surfaced via toast — otherwise the user gets no feedback at all and the dangling
    // key spawns a surprise card when a later source (e.g. a session restore) reuses it.
    val PendingTapTimeoutMs = 10000
    def timeoutPendingTap(pendingVar: Var[Set[String]], target: TapTarget): Unit = {
      val _ = window.setTimeout(
        () =>
          if (pendingVar.now().contains(target.key)) {
            pendingVar.update(_ - target.key)
            toastVar.set(
              Some(
                ToastMessage(s"Could not open tap on ${target.label}: no response from the server", ToastVariant.Error),
              ),
            )
          },
        PendingTapTimeoutMs.toDouble,
      )
    }
    val cardsStore = new CardsStore(
      liveContent = resultsVar.signal,
      onEditQuery = Observer[String](q => stateVar.update(_.copy(queryBarColor = None, query = q))),
      onReRun = (query, language) => submitQueryRef(query, language),
      onCloseTap = target => tapSubscriptions.close(target.key),
      // Stop = the store froze the card's buffer; free the tap subscription server-side.
      onStopTap = target => tapSubscriptions.close(target.key),
      // Restart = reopen the tap; the swap happens in resolvePendingTabularOpens. On
      // failure (timeout below, or an errored source in the binder) the card just stays
      // stopped — honest state.
      onRestartTap = target => {
        pendingTabularRestartsVar.update(_ + target.key)
        tapSubscriptions.open(target.key, target)
        timeoutPendingTap(pendingTabularRestartsVar, target)
      },
    )

    // One tap-card stream session: buffers `src`'s frames under the card's sample budget.
    // Filling the budget ends the session — the stream freezes itself, the entry is marked
    // ended, and the tap subscription is released so the server stops streaming. Fetch-more
    // and go-live reopen through the restart flow below (CardsStore seeds the continuation).
    def connectBudgetedTapStream(src: LiveSource, target: TapTarget): TapEntry = {
      val stream = new LiveStream
      val entry = new TapEntry(src, stream)
      stream.connect(
        src.records,
        budget = () => cardsStore.sampleBudgetFor(target),
        onBudgetFilled = () => {
          entry.ended.set(true)
          tapSubscriptions.close(target.key)
        },
      )
      entry
    }

    // ── Unified tap modal (exploration UI redesign) ─────────────────────────────────
    val tapModalOpenVar: Var[Boolean] = Var(false)
    def openTapModal(): Unit = tapModalOpenVar.set(true)

    // Tabular-destination opens (design doc §2 step 2 "Results card"): open the tap via
    // PanelTapSubscriptions, then once its LiveSource appears, spawn a tap-table card for it.
    // Target keys awaiting a card are tracked here and resolved by the lifecycleMods binder
    // below — the same "open now, select/create once the source arrives" shape as
    // TapsState.pendingSelectKey/sync, scoped to the card system instead.
    val pendingTabularOpensVar: Var[Set[String]] = Var(Set.empty)
    def openTabularTap(target: TapTarget): Unit = {
      pendingTabularOpensVar.update(_ + target.key)
      tapSubscriptions.open(target.key, target)
      timeoutPendingTap(pendingTabularOpensVar, target)
    }
    val resolvePendingTabularOpens: Binder[HtmlElement] = tapSubscriptions.sources --> { srcs =>
      val pendingOpens = pendingTabularOpensVar.now()
      val pendingRestarts = pendingTabularRestartsVar.now()
      if (pendingOpens.nonEmpty || pendingRestarts.nonEmpty)
        srcs.foreach { src =>
          src.tapTarget.foreach { target =>
            if (pendingRestarts.contains(target.key) || pendingOpens.contains(target.key)) {
              // One-shot read of the producer's current status (observe + kill — a Signal
              // has no ownerless `now`): a source that arrives already errored must not
              // become a card the user never asked to see fail.
              val statusObs = src.status.observe(unsafeWindowOwner)
              val statusNow = statusObs.now()
              statusObs.killOriginalSubscription()
              statusNow match {
                case SourceStatus.Error(message) =>
                  // Failed open/restart: drop the pending key (a dangling key would spawn
                  // a surprise card from a later source on the same target), free the tap,
                  // and surface the failure. A restarting card just stays stopped.
                  pendingTabularRestartsVar.update(_ - target.key)
                  pendingTabularOpensVar.update(_ - target.key)
                  tapSubscriptions.close(target.key)
                  toastVar.set(
                    Some(ToastMessage(s"Could not open tap on ${target.label}: $message", ToastVariant.Error)),
                  )
                case _ if pendingRestarts.contains(target.key) =>
                  val entry = connectBudgetedTapStream(src, target)
                  val installed = cardsStore.replaceTapTableEntry(target, entry, resolveTapCardQuery(target))
                  if (!installed) {
                    // Card closed while the reopen was in flight: free the reopened tap + stream.
                    entry.stream.freeze()
                    tapSubscriptions.close(target.key)
                  }
                  pendingTabularRestartsVar.update(_ - target.key)
                case _ =>
                  val entry = connectBudgetedTapStream(src, target)
                  cardsStore.addTapTableCard(target, entry, resolveTapCardQuery(target))
                  pendingTabularOpensVar.update(_ - target.key)
              }
            }
          }
        }
    }

    /** Install a restored card set (from a namespace snapshot — reload or namespace
      * switch; design doc §6 / checklist A11+C10). States are rebuilt via
      * [[CardSnapshot.toState]] (adhoc cards with their full saved outcome, tap cards
      * from their coordinates); tap-table cards that were live at save time then
      * reconnect through the same restart flow a manual Restart uses — the card sits
      * `stopped` on its placeholder entry until the fresh source arrives and
      * `resolvePendingTabularOpens` swaps it in. If the tap can't reopen (SQ deleted,
      * WS down) the card simply stays stopped.
      */
    def restoreCards(snapshots: Seq[CardSnapshot], expandedCardId: Option[String]): Unit = {
      val states = snapshots.flatMap(CardSnapshot.toState).toVector
      cardsStore.restore(states, expandedCardId.map(CardId(_)))
      snapshots.foreach { snap =>
        if (snap.kind == CardSnapshot.KindTapTable && !snap.stopped)
          CardSnapshot.tapTargetOf(snap).foreach { target =>
            pendingTabularRestartsVar.update(_ + target.key)
            tapSubscriptions.open(target.key, target)
            timeoutPendingTap(pendingTabularRestartsVar, target)
          }
      }
    }

    def cardTapTarget(kind: CardKind): Option[TapTarget] = kind match {
      case CardKind.TapTableCard(target, _, _) => Some(target)
      case _: CardKind.AdhocCard => None
    }

    // Currently-tapped keys — drives the pipeline tree's ✓ badge.
    val tappedKeys: Signal[Set[String]] =
      cardsStore.cards.map(_.flatMap(c => cardTapTarget(c.kind).map(_.key)).toSet)

    val editorConfig = EmbeddedEditorConfig(props.qpEnabled, props.routes.baseUrlOpt)

    val iconFontFace = "Ionicons"

    def toggleBookmarkDialog(): Unit = {
      val query = stateVar.now().query.trim
      if (query.nonEmpty) {
        bookmarkDraftVar.update {
          case Some(_) => None
          case None =>
            val existing = stateVar.now().sampleQueries.zipWithIndex.find(_._1.query.trim == query)
            Some(SampleQueryDraft.forQuery(query, existing))
        }
      }
    }

    lazy val cypherQueryRegex = js.RegExp(
      raw"^\s*(optional|match|return|unwind|create|foreach|merge|call|load|with|explain|show|profile)[^a-z]",
      flags = "i",
    )

    def guessQueryLanguage(query: String): QueryLanguage = cypherQueryRegex.test(query) match {
      case true => QueryLanguage.Cypher
      case false => QueryLanguage.Gremlin
    }

    /** Surface a failed query run as an error toast. A failed run never enters
      * `resultsVar`/the card system — errors are ephemeral, not tracked cards.
      *
      * Server compile errors embed their internal position representation verbatim
      * (`... @ Some(Position(1,18,17,SourceText(match (n) ...))) match (n) ... ^`) —
      * strip that tail: the human-readable part ("CompileError at 1.18 ...") already
      * carries the location, and the user's query is sitting right there in the bar.
      */
    def showQueryError(message: String): Unit = {
      val cleaned = message.replaceAll("""\s*@\s*Some\(Position\([\s\S]*""", "").trim
      val text = if (cleaned.nonEmpty) cleaned else "Query failed"
      toastVar.set(Some(ToastMessage(text, ToastVariant.Error)))
    }

    // Mutable refs
    var network: Option[vis.Network] = None
    var layout: NetworkLayout = props.initialLayout
    val visualization: GraphVisualization = new VisNetworkVisualization(props.graphData, () => network)
    val pinTracker = new PinTracker(visualization)
    var physicsRequestCount: Int = 0
    // Websocket clients keyed by namespace — each namespace gets its own independent connection,
    // so switching namespaces never disrupts in-flight queries on the previous namespace.
    val wsClients = mutable.Map.empty[String, Future[WebSocketQueryClient]]
    val wsClientsV2 = mutable.Map.empty[String, Future[V2WebSocketQueryClient]]
    // Bumped on every setNamespace call; a snapshot-restore callback only applies its result
    // if this still matches the version it captured, so an older in-flight switch can't
    // clobber state after a newer switch has already started.
    var setNamespaceVersion: Int = 0
    // Set on history mutations; the periodic autosave only writes when this is true. Viewport
    // pan/zoom and pin/unpin don't set it — they're flushed on namespace switch and
    // beforeunload instead, which both save unconditionally.
    var persistenceDirty: Boolean = false

    final case class CollapsedCluster(nodeIds: Seq[String], clusterId: String, name: String)

    final case class NamespaceSnapshot(
      nodes: js.Array[vis.Node],
      edges: js.Array[vis.Edge],
      query: String,
      history: History[QueryUiEvent],
      foundNodesCount: Option[Int],
      foundEdgesCount: Option[Int],
      /** Historical-query timestamp from the time-travel control (`None` = live/latest), not
        * when this snapshot was taken.
        */
      atTime: Option[Long],
      pinnedNodes: Set[String],
      viewPosition: CanvasPosition,
      viewScale: Double,
      layout: NetworkLayout,
      collapsedClusters: Seq[CollapsedCluster],
      clusterPositions: Map[String, CanvasPosition],
      resultsEntries: Vector[HistoryEntry],
      resultsCurrentIdx: Int,
      resultsCollapsed: Boolean,
      /** The card system's cards, kept in serialized form even in this in-memory snapshot:
        * live tap streams can't survive a namespace switch anyway (the wiretap store
        * renews per namespace), so memory- and IndexedDB-restores share one shape and one
        * restore path (`restoreCards`).
        */
      cards: Seq[CardSnapshot],
      expandedCardId: Option[String],
    )

    /** In-memory cache of full graph state (nodes, edges, history, results) for namespaces
      * visited this tab session, keyed by `namespaceKeyFor`. Checked before `ExplorerStore`
      * (IndexedDB) on namespace switch, so revisiting a namespace already seen this session
      * skips the async DB read. Entries are overwritten on every switch-away, not removed on
      * switch-into, so this holds one entry per distinct namespace ever visited — unbounded,
      * no eviction by size or recency. Only cleared via explicit user action (remove/reset a
      * namespace) or server-side namespace invalidation.
      */
    val namespaceSnapshots = mutable.Map.empty[String, NamespaceSnapshot]

    /** Convert the in-memory `NamespaceSnapshot` (live `vis.Node`/`vis.Edge` JS facade objects)
      * into the plain-data `FullSnapshot` that `ExplorerStore` can circe-encode and write to
      * IndexedDB. The facade objects aren't directly serializable, so node/edge data is pulled
      * out into `FullSerializableNode`/`SerializableEdge` here. See `fromFullSnapshot` for the
      * reverse direction.
      */
    def toFullSnapshot(snap: NamespaceSnapshot): FullSnapshot = {
      val nodes = snap.nodes.map { visNode =>
        val ext = visNode.asInstanceOf[QueryUiVisNodeExt]
        val xOpt = visNode.asInstanceOf[js.Dynamic].x.asInstanceOf[js.UndefOr[Double]].toOption
        val yOpt = visNode.asInstanceOf[js.Dynamic].y.asInstanceOf[js.UndefOr[Double]].toOption
        FullSerializableNode(
          ext.uiNode.id,
          ext.uiNode.hostIndex,
          ext.uiNode.label,
          ext.uiNode.properties,
          xOpt,
          yOpt,
          snap.pinnedNodes.contains(ext.uiNode.id),
        )
      }.toSeq
      val edges = snap.edges.map { visEdge =>
        val ext = visEdge.asInstanceOf[QueryUiVisEdgeExt]
        SerializableEdge(ext.uiEdge, ext.isSyntheticEdge)
      }.toSeq
      val clusters = collapsedClusterIds(snap.history)
      val serializedClusters = snap.history.past.reverse.collect {
        case QueryUiEvent.Collapse(nodeIds, clusterId, name) if clusters.contains(clusterId) =>
          SerializableCluster(nodeIds, clusterId, name)
      }
      FullSnapshot(
        nodes = nodes,
        edges = edges,
        history = snap.history,
        query = snap.query,
        foundNodesCount = snap.foundNodesCount,
        foundEdgesCount = snap.foundEdgesCount,
        atTime = snap.atTime,
        pinnedNodes = snap.pinnedNodes,
        viewPosition = (snap.viewPosition.x, snap.viewPosition.y),
        viewScale = snap.viewScale,
        layout = snap.layout match {
          case NetworkLayout.Graph => "graph"
          case NetworkLayout.Tree => "tree"
        },
        collapsedClusters = serializedClusters,
        clusterPositions = snap.clusterPositions.view.mapValues(p => (p.x, p.y)).toMap,
        resultsEntries = snap.resultsEntries.map { entry =>
          // Errors surface as toasts, never as history entries — `wasError` remains only
          // for persisted-format compatibility (old snapshots may still carry it).
          SerializableHistoryEntry(
            query = entry.content.queryEcho,
            pinned = entry.pinned,
            timeLabel = entry.timeLabel,
            wasError = false,
            errorMessage = None,
          )
        },
        resultsCurrentIdx = snap.resultsCurrentIdx,
        resultsCollapsed = snap.resultsCollapsed,
        cards = snap.cards,
        expandedCardId = snap.expandedCardId,
        savedAt = js.Date.now(),
      )
    }

    /** Convert a `FullSnapshot` loaded from `ExplorerStore` (IndexedDB) back into a
      * `NamespaceSnapshot`, rebuilding `vis.Node`/`vis.Edge` facade objects via
      * `nodeUi2Vis`/`edgeUi2Vis` so it can be applied to the canvas. Reverse of
      * `toFullSnapshot`.
      */
    def fromFullSnapshot(snap: FullSnapshot): NamespaceSnapshot = {
      import js.JSConverters._
      val visNodes = snap.nodes.map { sn =>
        val pos = for { x <- sn.x; y <- sn.y } yield CanvasPosition(x, y)
        val uiNode = UiNode[String](sn.id, sn.hostIndex, sn.label, sn.properties)
        val node = nodeUi2Vis(uiNode, pos)
        if (sn.fixed) node.asInstanceOf[js.Dynamic].fixed = true
        node
      }.toJSArray
      val visEdges =
        snap.edges.map(se => edgeUi2Vis(se.uiEdge, if (se.isSynthetic) EdgeKind.Synthetic else EdgeKind.Real)).toJSArray
      NamespaceSnapshot(
        nodes = visNodes,
        edges = visEdges,
        query = snap.query,
        history = snap.history,
        foundNodesCount = snap.foundNodesCount,
        foundEdgesCount = snap.foundEdgesCount,
        atTime = snap.atTime,
        pinnedNodes = snap.pinnedNodes,
        viewPosition = CanvasPosition(snap.viewPosition._1, snap.viewPosition._2),
        viewScale = snap.viewScale,
        layout = snap.layout match {
          case "tree" => NetworkLayout.Tree
          case _ => NetworkLayout.Graph
        },
        collapsedClusters = snap.collapsedClusters.map(c => CollapsedCluster(c.nodeIds, c.clusterId, c.name)),
        clusterPositions = snap.clusterPositions.view.mapValues { case (x, y) => CanvasPosition(x, y) }.toMap,
        resultsEntries = snap.resultsEntries.map { entry =>
          val outcome =
            if (entry.wasError) ResultOutcome.Restored(entry.errorMessage)
            else ResultOutcome.Restored(None)
          HistoryEntry(
            content =
              ResultsContent(outcome, entry.query, guessQueryLanguage(entry.query), runId = ResultsContent.nextRunId()),
            pinned = entry.pinned,
            timeLabel = entry.timeLabel,
          )
        }.toVector,
        resultsCurrentIdx = snap.resultsCurrentIdx,
        resultsCollapsed = snap.resultsCollapsed,
        cards = snap.cards,
        expandedCardId = snap.expandedCardId,
      )
    }

    def snapshotFor(state: State): Option[(String, NamespaceSnapshot)] = {
      val nsKey = namespaceKeyFor(state.namespace)
      network.map { _ =>
        val positions = visualization.readNodePositions()
        val nodesWithPositions = props.graphData.nodeSet.get().map { node =>
          positions.get(node.id.toString).fold(node) { case (x, y) =>
            js.Object
              .assign(js.Dynamic.literal(), node, js.Dynamic.literal(x = x, y = y))
              .asInstanceOf[vis.Node]
          }
        }
        val clusterIds = collapsedClusterIds(state.history)
        val liveClusters = state.history.past.reverse.collect {
          case QueryUiEvent.Collapse(nodeIds, clusterId, name) if clusterIds.contains(clusterId) =>
            CollapsedCluster(nodeIds, clusterId, name)
        }
        nsKey -> NamespaceSnapshot(
          nodes = nodesWithPositions,
          edges = props.graphData.edgeSet.get(),
          query = state.query,
          history = state.history,
          foundNodesCount = state.foundNodesCount,
          foundEdgesCount = state.foundEdgesCount,
          atTime = state.atTime,
          pinnedNodes = pinTracker.current,
          viewPosition = network
            .map { net =>
              val p = net.getViewPosition()
              CanvasPosition(p.x, p.y)
            }
            .getOrElse(CanvasPosition(0d, 0d)),
          viewScale = network.map(_.getScale()).getOrElse(1d),
          layout = layout,
          collapsedClusters = liveClusters,
          clusterPositions = readClusterPositions(state.history).view.mapValues { case (x, y) =>
            CanvasPosition(x, y)
          }.toMap,
          // Nothing populates `resultsHistory` since the drawer surface was removed, and
          // `applySnapshot` no longer restores these fields — persist them empty (the fields
          // stay in the snapshot format for compatibility).
          resultsEntries = Vector.empty,
          resultsCurrentIdx = -1,
          resultsCollapsed = resultsCollapsedVar.now(),
          cards = cardsStore.currentCards.map(CardSnapshot.fromState),
          expandedCardId = cardsStore.currentExpandedId.map(_.value),
        )
      }
    }

    def snapshotCurrentNamespace(): Option[(String, NamespaceSnapshot)] =
      snapshotFor(stateVar.now())

    def persistCurrentNamespace(): Unit =
      snapshotCurrentNamespace().foreach { case (k, v) =>
        ExplorerStore.save(k, toFullSnapshot(v))
      }

    /** Reset the canvas and all namespace-scoped UI state to empty — nodes, edges, history,
      * results, pins, context menu, and the query-count/pending-query trackers. Does not touch
      * persistence (`namespaceSnapshots`/`ExplorerStore`) or the query text/namespace/atTime
      * fields; callers that are dropping a namespace entirely handle those themselves.
      */
    def clearCanvas(): Unit = {
      props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
      props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
      stateVar.update(
        _.copy(
          history = History.empty,
          foundNodesCount = None,
          foundEdgesCount = None,
          runningQueryCount = 0,
          pendingTextQueries = Set.empty,
        ),
      )
      pinTracker.resetStateOnly(Set.empty)
      contextMenuVar.set(None)
      resultsVar.set(None)
      resultsHistory.entries.set(Vector.empty)
      resultsHistory.currentIdx.set(-1)
      resultsCollapsedVar.set(false)
      // Reset means reset: close every card through the normal close path, which also
      // frees tap subscriptions and disables graph-tap definitions server-side.
      cardsStore.closeAllCards()
      pendingTabularOpensVar.set(Set.empty)
      pendingTabularRestartsVar.set(Set.empty)
    }

    /** Apply a saved namespace view to the canvas: disable physics, re-add nodes/
      * edges, clusters and pins, restore the results history, re-fetch properties,
      * and finally recenter the viewport. Shared by the on-reload restore
      * ([[afterNetworkInit]]) and the namespace-switch restore ([[setNamespace]]).
      *
      * @param pending by-name; the namespace snapshot lookup. `ExplorerStore.load` reads
      *        IndexedDB asynchronously, so this yields a `Future` — `None` if no snapshot
      *        is stored for the namespace.
      * @param namespace the namespace these results belong to (also written to state).
      * @param reapplyAppearancesEagerly re-derive node appearances before the async
      *        property re-fetch. Safe on a namespace switch (config already loaded);
      *        skipped on first load, where the appearance config may still be loading.
      * @param stillCurrent checked once `pending` resolves and again after the two
      *        deferred animation frames, so a restore superseded by a newer `setNamespace`
      *        call (see its `setNamespaceVersion` guard) can never clobber state applied by
      *        that newer switch. Every caller passes a `setNamespaceVersion`-based guard,
      *        including the page-load restore — its IndexedDB read is async, so a user
      *        namespace switch can race it.
      */
    def applySnapshot(
      pending: => Future[Option[NamespaceSnapshot]],
      namespace: NamespaceParameter,
      reapplyAppearancesEagerly: Boolean,
      stillCurrent: () => Boolean,
    ): Unit =
      pending.foreach { snapshotOpt =>
        // Only take ownership of restoringVar while still current: a superseded restore that
        // set it true here would never reach the guarded clear below, leaving the loading
        // spinner stuck on after the newer restore already finished.
        if (stillCurrent()) {
          restoringVar.set(true)
          val _ = window.requestAnimationFrame { (_: Double) =>
            val _ = window.requestAnimationFrame { (_: Double) =>
              // If a newer restore has since started, it now owns restoringVar — leave it
              // alone so this stale restore can't clear it out from under the newer one.
              if (stillCurrent())
                try snapshotOpt.foreach { snapshot =>
                  if (snapshot.layout != layout) {
                    layout = snapshot.layout
                    applyLayoutMode(layout)
                  }
                  if (!stateVar.now().animating)
                    network.foreach(_.setOptions(new vis.Network.Options {
                      override val physics = new vis.Network.Options.Physics { override val enabled = false }
                    }))
                  props.graphData.nodeSet.add(snapshot.nodes)
                  props.graphData.edgeSet.add(snapshot.edges)
                  snapshot.collapsedClusters.foreach { c =>
                    collapseIntoCluster(c.nodeIds, c.clusterId, c.name)
                  }
                  network.foreach { net =>
                    snapshot.clusterPositions.foreach { case (clusterId, CanvasPosition(x, y)) =>
                      if (net.isCluster(clusterId)) net.moveNode(clusterId, x, y)
                    }
                  }
                  pinTracker.resetStateOnly(snapshot.pinnedNodes)
                  snapshot.pinnedNodes.foreach(visualization.pinNode)
                  stateVar.update(
                    _.copy(
                      query = snapshot.query,
                      queryBarColor = None,
                      history = snapshot.history,
                      animating = false,
                      foundNodesCount = snapshot.foundNodesCount,
                      foundEdgesCount = snapshot.foundEdgesCount,
                      atTime = snapshot.atTime,
                      namespace = namespace,
                    ),
                  )
                  if (reapplyAppearancesEagerly) reapplyAppearances()
                  // `snapshot.resultsEntries` is deliberately ignored (the field remains in the
                  // snapshot format for compatibility): the surface that displayed restored
                  // results history is gone, and re-running the entries registered invisible
                  // pending text queries that blocked submitQuery. The card system's own
                  // persistence below is the replacement.
                  resultsCollapsedVar.set(snapshot.resultsCollapsed)
                  restoreCards(snapshot.cards, snapshot.expandedCardId)
                  refetchNodeProperties(namespace, snapshot.atTime)
                  window.requestAnimationFrame { _ =>
                    network.foreach(
                      _.moveTo(
                        new vis.MoveToOptions {
                          override val position = new vis.Position {
                            val x = snapshot.viewPosition.x
                            val y = snapshot.viewPosition.y
                          }
                          override val scale = snapshot.viewScale
                        },
                      ),
                    )
                  }
                } finally restoringVar.set(false)
            }
          }
        }
      }

    /** Cluster ids currently collapsed, per the applied (past) history events */
    def collapsedClusterIds(history: History[QueryUiEvent]): Set[String] =
      history.past.reverse.foldLeft(Set.empty[String]) {
        case (acc, QueryUiEvent.Collapse(_, clusterId, _)) => acc + clusterId
        case (acc, QueryUiEvent.Expand(_, clusterId, _)) => acc - clusterId
        case (acc, _) => acc
      }

    /** Positions of the currently collapsed cluster nodes. These live in the vis
      * network, not the DataSet, so [[GraphVisualization.readNodePositions]] does
      * not see them.
      */
    def readClusterPositions(history: History[QueryUiEvent]): Map[String, (Double, Double)] =
      network.fold(Map.empty[String, (Double, Double)]) { net =>
        import js.JSConverters._
        val ids = collapsedClusterIds(history).filter(net.isCluster(_)).toSeq
        net
          .getPositions(ids.map(id => id: vis.IdType).toJSArray)
          .map { case (clusterId, pos) => clusterId -> ((pos.x, pos.y)) }
          .toMap
      }

    def selectedInterpreter: QueryInterpreter = QueryInterpreter.Cypher

    // --- WebSocket client management ---

    def namespaceKeyFor(ns: NamespaceParameter): String = ns.namespaceId

    def namespaceOptFor(ns: NamespaceParameter): Option[String] =
      if (ns == NamespaceParameter.defaultNamespaceParameter) None else Some(ns.namespaceId)

    /** Get or create a V1 websocket client for the given namespace.
      * The namespace is passed explicitly to avoid reading stale values from
      * stateVar.now() inside Laminar transaction callbacks (see Airstream #39).
      */
    def getWebSocketClient(namespace: NamespaceParameter): Future[WebSocketQueryClient] = {
      val nsKey = namespaceKeyFor(namespace)
      val nsOpt = namespaceOptFor(namespace)
      val existing = wsClients.getOrElse(nsKey, Future.failed(new Exception("Client not initialized")))
      existing.value match {
        case Some(Success(client)) if client.webSocket.readyState == dom.WebSocket.OPEN => ()
        case None => () // still connecting
        case Some(_) =>
          val client = props.routes.queryProtocolClient(nsOpt)
          val clientReady = Promise[WebSocketQueryClient]()
          val webSocket = client.webSocket

          webSocket
            .addEventListener[dom.MessageEvent]("open", (_: dom.MessageEvent) => clientReady.trySuccess(client))
          webSocket.addEventListener[dom.Event](
            "error",
            (_: dom.Event) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` failed")),
          )
          webSocket.addEventListener[dom.CloseEvent](
            "close",
            (_: dom.CloseEvent) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` was closed")),
          )

          wsClients(nsKey) = clientReady.future
      }
      wsClients.getOrElse(nsKey, existing)
    }

    /** Get or create a V2 websocket client for the given namespace.
      * See [[getWebSocketClient]] for why the namespace is passed explicitly.
      */
    def getWebSocketClientV2(namespace: NamespaceParameter): Future[V2WebSocketQueryClient] = {
      val nsKey = namespaceKeyFor(namespace)
      val nsOpt = namespaceOptFor(namespace)
      val existing = wsClientsV2.getOrElse(nsKey, Future.failed(new Exception("V2 client not initialized")))
      existing.value match {
        case Some(Success(client)) if client.webSocket.readyState == dom.WebSocket.OPEN => ()
        case None => () // still connecting
        case Some(_) =>
          val client = props.routes.queryProtocolClientV2(nsOpt)
          val clientReady = Promise[V2WebSocketQueryClient]()
          val webSocket = client.webSocket

          webSocket.addEventListener[dom.Event]("open", (_: dom.Event) => clientReady.trySuccess(client))
          webSocket.addEventListener[dom.Event](
            "error",
            (_: dom.Event) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` failed")),
          )
          webSocket.addEventListener[dom.CloseEvent](
            "close",
            (_: dom.CloseEvent) =>
              clientReady.tryFailure(new Exception(s"WebSocket connection to `${webSocket.url}` was closed")),
          )

          wsClientsV2(nsKey) = clientReady.future
      }
      wsClientsV2.getOrElse(nsKey, existing)
    }

    // --- Save helpers ---

    def saveSampleQueries(sqs: Vector[SampleQuery]): Unit = {
      val previous = stateVar.now().sampleQueries
      stateVar.update(_.copy(sampleQueries = sqs))
      props.dataService.queryUiConfigDispatch.onNext(
        QueryUiConfigService.SaveSampleQueries(
          sqs,
          replyTo = Observer[SaveResult] {
            case SaveSucceeded =>
              toastVar.set(Some(ToastMessage("Sample queries saved", ToastVariant.Success)))
            case SaveFailed(message) =>
              stateVar.update(_.copy(sampleQueries = previous))
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    // --- Node appearance and quick queries ---

    def quickQueriesFor(node: UiNode[String]): Seq[V2QuickQuery] =
      stateVar.now().uiNodeQuickQueries.collect {
        case V2UiNodeQuickQuery(predicate, qq) if predicate.matches(node) => qq
      }

    def appearanceFor(node: UiNode[String]): (String, vis.NodeOptions.Icon) = {
      val (sizeOpt, iconOpt, colorOpt, labelDescOpt) = stateVar
        .now()
        .uiNodeAppearances
        .find(_.predicate.matches(node))
        .map(appearance => (appearance.size, appearance.icon, appearance.color, appearance.label))
        .getOrElse((None, None, None, None))

      val visIcon = new vis.NodeOptions.Icon {
        override val face = iconFontFace
        override val color =
          colorOpt.getOrElse[String](props.hostColors(Math.floorMod(node.hostIndex, props.hostColors.length)))
        override val code = iconOpt.getOrElse[String]("\uf3a6")
        override val size = sizeOpt.getOrElse[Double](30.0)
      }

      val uiLabel = labelDescOpt match {
        case Some(UiNodeLabel.Constant(lbl)) => lbl
        case Some(UiNodeLabel.Property(key, prefix)) if node.properties.contains(key) =>
          val prop = node.properties(key)
          val propVal = prop.asString getOrElse prop.noSpaces
          prefix.getOrElse("") + propVal
        case _ => node.label
      }

      uiLabel -> visIcon
    }

    // --- Conversion helpers ---

    def nodeUi2Vis(node: UiNode[String], startingPosition: Option[CanvasPosition]): vis.Node = {
      val (uiLabel, iconStyle) = appearanceFor(node)

      new QueryUiVisNodeExt {
        override val id = node.id
        override val label = uiLabel
        override val icon = iconStyle
        override val uiNode = node

        override val x = startingPosition match {
          case Some(pos) => pos.x
          case None => js.undefined
        }
        override val y = startingPosition match {
          case Some(pos) => pos.y
          case None => js.undefined
        }

        override val title = {
          val idProp = s"<strong>ID : ${escapeHtml(node.id)}</strong>"
          val hostProp = if (props.showHostInTooltip) {
            List(s"<strong>Served from Host : ${node.hostIndex}</strong>")
          } else {
            Nil
          }
          val strProps = node.properties.toList
            .sortBy(_._1)
            .map { case (keyStr, valueJson) =>
              s"${escapeHtml(keyStr)} : ${escapeHtml(valueJson.noSpaces)}"
            }

          (idProp :: hostProp ++ strProps).mkString("<br>")
        }
      }
    }

    def edgeId(edge: UiEdge[String]): String =
      s"${edge.from}-${edge.edgeType}->${edge.to}"

    def edgeUi2Vis(edge: UiEdge[String], kind: EdgeKind): vis.Edge = {
      val isSyn = EdgeKind.isSynthetic(kind)
      new QueryUiVisEdgeExt {
        override val id = edgeId(edge)
        override val from = edge.from
        override val to = edge.to
        override val label = if (props.showEdgeLabels) edge.edgeType else js.undefined
        override val arrows = if (edge.isDirected) "to" else ""
        override val smooth = isSyn
        override val uiEdge = edge
        override val isSyntheticEdge = isSyn
        override val color = if (isSyn) "purple" else js.undefined
        override val dashes = if (isSyn) true else js.undefined
      }
    }

    // --- History event application ---

    /** Collapse the given nodes into a vis cluster. Used when applying Collapse
      * history events and when rebuilding clusters on namespace-snapshot restore.
      * Does not touch physics; callers decide whether a re-settle is wanted.
      */
    def collapseIntoCluster(nodes: Seq[String], clusterId: String, name: String): Unit = {
      import js.JSConverters._
      val nodeIds: Set[vis.IdType] = nodes.map(id => id: vis.IdType).toSet
      network.foreach(_.cluster(new vis.ClusterOptions {
        override val joinCondition =
          Some[js.Function1[js.Any, Boolean]]((n: js.Any) => nodeIds.contains(n.asInstanceOf[vis.Node].id)).orUndefined

        override val processProperties = Some[js.Function3[js.Any, js.Any, js.Any, js.Any]] {
          (clusterOptionsAny: js.Any, childNodes: js.Any, childEdges: js.Any) =>
            trait MutableClusterOptions extends js.Object {
              var id: js.UndefOr[vis.IdType]
              var label: js.UndefOr[String]
              var title: js.UndefOr[String]
              var collapsedNodes: js.UndefOr[js.Array[String]]
            }

            val clusterOptions = clusterOptionsAny.asInstanceOf[MutableClusterOptions]
            clusterOptions.id = clusterId
            clusterOptions.label = name
            clusterOptions.collapsedNodes = nodes.toJSArray

            clusterOptions
        }.orUndefined

        override val clusterNodeProperties = new vis.NodeOptions {
          override val icon = new vis.NodeOptions.Icon {
            override val code = "\uf413"
            override val size = 54
          }
        }
      }))
    }

    implicit lazy val queryUiEvent: History.Event[QueryUiEvent] = new History.Event[QueryUiEvent] {
      import QueryUiEvent._
      import js.JSConverters._

      def applyEvent(event: QueryUiEvent): Unit = event match {
        case Add(nodes, edges, updateNodes, syntheticEdges, explodeFromId) =>
          // Skip physics when the event is update-only (no new nodes/edges) so editing a
          // node's properties doesn't kick off a layout pass and send nodes flying around.
          val hasNewContent = nodes.nonEmpty || edges.nonEmpty || syntheticEdges.nonEmpty
          def body(): Unit = {
            val posOpt: Option[CanvasPosition] = explodeFromId.flatMap { startingId =>
              Option(props.graphData.nodeSet.get(startingId)).map { _ =>
                val bb = network.get.getBoundingBox(startingId)
                CanvasPosition((bb.left + bb.right) / 2, (bb.top + bb.bottom) / 2)
              }
            }

            props.graphData.nodeSet.add(nodes.map(nodeUi2Vis(_, posOpt)).toJSArray)
            props.graphData.edgeSet.add(edges.map(edgeUi2Vis(_, EdgeKind.Real)).toJSArray)
            props.graphData.edgeSet.add(syntheticEdges.map(edgeUi2Vis(_, EdgeKind.Synthetic)).toJSArray)
            props.graphData.nodeSet.update(updateNodes.map(nodeUi2Vis(_, None)).toJSArray)

            if (layout == NetworkLayout.Tree) {
              network.get.setOptions(new vis.Network.Options {
                override val nodes = new vis.NodeOptions {
                  override val shape = "icon"
                }
              })
            }
          }
          if (hasNewContent) withPhysics()(body()) else body()

        case Remove(nodes, edges, _, syntheticEdges, _) =>
          props.graphData.nodeSet.remove(nodes.map(n => n.id: vis.IdType).toJSArray)
          props.graphData.edgeSet.remove(edges.map(e => edgeId(e): vis.IdType).toJSArray)
          props.graphData.edgeSet.remove(syntheticEdges.map(e => edgeId(e): vis.IdType).toJSArray)
          pinTracker.removeNodes(nodes.map(_.id))
          ()

        case Collapse(nodes, clusterId, name) =>
          withPhysics() {
            collapseIntoCluster(nodes, clusterId, name)
          }

        case Expand(_, clusterId, _) =>
          withPhysics() {
            network.get.openCluster(clusterId)
          }

        case Checkpoint(_) =>

        case Layout(positions) =>
          pinTracker.resetStateOnly(positions.collect { case (id, NodePosition(_, _, true)) => id }.toSet)
          for ((nodeId, NodePosition(x, y, isFixed)) <- positions) {
            val nodeExists = props.graphData.nodeSet.get(nodeId) != null
            val isCluster = network.exists(_.isCluster(nodeId))
            if (nodeExists || isCluster) {
              visualization.setNodePosition(nodeId, x, y)
              // Pin updates write to the DataSet, which would create a phantom
              // entry for a cluster id — clusters only get their position moved
              if (isCluster) ()
              else if (isFixed) visualization.pinNode(nodeId)
              else visualization.unpinNode(nodeId)
            }
          }
      }

      def invert(event: QueryUiEvent) = event.invert
    }

    // --- History management ---

    def updateHistory(
      update: History[QueryUiEvent] => Option[History[QueryUiEvent]],
      callback: () => Unit = () => (),
    ): Unit = {
      stateVar.update(s => update(s.history).fold(s)(h => s.copy(history = h)))
      persistenceDirty = true
      callback()
    }

    def downloadHistoryFile(history: History[QueryUiEvent], fileName: String): Unit =
      DownloadUtils.downloadFile(HistoryJsonSchema.encode(history), fileName, "application/json")

    def uploadHistory(files: dom.FileList): Unit = {
      val file = if (files.length != 1) {
        val msg = s"Expected one file, but got ${files.length}"
        toastVar.set(Some(ToastMessage(msg, ToastVariant.Error)))
        return
      } else {
        files(0)
      }

      if (file.`type` != "application/json") {
        val msg = s"Expected JSON file, but `${file.name}' has type '${file.`type`}'."
        toastVar.set(Some(ToastMessage(msg, ToastVariant.Error)))
        return
      }

      val reader = new dom.FileReader()
      reader.onload = (e: dom.ProgressEvent) => {
        val jsonStr = e.target.asInstanceOf[dom.FileReader].result.asInstanceOf[String]
        HistoryJsonSchema.decode(jsonStr) match {
          case Validated.Valid(hist) =>
            val msg = """Uploading this history will erase your existing one.
                        |
                        |Do you wish to continue?""".stripMargin
            if (window.confirm(msg)) {
              // Replaying onto a non-empty canvas throws on the first Add whose
              // node already exists (vis DataSet duplicate-id), aborting the
              // replay mid-way. Start from a blank canvas like setAtTime does.
              props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
              props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
              pinTracker.resetStateOnly(Set.empty)
              stateVar.update(_.copy(history = hist))
              hist.past.reverse.foreach(queryUiEvent.applyEvent(_))
              // Persist only after the events are applied to the canvas, so the saved
              // snapshot's nodes/edges match the uploaded history rather than the old graph.
              persistCurrentNamespace()
            }

          case Validated.Invalid(errs) =>
            val msg = s"Malformed JSON history file:${errs.toList.mkString("\n  ", "\n  ", "")}"
            toastVar.set(Some(ToastMessage(msg, ToastVariant.Error)))
        }
      }
      reader.readAsText(file)
    }

    // --- SVG / download ---

    def downloadSvgSnapshot(fileName: String = "graph.svg"): Unit = {
      val positions = network.get.getPositions(props.graphData.nodeSet.getIds())
      SvgSnapshot(props.graphData, positions, fontFace = iconFontFace).map { svgElement =>
        val tempContainer = document.createElement("div")
        tempContainer.setAttribute("style", "position: absolute; visibility: hidden; pointer-events: none;")
        document.body.appendChild(tempContainer)
        tempContainer.appendChild(svgElement)

        val svgEl = svgElement.asInstanceOf[dom.svg.SVG]
        val bbox = svgEl.getBBox()
        val padding = 10
        val viewBoxMinX = bbox.x - padding
        val viewBoxMinY = bbox.y - padding
        val viewBoxWidth = bbox.width + 2 * padding
        val viewBoxHeight = bbox.height + 2 * padding
        svgEl.setAttribute("viewBox", s"$viewBoxMinX $viewBoxMinY $viewBoxWidth $viewBoxHeight")
        svgEl.setAttribute("width", s"${viewBoxWidth}px")
        svgEl.setAttribute("height", s"${viewBoxHeight}px")

        val blob = new dom.Blob(
          js.Array(tempContainer.innerHTML),
          new dom.BlobPropertyBag { `type` = "image/svg" },
        )

        document.body.removeChild(tempContainer)

        val a = document.createElement("a").asInstanceOf[dom.HTMLAnchorElement]
        a.setAttribute("download", fileName)
        a.setAttribute("href", dom.URL.createObjectURL(blob))
        a.setAttribute("target", "_blank")
        a.click()
      }
      ()
    }

    def makeSnapshot(): History[QueryUiEvent] = {
      val nodes: Seq[UiNode[String]] = props.graphData.nodeSet
        .get()
        .toSeq
        .map(_.asInstanceOf[QueryUiVisNodeExt].uiNode)

      val (syntheticEdges, edges) = props.graphData.edgeSet
        .get()
        .toSeq
        .map(_.asInstanceOf[QueryUiVisEdgeExt])
        .partition(_.isSyntheticEdge)

      History(
        past = List(QueryUiEvent.Add(nodes, edges.map(_.uiEdge), Seq.empty, syntheticEdges.map(_.uiEdge), None)),
        future = List(),
      )
    }

    // JSON-LD output has no positions, so there is nothing to capture in history
    def downloadGraphJsonLd(): Unit =
      DownloadUtils.downloadGraphJsonLd(props.graphData.nodeSet, props.graphData.edgeSet)

    // --- Query logic ---

    def invalidToException(invalid: Invalid): Exception = new Exception(invalid.errors mkString "\n")

    def mergeEndpointErrorsIntoFuture[A](fut: Future[Either[endpoints4s.Invalid, Option[A]]]): Future[A] =
      fut.flatMap { either =>
        Future.fromTry {
          either.left
            .map(invalidToException)
            .flatMap(_.toRight(new NoSuchElementException()))
            .toTry
        }
      }

    def nodeQuery(
      query: String,
      namespace: NamespaceParameter,
      atTime: Option[Long],
      language: QueryLanguage,
      parameters: Map[String, Json],
    ): Future[Option[Seq[UiNode[String]]]] =
      props.queryMethod match {
        case QueryMethod.Restful =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              props.routes.gremlinNodesPost((atTime, None, namespace, GremlinQuery(query))).future
            case QueryLanguage.Cypher =>
              props.routes.cypherNodesPost((atTime, None, namespace, CypherQuery(query))).future
          }).map(Some(_))

        case QueryMethod.RestfulV2 =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              Future.successful(Left(Invalid(Seq("Gremlin is not supported in APIv2"))))
            case QueryLanguage.Cypher =>
              props.routes.cypherNodesPostV2((atTime, None, namespace, CypherQuery(query))).future
          }).map(Some(_))

        case QueryMethod.WebSocket =>
          val nodeCallback = new QueryCallbacks.CollectNodesToFuture()
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
          for {
            client <- getWebSocketClient(namespace)
            _ <- Future.fromTry(client.query(streamingQuery, nodeCallback).toTry)
            results <- nodeCallback.future
          } yield results

        case QueryMethod.WebSocketV2 =>
          val cb = new V2QueryCallbacks.CollectNodesToFuture()
          val interpreter = selectedInterpreter
          val sq =
            V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
          for {
            client <- getWebSocketClientV2(namespace)
            _ <- Future.fromTry(client.query(sq, cb).toTry)
            results <- cb.future
          } yield results.map(_.map(n => UiNode(n.id, n.hostIndex, n.label, n.properties)))
      }

    def edgeQuery(
      query: String,
      atTime: Option[Long],
      namespace: NamespaceParameter,
      language: QueryLanguage,
      parameters: Map[String, Json],
    ): Future[Option[Seq[UiEdge[String]]]] =
      props.queryMethod match {
        case QueryMethod.Restful =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              props.routes.gremlinEdgesPost((atTime, None, namespace, GremlinQuery(query, parameters))).future
            case QueryLanguage.Cypher =>
              props.routes.cypherEdgesPost((atTime, None, namespace, CypherQuery(query, parameters))).future
          }).map(Some(_))

        case QueryMethod.RestfulV2 =>
          mergeEndpointErrorsIntoFuture(language match {
            case QueryLanguage.Gremlin =>
              Future.successful(Left(Invalid(Seq("Gremlin is not supported in APIv2"))))
            case QueryLanguage.Cypher =>
              props.routes.cypherEdgesPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future
          }).map(Some(_))

        case QueryMethod.WebSocket =>
          val edgeCallback = new QueryCallbacks.CollectEdgesToFuture()
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, None, Some(100))
          for {
            client <- getWebSocketClient(namespace)
            _ <- Future.fromTry(client.query(streamingQuery, edgeCallback).toTry)
            results <- edgeCallback.future
          } yield results

        case QueryMethod.WebSocketV2 =>
          val cb = new V2QueryCallbacks.CollectEdgesToFuture()
          val interpreter = selectedInterpreter
          val sq =
            V2StreamingQuery(query, parameters, interpreter = interpreter, atTime = atTime, maxResultBatch = Some(100))
          for {
            client <- getWebSocketClientV2(namespace)
            _ <- Future.fromTry(client.query(sq, cb).toTry)
            results <- cb.future
          } yield results.map(_.map(e => UiEdge(e.from, e.edgeType, e.to, e.isDirected)))
      }

    def textQuery(
      query: String,
      atTime: Option[Long],
      namespace: NamespaceParameter,
      language: QueryLanguage,
      parameters: Map[String, Json],
      updateResults: Either[Seq[Json], CypherQueryResult] => Unit,
    ): Future[Option[Unit]] =
      (props.queryMethod, language) match {
        case (QueryMethod.Restful, QueryLanguage.Gremlin) =>
          val gremlinResults =
            props.routes.gremlinPost((atTime, None, namespace, GremlinQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(gremlinResults).map { results =>
            updateResults(Left(results))
            Some(())
          }

        case (QueryMethod.Restful, QueryLanguage.Cypher) =>
          val cypherResults =
            props.routes.cypherPost((atTime, None, namespace, CypherQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(cypherResults).map { results =>
            updateResults(Right(results))
            Some(())
          }

        case (QueryMethod.RestfulV2, QueryLanguage.Gremlin) =>
          Future.successful(Some(()))

        case (QueryMethod.RestfulV2, QueryLanguage.Cypher) =>
          val cypherResults =
            props.routes.cypherPostV2((atTime, None, namespace, CypherQuery(query, parameters))).future
          mergeEndpointErrorsIntoFuture(cypherResults).map { results =>
            updateResults(Right(results))
            Some(())
          }

        case (QueryMethod.WebSocket, _) =>
          val result = Promise[Option[Unit]]()

          val textCallback: QueryCallbacks = language match {
            case QueryLanguage.Gremlin =>
              new QueryCallbacks.NonTabularCallbacks {
                private var buffered = Seq.empty[Json]
                private var cancelled = false

                def onNonTabularResults(batch: Seq[Json]): Unit = {
                  buffered ++= batch
                  updateResults(Left(buffered))
                }
                def onError(message: String): Unit = {
                  result.tryFailure(new Exception(message))
                  ()
                }
                def onComplete(): Unit = {
                  result.trySuccess(if (cancelled) None else Some(()))
                  ()
                }

                def onQueryStart(
                  isReadOnly: Boolean,
                  canContainAllNodeScan: Boolean,
                  columns: Option[Seq[String]],
                ): Unit = ()

                def onQueryCancelOk(): Unit = cancelled = true
                def onQueryCancelError(message: String): Unit = ()
              }

            case QueryLanguage.Cypher =>
              new QueryCallbacks.TabularCallbacks {
                private var buffered = Seq.empty[Seq[Json]]
                private var cancelled = false

                def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit = {
                  buffered ++= batch
                  updateResults(Right(CypherQueryResult(columns, buffered)))
                }
                def onError(message: String): Unit = {
                  result.tryFailure(new Exception(message))
                  ()
                }
                def onComplete(): Unit = {
                  result.trySuccess(if (cancelled) None else Some(()))
                  ()
                }

                def onQueryStart(
                  isReadOnly: Boolean,
                  canContainAllNodeScan: Boolean,
                  columns: Option[Seq[String]],
                ): Unit =
                  for (cols <- columns)
                    updateResults(Right(CypherQueryResult(cols, buffered)))

                def onQueryCancelOk(): Unit = cancelled = true
                def onQueryCancelError(message: String): Unit = ()
              }
          }
          val streamingQuery = StreamingQuery(query, parameters, language, atTime, Some(1000), Some(100))
          for {
            client <- getWebSocketClient(namespace)
            queryId <- Future.fromTry(client.query(streamingQuery, textCallback).toTry)
            _ = {
              stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries + queryId))
              result.future.onComplete { _ =>
                stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries - queryId))
              }
            }
            results <- result.future
          } yield results

        case (QueryMethod.WebSocketV2, _) =>
          val result = Promise[Option[Unit]]()

          val textCallback = new V2QueryCallbacks.TextCallbacks {
            private var buffered = Seq.empty[Seq[Json]]
            private var cancelled = false

            override def onTabularResults(columns: Seq[String], batch: Seq[Seq[Json]]): Unit = {
              buffered ++= batch
              updateResults(Right(CypherQueryResult(columns, buffered)))
            }
            def onError(message: String): Unit = {
              result.tryFailure(new Exception(message))
              ()
            }
            def onComplete(): Unit = {
              result.trySuccess(if (cancelled) None else Some(()))
              ()
            }

            def onQueryStart(
              isReadOnly: Boolean,
              canContainAllNodeScan: Boolean,
              columns: Option[Seq[String]],
            ): Unit =
              for (cols <- columns)
                updateResults(Right(CypherQueryResult(cols, buffered)))

            def onQueryCancelOk(): Unit = cancelled = true
            def onQueryCancelError(message: String): Unit = ()
          }
          val interpreter = selectedInterpreter
          val sq = V2StreamingQuery(
            query,
            parameters,
            interpreter = interpreter,
            atTime = atTime,
            maxResultBatch = Some(1000),
            resultsWithinMillis = Some(100),
          )
          for {
            client <- getWebSocketClientV2(namespace)
            queryId <- Future.fromTry(client.query(sq, textCallback).toTry)
            _ = {
              stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries + queryId))
              result.future.onComplete { _ =>
                stateVar.update(s => s.copy(pendingTextQueries = s.pendingTextQueries - queryId))
              }
            }
            results <- result.future
          } yield results
      }

    lazy val ObserveStandingQuery = "(?:OBSERVE|observe) [\"']?(.*)[\"']?".r

    /** Submit a query and load its results into the graph (or render them in the bottom bar
      * for text queries). The returned Future resolves once the query has finished applying
      * its results (or fails on error). Useful for callers that need to chain follow-up work
      * after the graph is updated, like overlaying synthetic edges.
      */
    /** `None` when the submission was rejected without starting a run (blank query, a
      * pending text query already in flight) — callers that claim per-run state (e.g. a
      * card's edit association) must not do so in that case; `Some` completes when the
      * accepted run finishes.
      */
    def submitQuery(
      uiQueryType: UiQueryType,
      queryOverride: Option[String] = None,
      namespaceOverride: Option[NamespaceParameter] = None,
      parameters: Map[String, Json] = Map.empty,
    ): Option[Future[Unit]] = {
      val state = stateVar.now()
      val namespace = namespaceOverride.getOrElse(state.namespace)

      val query = queryOverride.getOrElse(state.query) match {
        case ObserveStandingQuery(sqName) =>
          val amendedQuery = s"CALL standing.wiretap({ name: '$sqName' })"
          stateVar.update(_.copy(query = amendedQuery))
          amendedQuery
        case other =>
          other
      }
      if (query.isBlank) return None

      val language = guessQueryLanguage(query)

      if (state.pendingTextQueries.nonEmpty) {
        window.alert(
          """You have a pending text query. You must cancel it before issuing another query.
            |Pending queries can be cancelled by clicking on the spinning loader in the top right.
            |""".stripMargin,
        )
        return None
      }

      val done = Promise[Unit]()

      stateVar.update(s =>
        s.copy(
          foundNodesCount = None,
          foundEdgesCount = None,
          queryBarColor = None,
          runningQueryCount = s.runningQueryCount + 1,
        ),
      )
      resultsVar.set(None)

      uiQueryType match {
        case UiQueryType.Text | UiQueryType.SideEffectsText =>
          // SideEffectsText intentionally never populates the result panel — the query returns no
          // rows by design (only the query-bar color flash signals completion). Plain Text renders
          // its results in the panel as usual.
          var lastResultCount: Int = 0
          // One run identity for every emission of this submission; the revision bump per
          // batch is what lets downstream `.distinctBy((runId, revision))` see new batches
          // without deep-comparing the growing result buffer.
          val runId = ResultsContent.nextRunId()
          var revision: Int = 0
          def updateResults(result: Either[Seq[Json], CypherQueryResult]): Unit =
            if (uiQueryType == UiQueryType.Text) {
              val outcome = result match {
                case Left(values) => ResultOutcome.TextResults(values)
                case Right(results) => ResultOutcome.Tabular(results)
              }
              lastResultCount = result match {
                case Left(values) => values.size
                case Right(results) => results.results.size
              }
              revision += 1
              resultsVar.set(Some(ResultsContent(outcome, query, language, runId, revision)))
            }

          textQuery(query, state.atTime, namespace, language, parameters, updateResults).onComplete {
            case Success(outcome) =>
              // Text: a query that returned nothing gets the distinct empty state. SideEffectsText:
              // leave the panel unpopulated (it stays None from submit-start).
              if (uiQueryType == UiQueryType.Text && outcome.isEmpty)
                resultsVar.update {
                  case Some(c) => Some(c.copy(outcome = ResultOutcome.toEmpty(c.outcome), revision = c.revision + 1))
                  case None =>
                    Some(ResultsContent(ResultOutcome.EmptyResult(wasTabular = true, Nil), query, language, runId))
                }
              // SideEffectsText flashes grey (ran cleanly, no rows by design); Text flashes grey when
              // empty, green otherwise.
              val outcomeColor =
                if (uiQueryType == UiQueryType.SideEffectsText) Styles.queryResultEmpty
                else if (outcome.isEmpty) Styles.queryResultEmpty
                else Styles.queryResultSuccess
              stateVar.update(s =>
                s.copy(
                  queryBarColor = Some(outcomeColor),
                  runningQueryCount = s.runningQueryCount - 1,
                ),
              )
              window.setTimeout(
                () => stateVar.update(_.copy(queryBarColor = None)),
                750,
              )
              // Ephemeral result-count overlay trigger (design doc §5): fires the indicator's
              // fade-in, which then displays the live canvas node/edge counters — Text only —
              // SideEffectsText returns no rows by design and doesn't populate the results
              // surface either.
              if (uiQueryType == UiQueryType.Text)
                resultCountBus.emit(ResultCountEvent(lastResultCount, nodeCount = None))
              done.success(())
              ()

            case Failure(err) =>
              showQueryError(Option(err.getMessage).getOrElse(""))
              stateVar.update(s =>
                s.copy(
                  queryBarColor = Some(Styles.queryResultError),
                  runningQueryCount = s.runningQueryCount - 1,
                ),
              )
              done.failure(err)
          }

        case popup: UiQueryType.TextPopup =>
          val (popupX, popupY) = (popup.x, popup.y)

          def updateResults(result: Either[Seq[Json], CypherQueryResult]): Unit =
            nodePopupVar.set(
              Some(
                NodePopupState(
                  popupX,
                  popupY,
                  NodePropertiesPopup.parseContent(result),
                  popup.iconCode,
                  popup.iconColor,
                ),
              ),
            )

          textQuery(query, state.atTime, namespace, language, parameters, updateResults).onComplete {
            case Success(outcome) =>
              val outcomeColor = if (outcome.isEmpty) Styles.queryResultEmpty else Styles.queryResultSuccess
              stateVar.update(s =>
                s.copy(
                  queryBarColor = Some(outcomeColor),
                  runningQueryCount = s.runningQueryCount - 1,
                ),
              )
              window.setTimeout(
                () => stateVar.update(_.copy(queryBarColor = None)),
                750,
              )
              ()

            case Failure(err) =>
              stateVar.update(s =>
                s.copy(
                  queryBarColor = Some(Styles.queryResultError),
                  runningQueryCount = s.runningQueryCount - 1,
                ),
              )
              nodePopupVar.set(
                Some(
                  NodePopupState(
                    popupX,
                    popupY,
                    NodePopupContent.Fallback(pre(cls := "wrap", err.getMessage)),
                    popup.iconCode,
                    popup.iconColor,
                  ),
                ),
              )
          }

        case UiQueryType.Node | (_: UiQueryType.NodeFromId) =>
          val nodesEdgesFut = for {
            rawNodesOpt <- nodeQuery(query, namespace, state.atTime, language, parameters)
            dedupedNodesOpt = rawNodesOpt.map { rawNodes =>
              val dedupedIds = mutable.Set.empty[String]
              rawNodes.filter(n => dedupedIds.add(n.id))
            }

            nodes = dedupedNodesOpt match {
              case Some(dedupedNodes) if dedupedNodes.length > props.nodeResultSizeLimit =>
                val limitedCount: Option[Int] = Option(
                  window.prompt(
                    s"You are about to render ${dedupedNodes.length} nodes.\nHow many do you want to render?",
                    dedupedNodes.length.toString,
                  ),
                ).map(_.toInt)
                stateVar.update(s => s.copy(foundNodesCount = limitedCount))
                limitedCount.fold(Seq.empty[UiNode[String]])(dedupedNodes.take(_))

              case Some(dedupedNodes) =>
                stateVar.update(s => s.copy(foundNodesCount = Some(dedupedNodes.length)))
                dedupedNodes

              case None =>
                Nil
            }

            newNodes = nodes.map(_.id).toSet
            edgesOpt <-
              if (newNodes.isEmpty) {
                Future.successful(Some(Nil))
              } else {
                val edgeQueryStr = props.edgeQueryLanguage match {
                  case QueryLanguage.Gremlin =>
                    """g.V(new).bothE().dedup().where(_.and(
                    | _.outV().strId().is(within(all)),
                    | _.inV().strId().is(within(all))))""".stripMargin

                  case QueryLanguage.Cypher =>
                    """UNWIND $new AS newId
                    |CALL getFilteredEdges(newId, [], [], $all) YIELD edge
                    |RETURN DISTINCT edge AS e""".stripMargin
                }
                val existingNodes = props.graphData.nodeSet.getIds().map(_.toString).toVector
                val queryParameters = Map(
                  "new" -> Json.fromValues(newNodes.map(Json.fromString)),
                  "all" -> Json.fromValues((existingNodes ++ newNodes).map(Json.fromString)),
                )
                edgeQuery(edgeQueryStr, state.atTime, namespace, props.edgeQueryLanguage, queryParameters)
              }
            edges = edgesOpt match {
              case Some(edges) =>
                if (rawNodesOpt.nonEmpty) stateVar.update(s => s.copy(foundEdgesCount = Some(edges.length)))
                edges
              case None =>
                Nil
            }
          } yield (nodes, edges, rawNodesOpt.fold(nodes.length)(_.length))

          nodesEdgesFut.onComplete {
            case Success((nodes, edges, rawResultCount)) =>
              val (syntheticEdges, explodeFromIdOpt) = uiQueryType match {
                case UiQueryType.NodeFromId(explodeFromId, Some(syntheticEdgeLabel)) =>
                  nodes.map(n => UiEdge(explodeFromId, syntheticEdgeLabel, n.id)) -> Some(explodeFromId)
                case UiQueryType.NodeFromId(explodeFromId, None) =>
                  (Seq.empty, Some(explodeFromId))
                case UiQueryType.Node | UiQueryType.Text | UiQueryType.SideEffectsText |
                    UiQueryType.TextPopup(_, _, _, _) =>
                  (Seq.empty, None)
              }

              val (newNodes, existingNodes) = nodes.partition(n => props.graphData.nodeSet.get(n.id) == null)
              val nodesToUpdate = existingNodes.filter { (node: UiNode[String]) =>
                val currentNode: vis.Node = props.graphData.nodeSet.get(node.id).merge
                val currentUiNode = currentNode.asInstanceOf[QueryUiVisNodeExt].uiNode
                node != currentUiNode
              }

              val addEvent = QueryUiEvent.Add(
                newNodes,
                edges.filter(e => props.graphData.edgeSet.get(edgeId(e)) == null),
                nodesToUpdate,
                syntheticEdges.filter(e => props.graphData.edgeSet.get(edgeId(e)) == null),
                explodeFromIdOpt,
              )

              if (addEvent.nonEmpty) {
                updateHistory(hist => Some(hist.observe(addEvent)))
              }
              stateVar.update(s => s.copy(runningQueryCount = s.runningQueryCount - 1))
              // Ephemeral result-count overlay trigger (design doc §5): fires the indicator's
              // fade-in on completion. The event payload itself isn't rendered — the indicator
              // shows the live canvas node/edge counters (stateVar.foundNodesCount/
              // foundEdgesCount, updated just above via updateHistory/the addEvent) — but is
              // kept for now as a record of what this query actually produced.
              resultCountBus.emit(
                ResultCountEvent(
                  rawResultCount,
                  nodeCount = if (nodes.length == rawResultCount) None else Some(nodes.length),
                ),
              )
              done.success(())

            case Failure(err) =>
              val message = Option(err.getMessage).filter(_.nonEmpty).getOrElse("Cannot connect to server")
              stateVar.update(s =>
                s.copy(
                  queryBarColor = Some(Styles.queryResultError),
                  runningQueryCount = s.runningQueryCount - 1,
                ),
              )
              // A valid query that simply returns non-node values fails the node/graph run
              // with a TypeMismatch (thrown server-side in QueryUiCypherApiMethods when a node
              // query yields a non-node result). Rather than surface that raw type error, tell
              // the user the query is fine and offer to re-run it as a text/table query — the
              // same "Run as text query" path adhoc cards use.
              if (message.contains("Expected type(s) Node but got value"))
                toastVar.set(
                  Some(
                    ToastMessage(
                      "This query is valid but returns values that aren't nodes. " +
                      "Run it as a text query to see the results.",
                      ToastVariant.Info,
                      Some(
                        ToastAction(
                          "Run as text query",
                          () =>
                            submitQuery(UiQueryType.Text, Some(query), namespaceOverride, parameters)
                              .foreach(_ => ()),
                        ),
                      ),
                    ),
                  ),
                )
              else
                showQueryError(message)
              done.failure(err)
          }
      }
      Some(done.future)
    }
    // Now that submitQuery exists, point the forwarding cell cardsStore's onReRun uses at the
    // real thing — adhoc cards' Re-run always runs as a text/table query (design doc §3: "Adhoc
    // cards are always run as text/table queries"), same path "Run as text query" uses.
    submitQueryRef = (query, _language) => submitQuery(UiQueryType.Text, Some(query), None, Map.empty).isDefined

    /** Run the tap query's Cypher to populate the graph, then overlay any configured synthetic
      * edges by reading their endpoint IDs out of the wiretap message.
      *
      * For each spec, `fromNode` / `toNode` are dot-paths into the wiretap message JSON
      * (today the only supported `nodeIdsFrom`). We chain on `submitQuery`'s completion so
      * the synthetic edges land after the query's nodes/edges — vis-network would handle the
      * other ordering too, but this keeps the visual transition crisp.
      */
    def submitQueryWithSyntheticEdges(
      query: String,
      message: Json,
      syntheticEdgeSpecs: Vector[V2ApiTypes.V2SyntheticEdge],
      parameters: Map[String, Json],
    ): Unit = {
      val graphFut = submitQuery(UiQueryType.Node, queryOverride = Some(query), parameters = parameters)
        .getOrElse(Future.successful(()))
      if (syntheticEdgeSpecs.nonEmpty) {
        graphFut.foreach { _ =>
          val edges = syntheticEdgeSpecs.flatMap { spec =>
            spec.nodeIdsFrom match {
              case "WIRETAP_MESSAGE" =>
                for {
                  fromId <- readStringAtPath(message, spec.fromNode)
                  toId <- readStringAtPath(message, spec.toNode)
                } yield buildEdgeWithDirection(fromId, toId, spec.label, spec.direction)
              case _ => Seq.empty // unknown source — silently skip
            }
          }

          if (edges.nonEmpty) {
            val addEvent = QueryUiEvent.Add(
              nodes = Seq.empty,
              edges = Seq.empty,
              updatedNodes = Seq.empty,
              syntheticEdges = edges.filter(e => props.graphData.edgeSet.get(edgeId(e)) == null),
              explodeFromId = None,
            )
            if (addEvent.nonEmpty) updateHistory(hist => Some(hist.observe(addEvent)))
          }
        }
      }
    }

    /** Walk a dot-separated path into a JSON value and return its string contents, if any.
      *
      *   readStringAtPath({"data":{"even1":"abc"}}, "data.even1") == Some("abc")
      */
    def readStringAtPath(json: Json, path: String): Option[String] =
      path
        .split('.')
        .foldLeft(Option(json.hcursor: io.circe.ACursor)) { (cur, key) =>
          cur.map(_.downField(key))
        }
        .flatMap(_.as[String].toOption)

    def buildEdgeWithDirection(
      fromId: String,
      toId: String,
      label: String,
      direction: String,
    ): UiEdge[String] = direction match {
      case "IN" => UiEdge(toId, label, fromId, isDirected = true)
      case "UNDIRECTED" => UiEdge(fromId, label, toId, isDirected = false)
      case _ => UiEdge(fromId, label, toId, isDirected = true) // default OUT
    }

    // --- Network management ---

    lazy val networkOptions = new vis.Network.Options {
      override val interaction = new vis.Network.Options.Interaction {
        override val hover = true
        override val tooltipDelay = 700
        override val zoomSpeed = 0.3
      }
      override val layout = new vis.Network.Options.Layout {
        override val hierarchical = false: js.Any
        override val improvedLayout = true
        override val randomSeed = 10203040
      }
      override val physics = new vis.Network.Options.Physics {
        override val forceAtlas2Based = new vis.Network.Options.Physics.ForceAtlas2Based {
          override val gravitationalConstant = -26
          override val centralGravity = 0.005
          override val springLength = 230
          override val springConstant = 0.18
          override val avoidOverlap = 1.5
        }
        override val maxVelocity = 25
        override val solver = "forceAtlas2Based"
        override val timestep = 0.25
        override val stabilization = new vis.Network.Options.Physics.Stabilization {
          override val enabled = true
          override val iterations = 150
          override val updateInterval = 25
        }
      }
      override val nodes = new vis.NodeOptions {
        override val shape = "icon"
        override val icon = new vis.NodeOptions.Icon {
          override val face = iconFontFace
        }
      }
      override val edges = new vis.EdgeOptions {
        override val smooth = false
        override val arrows = "to"
      }
    }

    /** Apply the vis network options for a layout mode. Does not touch physics
      * or the `layout` var; callers handle both. Tree mode recomputes positions
      * deterministically, so it needs no physics; graph mode honors node x/y.
      */
    def applyLayoutMode(target: NetworkLayout): Unit = target match {
      case NetworkLayout.Tree =>
        network.foreach(
          _.setOptions(
            new vis.Network.Options {
              override val layout = new vis.Network.Options.Layout {
                override val hierarchical = jsObj(
                  enabled = true,
                  sortMethod = "directed",
                  shakeTowards = "roots",
                )
              }
            },
          ),
        )

      case NetworkLayout.Graph =>
        network.foreach { net =>
          net.setOptions(
            new vis.Network.Options {
              override val layout = new vis.Network.Options.Layout {
                override val hierarchical = false: js.Any
              }
            },
          )

          net.setOptions(new vis.Network.Options {
            override val edges = new vis.EdgeOptions {
              override val smooth = false
              override val arrows = "to"
            }
          })
        }
    }

    lazy val toggleNetworkLayout: () => Unit = { () =>
      withPhysics() {
        layout = layout match {
          case NetworkLayout.Graph => NetworkLayout.Tree
          case NetworkLayout.Tree => NetworkLayout.Graph
        }
        applyLayoutMode(layout)
      }
    }

    lazy val recenterNetworkViewport: () => Unit =
      () =>
        network.get.moveTo(
          new vis.MoveToOptions {
            override val position = new vis.Position {
              val x = 0d
              val y = 0d
            }
            override val scale = 1d
            override val animation = new vis.AnimationOptions {
              val duration = 2000d
              val easingFunction = "easeInOutCubic"
            }
          },
        )

    /** Enable physics synchronously, run `body`, then schedule physics disable.
      * Physics is enabled *before* the body executes so vis.js initializes
      * node positions immediately when they are added to the DataSet.
      * Uses a counter so overlapping calls keep physics alive until the last
      * one expires. The disable is also skipped if the user has manually
      * toggled physics on via the toolbar.
      */
    def withPhysics(millis: Double = 1000)(body: => Unit): Unit = {
      physicsRequestCount += 1
      val physicsOn = new vis.Network.Options {
        override val physics = new vis.Network.Options.Physics { override val enabled = true }
      }
      network.foreach(_.setOptions(physicsOn))
      body
      window.setTimeout(
        () => {
          physicsRequestCount -= 1
          if (physicsRequestCount <= 0 && !stateVar.now().animating) {
            physicsRequestCount = 0
            val physicsOff = new vis.Network.Options {
              override val physics = new vis.Network.Options.Physics { override val enabled = false }
            }
            network.foreach(_.setOptions(physicsOff))
          }
        },
        millis,
      )
      ()
    }

    /** Unfix pinned nodes at drag start so vis-network allows repositioning.
      * They're re-pinned at drag end by [[networkDragEnd]].
      */
    def networkDragStart(event: vis.ClickEvent): Unit = {
      val draggedIds = event.nodes.toSeq
        .map(_.asInstanceOf[String])
        .filterNot(nodeId => network.exists(_.isCluster(nodeId)))
      pinTracker.beginDrag(draggedIds)
    }

    /** Dragging a node pins it in place. Pins are visual state, not history
      * events — they persist via the namespace snapshot's `pinnedNodes`.
      */
    def networkDragEnd(event: vis.ClickEvent): Unit = {
      val draggedIds = event.nodes.toSeq
        .map(_.asInstanceOf[String])
        .filterNot(nodeId => network.exists(_.isCluster(nodeId)))
      pinTracker.pin(draggedIds)
      // Pins/positions live only in the persisted snapshot, so a drag must
      // mark it dirty or the autosave skips it until the next history event.
      persistenceDirty = true
    }

    /** Shift+hold = unpin. The target set is the live selection ∪ {held node},
      * filtered to actually-pinned nodes. Also re-adds the held node to the
      * selection: when the held node was already selected, vis-network's hold
      * path deselects it; prepending undoes that without needing a pre-gesture
      * snapshot (year-ago technique).
      */
    def networkHold(event: vis.ClickEvent): Unit = {
      val shift = event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey
      if (!shift) return
      val heldOpt = network.get
        .getNodeAt(event.pointer.DOM)
        .toOption
        .map(_.asInstanceOf[String])
      val selected = network.get.getSelectedNodes().toSeq.map(_.asInstanceOf[String])
      val unionSet = selected.toSet ++ heldOpt
      val targets = unionSet.toSeq.filterNot(id => network.exists(_.isCluster(id)))
      pinTracker.unpinWithFlash(targets.filter(pinTracker.isPinned))
      persistenceDirty = true
      if (heldOpt.nonEmpty) {
        network.get.selectNodes(
          js.Array[vis.IdType](unionSet.toSeq.map(s => s: vis.IdType): _*),
        )
      }
    }

    /** Implements "shift+click toggles node in selection" — vis-network natively
      * replaces selection on shift+click (multiselect:true only honors Ctrl/Cmd),
      * which fires deselectNode for the previously-selected nodes. We restore
      * them and either remove or add the clicked node depending on prior state.
      */
    def networkDeselect(event: vis.DeselectEvent): Unit = {
      val shift = event.event.asInstanceOf[VisIndirectMouseEvent].srcEvent.shiftKey
      if (!shift) return
      val clickedId = network.get.getNodeAt(event.pointer.DOM).toOption match {
        case None => return
        case Some(nodeId) => nodeId.asInstanceOf[String]
      }
      val prevIds = event.previousSelection.nodes.toSeq.map(_.id.asInstanceOf[String])
      val next =
        if (prevIds.contains(clickedId)) prevIds.filter(_ != clickedId)
        else prevIds :+ clickedId
      network.get.selectNodes(js.Array[vis.IdType](next.map(s => s: vis.IdType): _*))
    }

    def buildContextMenu(nodeId: String, selectedIds: Seq[String], x: Double, y: Double): ContextMenuModel = {

      if (network.get.isCluster(nodeId)) {
        val expandActions = stateVar
          .now()
          .history
          .past
          .find {
            case QueryUiEvent.Collapse(_, cId, _) => cId == nodeId
            case _ => false
          }
          .map(queryUiEvent.invert)
          .toVector
          .map { expand =>
            MenuAction(
              name = "Expand cluster",
              action = () => {
                contextMenuVar.set(None)
                updateHistory(hist => Some(hist.observe(expand)))
              },
            )
          }

        ContextMenuModel.actionsOnly(expandActions)
      } else {
        val startingNodes: Seq[String] = if (selectedIds.contains(nodeId)) selectedIds else Seq(nodeId)
        val visNode: vis.Node = props.graphData.nodeSet.get(nodeId).merge
        val uiNode: UiNode[String] = visNode.asInstanceOf[QueryUiVisNodeExt].uiNode

        val appearance = stateVar.now().uiNodeAppearances.find(_.predicate.matches(uiNode))
        val iconCode = appearance.flatMap(_.icon).getOrElse[String]("")
        val iconColor = appearance
          .flatMap(_.color)
          .getOrElse[String](props.hostColors(Math.floorMod(uiNode.hostIndex, props.hostColors.length)))

        val header: Modifier[HtmlElement] = div(
          cls := Styles.nodePropertiesEyebrowRow,
          NodePropertiesPopup.nodeIcon(iconCode, iconColor),
          span(cls := Styles.nodePropertiesEyebrow, "Node"),
          span(cls := "context-menu-node-id", uiNode.id),
        )

        val actionItems = Vector.newBuilder[MenuAction]
        if (startingNodes.size == 1) {
          actionItems += MenuAction(
            name = "View properties",
            action = () => {
              // Read the context menu's current offset position (may have been
              // dragged from the original right-click location). offsetLeft/Top
              // are parent-relative, matching the coordinate space the popup uses.
              val menuEl = Option(dom.document.querySelector("." + Styles.contextMenu))
                .map(_.asInstanceOf[dom.html.Element])
              val cx = menuEl.fold(x)(_.offsetLeft)
              val cy = menuEl.fold(y)(_.offsetTop)
              contextMenuVar.set(None)
              // Remember how to return to this node's context menu from the popup's back button.
              // The popup passes its current (posX, posY) so the menu reopens where
              // the user dragged the popup, not where the original right-click was.
              nodePopupBackVar.set(Some { (bx, by) =>
                nodePopupVar.set(None)
                contextMenuVar.set(Some(ContextMenuState(bx, by, buildContextMenu(nodeId, selectedIds, bx, by))))
              })
              val propertiesQuery =
                V2QuickQuery("Get Node Details", "RETURN n", V2QuerySort.Text, None).fullQuery(startingNodes)
              nodePopupRefreshVar.set(Some { (rx, ry) =>
                val _ = submitQuery(
                  UiQueryType.TextPopup(rx, ry, iconCode, iconColor),
                  queryOverride = Some(propertiesQuery),
                )
              })
              val _ = submitQuery(
                UiQueryType.TextPopup(cx, cy, iconCode, iconColor),
                queryOverride = Some(propertiesQuery),
              )
            },
          )
        }
        if (selectedIds.length > 1) {
          actionItems += MenuAction(
            name = "Collapse selected nodes",
            action = () => {
              contextMenuVar.set(None)
              Option(window.prompt("Name your cluster:")).foreach { name =>
                updateHistory { hist =>
                  val clusterId = "CLUSTER-" + (Random.nextInt().toLong.abs).toString
                  val clusterEvent = QueryUiEvent.Collapse(selectedIds, clusterId, name)
                  Some(hist.observe(clusterEvent))
                }
              }
            },
          )
        }
        val actionsSection = MenuSection("Actions", actionItems.result())

        val matchedQueries = if (startingNodes.size == 1) {
          quickQueriesFor(uiNode)
        } else {
          val intersectionOfPossibleQuickQueries = startingNodes.iterator
            .map { (startNodeId: String) =>
              val startingVisNode: vis.Node = props.graphData.nodeSet.get(startNodeId).merge
              val startingUiNode: UiNode[String] = startingVisNode.asInstanceOf[QueryUiVisNodeExt].uiNode
              quickQueriesFor(startingUiNode).filter(_.edgeLabel.isEmpty).toSet
            }
            .reduce(_ intersect _)
          quickQueriesFor(uiNode).filter(intersectionOfPossibleQuickQueries contains _)
        }

        val quickQuerySection = MenuSection(
          "Quick Queries",
          matchedQueries.toVector.map { qq =>
            val queryType = qq.sort match {
              case V2QuerySort.Text => UiQueryType.Text
              case V2QuerySort.Node if startingNodes.size == 1 => UiQueryType.NodeFromId(uiNode.id, qq.edgeLabel)
              case V2QuerySort.Node => UiQueryType.Node
            }
            MenuAction(
              name = qq.name,
              action = () => {
                stateVar.update(_.copy(query = qq.fullQuery(startingNodes)))
                contextMenuVar.set(None)
                val _ = submitQuery(queryType)
              },
            )
          },
        )

        val filteredSections = Vector(actionsSection, quickQuerySection)
          .filter(_.actions.nonEmpty)
        ContextMenuModel(
          header = Some(header),
          sections = filteredSections,
          primarySectionIndex = Some(filteredSections.indexOf(quickQuerySection)).filter(_ >= 0),
        )
      }
    }

    def networkRightClick(event: vis.ClickEvent): Unit = {
      val menu = network.get.getNodeAt(event.pointer.DOM).toOption match {
        case None =>
          ContextMenuModel.actionsOnly(
            Vector(
              MenuAction(
                name = "Export SVG",
                action = () => {
                  contextMenuVar.set(None)
                  downloadSvgSnapshot()
                },
              ),
            ),
          )
        case Some(nodeId) =>
          buildContextMenu(
            nodeId.asInstanceOf[String],
            event.nodes.toSeq.asInstanceOf[Seq[String]],
            event.pointer.DOM.x,
            event.pointer.DOM.y,
          )
      }

      nodePopupVar.set(None)
      contextMenuVar.set(
        Some(
          ContextMenuState(
            x = event.pointer.DOM.x,
            y = event.pointer.DOM.y,
            model = menu,
          ),
        ),
      )
    }

    def networkDoubleClick(event: vis.ClickEvent): Unit = {
      val clickedId: String = if (event.nodes.length == 1) {
        event.nodes(0).asInstanceOf[String]
      } else {
        return
      }

      val menu = buildContextMenu(clickedId, Seq.empty, event.pointer.DOM.x, event.pointer.DOM.y)
      menu.firstQueryAction.foreach(_.action())
    }

    def networkKeyDown(event: dom.KeyboardEvent): Unit =
      if (event.key == "Escape") {
        if (contextMenuVar.now().isDefined) contextMenuVar.set(None)
      } else if (event.key == "Delete" || event.key == "Backspace") {
        event.preventDefault()
        val selectedIds = network.get.getSelectedNodes()
        if (selectedIds.nonEmpty) {
          val nodes: Seq[QueryUiEvent.Node] = selectedIds.map { (id: vis.IdType) =>
            val visNode: vis.Node = props.graphData.nodeSet.get(id).merge
            visNode.asInstanceOf[QueryUiVisNodeExt].uiNode
          }.toSeq
          val removeEvent = QueryUiEvent.Remove(nodes, Seq.empty, Seq.empty, Seq.empty, None)
          updateHistory(hist => Some(hist.observe(removeEvent)))
        }
      } else if (event.key == "a" && event.ctrlKey) {
        network.get.selectNodes(props.graphData.nodeSet.getIds())
      }

    def currentLayoutEvent(): QueryUiEvent.Layout = {
      val coords = visualization.readNodePositions()
      val positions = coords.map { case (nodeId, (x, y)) =>
        nodeId -> QueryUiEvent.NodePosition(x, y, pinTracker.isPinned(nodeId))
      }
      // Cluster nodes are not in the DataSet, so record their positions
      // separately or checkpoints would lose where dragged clusters sit
      val clusterPositions = readClusterPositions(stateVar.now().history).map { case (clusterId, (x, y)) =>
        clusterId -> QueryUiEvent.NodePosition(x, y, false)
      }
      QueryUiEvent.Layout(positions ++ clusterPositions)
    }

    def afterNetworkInit(net: vis.Network): Unit = {
      import vis._

      network = Some(net)
      props.onNetworkCreate.foreach(func => func(net))

      net.onDoubleClick(networkDoubleClick)
      net.onContext(networkRightClick)
      net.onDeselectNode(networkDeselect)
      net.onDragStart(networkDragStart)
      net.onDragEnd(networkDragEnd)
      net.onHold(networkHold)

      if (props.initialLayout == NetworkLayout.Tree) {
        // Network initializes in graph mode; switch to tree layout.
        // Can't use toggleNetworkLayout() because the `layout` var already says Tree.
        network.get.setOptions(
          new vis.Network.Options {
            override val layout = new vis.Network.Options.Layout {
              override val hierarchical = jsObj(
                enabled = true,
                sortMethod = "directed",
                shakeTowards = "roots",
              )
            }
          },
        )
      }

      // Restore the current namespace's graph from IndexedDB after reload.
      // Non-current namespaces are lazily restored via ExplorerStore.load when
      // setNamespace switches to them; this handles the active one.
      // Read the persisted namespace selection so we restore the correct graph
      // immediately, avoiding a flash of the default namespace's data.
      val persistedNs = Option(window.sessionStorage.getItem("thatdot.explorer.selectedGraph"))
        .filter(_.nonEmpty)
        .flatMap(NamespaceParameter(_))
      val currentKey = persistedNs.map(namespaceKeyFor).getOrElse(namespaceKeyFor(stateVar.now().namespace))
      val restoredNamespace = persistedNs.getOrElse(stateVar.now().namespace)
      // Set namespace synchronously so that setNamespace (fired later from
      // namespaceSignal when fetchNamespaces completes) sees the namespace
      // already matches and skips, avoiding a race that clears the canvas
      // mid-restore.
      stateVar.update(_.copy(namespace = restoredNamespace))

      // The IndexedDB read is async, so a user switching namespaces before it resolves would
      // otherwise get this stale restore applied on top of the new namespace's canvas. Capture
      // the current version so any setNamespace call in the interim supersedes this restore.
      val mountVersion = setNamespaceVersion
      applySnapshot(
        pending = namespaceSnapshots.remove(currentKey) match {
          case Some(snap) => Future.successful(Some(snap))
          case None => ExplorerStore.load(currentKey).map(_.map(fromFullSnapshot))
        },
        namespace = restoredNamespace,
        reapplyAppearancesEagerly = false,
        stillCurrent = () => setNamespaceVersion == mountVersion,
      )

      ExplorerStore.purgeStale().foreach { count =>
        if (count > 0) dom.console.log(s"[Explorer] Purged $count stale IndexedDB entries")
      }
    }

    def reapplyAppearances(): Unit =
      props.graphData.nodeSet.get().foreach { visNode =>
        val ext = visNode.asInstanceOf[QueryUiVisNodeExt]
        val (uiLabel, iconStyle) = appearanceFor(ext.uiNode)
        props.graphData.nodeSet.update(new vis.Node {
          override val id = ext.uiNode.id
          override val label = uiLabel
          override val icon = iconStyle
        })
      }

    def refetchNodeProperties(namespace: NamespaceParameter, atTime: Option[Long]): Unit = {
      val nodeIds = props.graphData.nodeSet.getIds().map(_.toString).toVector
      if (nodeIds.isEmpty) return
      val idList = nodeIds.map(id => s""""$id"""").mkString("[", ",", "]")
      val query = s"UNWIND $idList AS nId MATCH (n) WHERE strId(n) = nId RETURN n"
      nodeQuery(query, namespace, atTime, QueryLanguage.Cypher, Map.empty).onComplete {
        case Success(Some(freshNodes)) =>
          import js.JSConverters._
          props.graphData.nodeSet.update(freshNodes.map(nodeUi2Vis(_, None)).toJSArray)
          freshNodes.foreach { fresh =>
            val existing = props.graphData.nodeSet.get(fresh.id)
            if (existing != null)
              existing.asInstanceOf[js.Dynamic].uiNode = fresh.asInstanceOf[js.Any]
          }
          reapplyAppearances()
        case Success(None) =>
          dom.console.warn("[Explorer] Node property refetch returned no results")
        case Failure(err) =>
          dom.console.warn(s"[Explorer] Failed to refetch node properties: ${err.getMessage}")
      }
    }

    // --- Checkpoint navigation ---

    /** Events that don't count as a user-visible history step: checkpoint
      * markers (applying one does nothing) and the layout events recorded
      * alongside them at checkpoint creation. Undo/redo slide over these
      * rather than stopping — sliding still applies them, so a layout's
      * positions are restored in passing without ever costing a dead click.
      */
    def isMarker(event: QueryUiEvent): Boolean = event match {
      case _: QueryUiEvent.Checkpoint | _: QueryUiEvent.Layout => true
      case _ => false
    }

    /** Plain undo: slide over any markers, then invert one real event. The
      * step count is computed from a single state read because Laminar queues
      * Var updates issued inside an event-handler transaction: a loop that
      * re-reads `stateVar.now()` after each update would spin forever on the
      * stale head event.
      */
    def undoOne(): Unit = {
      val past = stateVar.now().history.past
      val steps = past.indexWhere(e => !isMarker(e)) match {
        case -1 => past.length
        case realEventIdx => realEventIdx + 1
      }
      for (_ <- 0 until steps) updateHistory(_.stepBack())
    }

    /** Plain redo, mirroring [[undoOne]] — but also consume any markers
      * immediately after the real event. Undo slides a checkpoint's marker
      * pair into the future ahead of the event it annotates; a redo that
      * stopped at the real event would strand those markers at the head of
      * the future, costing a dead click to slide back over them. With no real
      * event ahead the whole (marker-only) future is a checkpoint: consume it
      * all, landing on the checkpoint with its layout applied in passing.
      */
    def redoOne(): Unit = {
      val future = stateVar.now().history.future
      val steps = future.indexWhere(e => !isMarker(e)) match {
        case -1 => future.length
        case realEventIdx =>
          val afterReal = realEventIdx + 1
          afterReal + future.drop(afterReal).takeWhile(isMarker).length
      }
      for (_ <- 0 until steps) updateHistory(_.stepForward())
    }

    /** Walk the stream back to the nearest (or named) checkpoint marker,
      * replaying inverses along the way. The marker stays in the past (the
      * "present" position); the layout recorded under it at creation is then
      * re-applied transiently so node positions match the checkpoint. With no
      * marker found this undoes everything.
      */
    def stepBackToCheckpoint(name: Option[String] = None): Unit = {
      val past = stateVar.now().history.past
      // Unnamed = "Previous Checkpoint". Standing at a checkpoint leaves its
      // marker pair at the head of the past; searching from index 0 would
      // match it again and go nowhere, so skip the pair to reach the one
      // before it. A named jump may target the head marker deliberately (the
      // menu's "(present)" entry re-snaps positions), so it searches from 0.
      val searchFrom = (name, past) match {
        case (None, QueryUiEvent.Checkpoint(_) :: (_: QueryUiEvent.Layout) :: _) => 2
        case (None, QueryUiEvent.Checkpoint(_) :: _) => 1
        case _ => 0
      }
      val steps = past.indexWhere(
        {
          case QueryUiEvent.Checkpoint(n) => name.forall(_ == n)
          case _ => false
        },
        searchFrom,
      ) match {
        case -1 => past.length
        case markerIdx => markerIdx
      }
      for (_ <- 0 until steps) updateHistory(_.stepBack())
      past.drop(steps) match {
        case QueryUiEvent.Checkpoint(_) :: (layout: QueryUiEvent.Layout) :: _ =>
          // After the queued step-backs have run, not before
          val _ = window.setTimeout(() => queryUiEvent.applyEvent(layout), 0)
        case _ =>
      }
    }

    /** Forward mirror of [[stepBackToCheckpoint]]; consumes the marker (and
      * the layout event preceding it, which restores the checkpoint's node
      * positions on the way through) so the checkpoint ends at the "present"
      * position.
      */
    def stepForwardToCheckpoint(name: Option[String] = None): Unit = {
      val future = stateVar.now().history.future
      val steps = future.indexWhere {
        case QueryUiEvent.Checkpoint(n) => name.forall(_ == n)
        case _ => false
      } match {
        case -1 => future.length
        case markerIdx => markerIdx + 1
      }
      for (_ <- 0 until steps) updateHistory(_.stepForward())
    }

    /** One menu entry per checkpoint marker in the stream, labeled by where
      * it sits relative to the present; selecting one walks the timeline to
      * it (never truncating the redo stack).
      */
    def checkpointContextMenuItems(): Seq[ContextMenuItem] = {
      val state = stateVar.now()
      val contextItems = Seq.newBuilder[ContextMenuItem]

      for (event <- state.history.future.reverse)
        event match {
          case QueryUiEvent.Checkpoint(name) =>
            val stepForward = () => {
              contextMenuVar.set(None)
              stepForwardToCheckpoint(Some(name))
            }
            contextItems += ContextMenuItem(div(name, em(" (future)")), name, stepForward)
          case _ =>
        }

      for ((event, idx) <- state.history.past.zipWithIndex)
        event match {
          case QueryUiEvent.Checkpoint(name) =>
            val item: HtmlElement = div(name, em(if (idx == 0) " (present)" else " (past)"))
            val stepBack = () => {
              contextMenuVar.set(None)
              stepBackToCheckpoint(Some(name))
            }
            contextItems += ContextMenuItem(item, name, stepBack)
          case _ =>
        }

      contextItems.result()
    }

    def setAtTime(atTime: Option[Long]): Unit =
      if (atTime != stateVar.now().atTime) {
        props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
        props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
        ExplorerStore.remove(namespaceKeyFor(stateVar.now().namespace))
        stateVar.update(
          _.copy(
            queryBarColor = None,
            history = History.empty,
            animating = false,
            foundNodesCount = None,
            foundEdgesCount = None,
            atTime = atTime,
          ),
        )
        resultsVar.set(None)
        contextMenuVar.set(None)
      }

    /** Switch the explorer to `namespace`. No-op if already on it. Snapshots and persists the
      * outgoing namespace (in-memory `namespaceSnapshots` and `ExplorerStore`), closes its
      * websocket clients, then restores the target namespace from the first source that has
      * it: the in-memory cache, then `ExplorerStore` (IndexedDB), then a fresh/empty state.
      * Every restore path is guarded by `setNamespaceVersion` (via `stillCurrent`) so a rapid
      * second switch can't be clobbered by a slower first one resolving late.
      */
    def setNamespace(namespace: NamespaceParameter): Unit = {
      val previous = stateVar.now()
      if (namespace != previous.namespace) {
        // Close websockets for the old namespace — the new namespace gets
        // its own connection lazily via getWebSocketClient/getWebSocketClientV2
        val oldKey = namespaceKeyFor(previous.namespace)
        wsClients.remove(oldKey).foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))
        wsClientsV2.remove(oldKey).foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))

        // Clear in-flight query state so the spinner and "pending text query"
        // guard don't leak into the target namespace
        stateVar.update(_.copy(runningQueryCount = 0, pendingTextQueries = Set.empty))

        // Snapshot the outgoing namespace's view so its tab can be restored later.
        // Live positions are merged into the stored nodes; physics is disabled at
        // rest, so they hold when the nodes are added back.
        snapshotFor(previous).foreach { case (key, snap) =>
          namespaceSnapshots(key) = snap
          ExplorerStore.save(key, toFullSnapshot(snap))
        }

        // Cards are namespace-scoped, like everything else persisted: the outgoing set
        // was just captured above, so drop it client-side (no server-side closes — the
        // wiretap runtime renews its per-namespace store on switch and closes the old
        // namespace's sockets itself) and forget any in-flight tap opens/restarts so a
        // stale key can't spawn a card in the target namespace.
        cardsStore.resetForNamespaceSwitch()
        pendingTabularOpensVar.set(Set.empty)
        pendingTabularRestartsVar.set(Set.empty)

        // Clear the graph and swap in the target namespace's saved view, if any
        props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
        props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
        resultsVar.set(None)
        contextMenuVar.set(None)
        setNamespaceVersion += 1
        val myVersion = setNamespaceVersion
        def stillCurrent(): Boolean = setNamespaceVersion == myVersion
        val targetKey = namespaceKeyFor(namespace)

        namespaceSnapshots.get(targetKey) match {
          case Some(snapshot) =>
            applySnapshot(
              pending = Future.successful(Some(snapshot)),
              namespace = namespace,
              reapplyAppearancesEagerly = true,
              stillCurrent = () => stillCurrent(),
            )

          case None =>
            ExplorerStore.load(targetKey).recover { case _ => None }.foreach {
              case Some(fullSnapshot) =>
                applySnapshot(
                  pending = Future.successful(Some(fromFullSnapshot(fullSnapshot))),
                  namespace = namespace,
                  reapplyAppearancesEagerly = true,
                  stillCurrent = () => stillCurrent(),
                )

              case None =>
                // No saved view for this namespace — set up a fresh/default state. Deferred
                // two animation frames and version-guarded the same way applySnapshot is, so a
                // rapid second switch can't be clobbered by this one resolving late. Only take
                // ownership of restoringVar while still current: a superseded restore that set
                // it true here would never reach the guarded clear below, leaving the loading
                // spinner stuck on after the newer restore already finished.
                if (stillCurrent()) {
                  restoringVar.set(true)
                  val _ = window.requestAnimationFrame { (_: Double) =>
                    val _ = window.requestAnimationFrame { (_: Double) =>
                      if (stillCurrent())
                        try {
                          if (layout != props.initialLayout) {
                            layout = props.initialLayout
                            applyLayoutMode(layout)
                          }
                          pinTracker.resetStateOnly(Set.empty)
                          stateVar.update(
                            _.copy(
                              query = props.initialQuery,
                              queryBarColor = None,
                              history = History.empty,
                              animating = false,
                              foundNodesCount = None,
                              foundEdgesCount = None,
                              atTime = props.initialAtTime,
                              namespace = namespace,
                            ),
                          )
                          resultsHistory.entries.set(Vector.empty)
                          resultsHistory.currentIdx.set(-1)
                          resultsCollapsedVar.set(false)
                          // Run the initial query the first time a namespace is opened
                          if (props.initialQuery.nonEmpty) {
                            val _ = submitQuery(
                              UiQueryType.Node,
                              queryOverride = Some(props.initialQuery),
                              namespaceOverride = Some(namespace),
                            )
                          }
                        } finally restoringVar.set(false)
                    }
                  }
                }
            }
        }
      }
    }

    def cancelQueries(queryIds: Option[Iterable[QueryId]] = None): Unit = {
      val namespace = stateVar.now().namespace
      props.queryMethod match {
        case QueryMethod.WebSocket =>
          getWebSocketClient(namespace).value.flatMap(_.toOption).foreach { (client: WebSocketQueryClient) =>
            val queries = queryIds.getOrElse(client.activeQueries.keys).toList
            if (queries.nonEmpty && window.confirm(s"Cancel ${queries.size} running query execution(s)?")) {
              queries.foreach(queryId => client.cancelQuery(queryId))
            }
          }

        case QueryMethod.WebSocketV2 =>
          getWebSocketClientV2(namespace).value.flatMap(_.toOption).foreach { (client: V2WebSocketQueryClient) =>
            val queries = queryIds.getOrElse(client.activeQueries.keys).toList
            if (queries.nonEmpty && window.confirm(s"Cancel ${queries.size} running query execution(s)?")) {
              queries.foreach(queryId => client.cancelQuery(queryId))
            }
          }

        case QueryMethod.Restful | QueryMethod.RestfulV2 =>
          window.alert("You cannot cancel queries when issuing queries through the REST api")
      }
    }

    // --- Build the UI ---

    // Step counts come from one state read: Var updates issued inside an
    // event-handler transaction are queued, so a re-reading loop never
    // observes progress (see undoOne)
    def stepBackAll(): Unit =
      for (_ <- 0 until stateVar.now().history.past.length) updateHistory(_.stepBack())
    def stepForwardAll(): Unit =
      for (_ <- 0 until stateVar.now().history.future.length) updateHistory(_.stepForward())

    /** Hidden host for the tap-query match dispatch subscription. Mounted unconditionally
      * so dispatch stays alive for the full lifetime of QueryUi. The wiretap sockets
      * themselves live in the DataService, which renews them per graph namespace.
      */
    val wiretapLifecycle: HtmlElement =
      div(
        display := "none",
        // Tap query dispatch host — joins the service's enabled tap queries (the V2TapQuery
        // context registered by whichever UI enabled one locally, keyed by tap-query name)
        // with its live handlers under TapQueryOwner; only names present in both get a
        // dispatch span. `splitSeq` keys those by tap-query name so each span is created
        // once when a tap query becomes (enabled AND live) and torn down when either
        // condition drops — without disturbing the others when the entry set changes.
        children <-- props.dataService.wiretapsSignal
          .combineWith(props.dataService.enabledTapQueriesSignal)
          .map { case (active, meta) =>
            val tapHandlersByName: Map[String, WiretapHandler] =
              active.getOrElse(ExplorerSettingsModal.TapQueryOwner, List.empty).map(h => h.key -> h).toMap
            meta.iterator.flatMap { case (name, tapQuery) =>
              tapHandlersByName.get(name).map(h => (name, tapQuery, h))
            }.toList
          }
          .splitSeq(_._1) { strictSignal =>
            val handler = strictSignal.now()._3
            span(
              display := "none",
              handler.matches --> Observer[Json] { message =>
                // Read the tap query fresh on each match rather than capturing it at span
                // creation, so edits to the query or synthetic edges apply live: the tap-query
                // name (and thus this span) is unchanged, so `strictSignal` carries the update.
                val tapQuery = strictSignal.now()._2
                // The wiretap envelope is always a JSON object (`{meta: ..., data: ...}`), so each
                // top-level field becomes a Cypher parameter: queries reference `$data.foo`,
                // `$meta.isPositiveMatch`, etc. Values flow via the parameter map, so no
                //  risk of injection from the wiretap message itself.
                val params = message.asObject.map(_.toMap).getOrElse(Map.empty[String, Json])
                submitQueryWithSyntheticEdges(
                  tapQuery.query,
                  message,
                  tapQuery.syntheticEdges,
                  parameters = params,
                )
              },
            )
          },
      )

    div(
      height := "100%",
      width := "100%",
      overflow := "hidden",
      position := "relative",
      display := "flex",
      flexDirection := "column",
      // Subscribe to external namespace changes (if provided).
      // Using signal --> (not _.updates) so we receive the current value on mount,
      // avoiding a race where fetchNamespaces completes before the subscription is active.
      props.namespaceSignal.map(_ --> { ns => setNamespace(ns) }).getOrElse(emptyMod),
      // Drop saved view state and connections for namespaces deleted server-side,
      // so a recreated graph with the same name starts fresh
      props.invalidatedNamespaces
        .map(_ --> { ns =>
          val key = namespaceKeyFor(ns)
          namespaceSnapshots.remove(key)
          ExplorerStore.remove(key)
          wsClients.remove(key).foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))
          wsClientsV2.remove(key).foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))
          // If the deleted namespace is the one currently displayed, clear its
          // graph data so the user doesn't see stale nodes from a gone namespace
          if (ns == stateVar.now().namespace) {
            props.graphData.nodeSet.remove(props.graphData.nodeSet.getIds())
            props.graphData.edgeSet.remove(props.graphData.edgeSet.getIds())
            stateVar.update(
              _.copy(
                history = History.empty,
                foundNodesCount = None,
                foundEdgesCount = None,
                runningQueryCount = 0,
                pendingTextQueries = Set.empty,
              ),
            )
          }
        })
        .getOrElse(emptyMod),
      // The shared DataService feeds keep the explorer's UI configuration current for as
      // long as it is mounted; saves still write stateVar optimistically and re-converge
      // on the feed's next data change.
      if (canViewUiData)
        List[Modifier[HtmlElement]](
          props.dataService.nodeAppearancesSignal --> { nas =>
            stateVar.update(_.copy(uiNodeAppearances = nas))
          },
          props.dataService.sampleQueriesSignal --> { sqs =>
            stateVar.update(_.copy(sampleQueries = sqs))
          },
          props.dataService.quickQueriesSignal --> { qqs =>
            stateVar.update(_.copy(uiNodeQuickQueries = qqs.map(QuickQueryConversions.v1ToV2)))
          },
        )
      else List.empty[Modifier[HtmlElement]],
      // Subscribe to external refresh trigger (if provided) — re-runs initial query
      props.refreshSignal
        .map(_ --> { _ =>
          if (props.initialQuery.nonEmpty) {
            val _ = submitQuery(UiQueryType.Node, queryOverride = Some(props.initialQuery))
          }
        })
        .getOrElse(emptyMod),
      // Results-panel changes (new runs, history navigation, collapse) don't go through
      // updateHistory, so mark persistence dirty here for the periodic autosave. Restores
      // also set these Vars, which costs at most one redundant save per interval.
      resultsHistory.entries.signal.updates --> { _ => persistenceDirty = true },
      resultsHistory.currentIdx.signal.updates --> { _ => persistenceDirty = true },
      resultsCollapsedVar.signal.updates --> { _ => persistenceDirty = true },
      // Card list / expansion changes ride the same periodic autosave. Viewer-level Var
      // tweaks (search, sort, column widths) don't tick these signals — like pan/zoom,
      // they're flushed by the keep-alive save, namespace switch, and beforeunload.
      cardsStore.cards.updates --> { _ => persistenceDirty = true },
      cardsStore.expandedId.updates --> { _ => persistenceDirty = true },
      onMountCallback { _ =>
        val PeriodicSaveIntervalMs = 30000d
        // Force a save at least this often even when nothing is dirty, so this tab's
        // savedAt stays fresh and another tab's purgeStale can't GC a live-but-idle
        // tab's current snapshot as if its tab had closed.
        val KeepAliveTicks = 20 // 10 minutes
        var ticksSinceSave = 0
        val periodicSaveInterval = window.setInterval(
          () => {
            ticksSinceSave += 1
            if (persistenceDirty || ticksSinceSave >= KeepAliveTicks) {
              persistenceDirty = false
              ticksSinceSave = 0
              persistCurrentNamespace()
            }
          },
          PeriodicSaveIntervalMs,
        )
        val beforeUnloadHandler: js.Function1[dom.BeforeUnloadEvent, Unit] = { (_: dom.BeforeUnloadEvent) =>
          window.clearInterval(periodicSaveInterval)
          persistCurrentNamespace()
        }
        window.addEventListener("beforeunload", beforeUnloadHandler)

        // When a namespaceSignal is provided, the signal --> subscription above handles
        // initialization (including the initial value on mount). Otherwise, initialize directly.
        if (props.namespaceSignal.isDefined) () // signal --> handles everything
        else {
          val namespace = stateVar.now().namespace
          props.queryMethod match {
            case QueryMethod.WebSocket => getWebSocketClient(namespace)
            case QueryMethod.WebSocketV2 => getWebSocketClientV2(namespace)
            case _ => ()
          }
          if (props.initialQuery.nonEmpty) {
            val _ = submitQuery(UiQueryType.Node)
          }
        }
      },
      // TopBar
      if (props.isQueryBarVisible) {
        // Lifted out of HistoryNavigationButtons' construction (design doc §4: the reload icon
        // moved into the junk drawer's Maintenance section, wired below).
        // Multi-namespace hosts announce themselves one of two ways: enterprise injects a
        // graph selector (`queryBarTrailing`), Novelty drives its Model dropdown through
        // `namespaceSignal`. OSS sets neither — the one single-namespace host — and never
        // sees namespace wording: its confirm drops the scope clause and the drawer offers
        // no all-namespaces purge at all (see JunkDrawer's Maintenance section).
        val multiNamespace = props.queryBarTrailing.isDefined || props.namespaceSignal.isDefined
        val resetGraph: () => Unit = () =>
          if (
            window.confirm(
              "Reset Canvas?\n\nThis will clear all nodes, edges, and results from the canvas " +
              "and delete persisted browser session storage" +
              (if (multiNamespace) " for this namespace." else "."),
            )
          ) {
            val nsKey = namespaceKeyFor(stateVar.now().namespace)
            namespaceSnapshots.remove(nsKey)
            ExplorerStore.remove(nsKey)
            clearCanvas()
          }
        val resetAllNamespaces: () => Unit = () =>
          if (
            window.confirm(
              "Clear All Namespaces?\n\nThis will clear the canvas and delete all persisted " +
              "browser state for every namespace in this tab.",
            )
          ) {
            namespaceSnapshots.clear()
            ExplorerStore.clear()
            wsClients.values.foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))
            wsClientsV2.values.foreach(_.value.flatMap(_.toOption).foreach(_.webSocket.close()))
            wsClients.clear()
            wsClientsV2.clear()
            clearCanvas()
          }
        TopBar(
          query = stateVar.signal.map(_.query),
          updateQuery = (newQuery: String) => stateVar.update(_.copy(queryBarColor = None, query = newQuery)),
          // .distinct: every stateVar update (query-bar color reset, canvas updates, keystrokes)
          // re-emits; downstream `child <--` blocks rebuild the query menu button on each
          // emission, tearing down its open popover and document listeners.
          runningTextQuery = stateVar.signal.map(_.pendingTextQueries.nonEmpty).distinct,
          queryBarColor = stateVar.signal.map(_.queryBarColor),
          sampleQueries = stateVar.signal.map(_.sampleQueries),
          submitButton = (uiQueryType: UiQueryType) => {
            val _ = submitQuery(uiQueryType)
          },
          cancelButton = () => cancelQueries(Some(stateVar.now().pendingTextQueries)),
          // baseUrlOpt is ClientRoutes' effective base URL (empty ⇒ same origin); MonacoQueryInput
          // derives the LSP WebSocket URL from it via lspWebSocketUrl.
          serverUrl = props.routes.baseUrlOpt,
          navButtons = HistoryNavigationButtons(
            // Backward is marker-aware: markers at the head of the past mean
            // "standing at a checkpoint now", so undoing only them changes
            // nothing visible. Forward is not: markers only enter the future as
            // a checkpoint's [Layout, Checkpoint] pair, so a marker-only future
            // still holds a whole checkpoint that redoOne can step onto.
            canStepBackward = stateVar.signal.map(_.history.past.exists(e => !isMarker(e))),
            canStepForward = stateVar.signal.map(_.history.future.nonEmpty),
            isAnimating = stateVar.signal.map(_.animating),
            undo = () => undoOne(),
            undoMany = () => stepBackToCheckpoint(),
            undoAll = () => stepBackAll(),
            animate = () => stateVar.update(s => s.copy(animating = !s.animating)),
            redo = () => redoOne(),
            redoMany = () => stepForwardToCheckpoint(),
            redoAll = () => stepForwardAll(),
            makeCheckpoint = () => {
              val hist = stateVar.now().history
              val existingNames = (hist.past.iterator ++ hist.future.iterator).collect {
                case QueryUiEvent.Checkpoint(n) => n
              }.toSet
              // Checkpoint names key the navigation menu, so blank or duplicate
              // names would make its entries ambiguous — re-prompt instead
              @tailrec def promptName(message: String): Option[String] =
                Option(window.prompt(message)).map(_.trim) match {
                  case None => None
                  case Some("") => promptName("Name your checkpoint (a name is required)")
                  case Some(name) if existingNames.contains(name) =>
                    promptName(s"""A checkpoint named "$name" already exists. Pick another name""")
                  case some => some
                }
              promptName("Name your checkpoint").foreach { name =>
                // Record the current layout under the marker so checkpoint
                // navigation restores node positions; undo/redo slide over
                // both without costing a click (see isMarker)
                updateHistory(hist => Some(hist.observe(currentLayoutEvent())))
                updateHistory(hist => Some(hist.observe(QueryUiEvent.Checkpoint(name))))
              }
            },
            checkpointMenuItems = () =>
              checkpointContextMenuItems().map { item =>
                ToolbarButton.MenuAction(item.item, item.title, item.action)
              },
            checkpointMenuItemsAvailable = stateVar.signal.map(_.history match {
              case History(past, future) =>
                (past.iterator ++ future.iterator).exists(_.isInstanceOf[QueryUiEvent.Checkpoint])
            }),
            downloadHistory = (snapshotOnly: Boolean) => {
              // Prepend current positions to the downloaded file only: recording a
              // Layout event into live history would truncate the redo stack.
              val layout = currentLayoutEvent()
              val base = if (snapshotOnly) makeSnapshot() else stateVar.now().history
              val history = base.copy(past = layout :: base.past)
              downloadHistoryFile(history, if (snapshotOnly) "snapshot.json" else "history.json")
            },
            downloadGraphJsonLd = () => downloadGraphJsonLd(),
            uploadHistory = files => uploadHistory(files),
            atTime = stateVar.signal.map(_.atTime),
            canSetTime = stateVar.signal.map(_.runningQueryCount == 0),
            setTime = setAtTime(_),
            toggleLayout = toggleNetworkLayout,
            recenterViewport = recenterNetworkViewport,
          ),
          useV2Api = props.queryMethod match {
            case QueryMethod.RestfulV2 | QueryMethod.WebSocketV2 => true
            case QueryMethod.Restful | QueryMethod.WebSocket => false
          },
          qpEnabled = props.qpEnabled,
          // The JunkDrawer mounts in every host — it carries the only canvas-reset and
          // persisted-state-purge affordances, so it must not depend on the optional
          // enterprise-injected `queryBarTrailing` content. That content (the graph
          // selector) just fills the drawer's Graph section when present.
          trailing = Some(
            JunkDrawer(
              activeGraphName = stateVar.signal.map(_.namespace.namespaceId),
              graphSection = props.queryBarTrailing,
              multiNamespace = multiNamespace,
              // Opens the Explorer Settings modal mounted below (no navigation — the old
              // dedicated settings page is gone). Hidden for users lacking the settings
              // write permissions, matching the gate the old nav item used.
              showExplorerSettings = canEditExplorerSettings,
              onOpenExplorerSettings = () => settingsModalOpenVar.set(true),
              onClearNamespace = resetGraph,
              onResetAllNamespaces = resetAllNamespaces,
              // Selecting a graph in the drawer's namespace list changes the namespace; close
              // the drawer on that change like a menu selection.
              closeOn = stateVar.signal.map(_.namespace).distinct.updates.mapTo(()),
            ),
          ),
          permissions = props.permissions,
          bookmarkDialog = SampleQueryBookmark.editDialog(
            bookmarkDraftVar,
            onSave = (existingIdx, sq) => {
              val current = stateVar.now().sampleQueries
              val updated = existingIdx match {
                case Some(idx) if idx < current.size => current.updated(idx, sq)
                case _ => current :+ sq
              }
              saveSampleQueries(updated)
            },
            onDelete = idx => {
              val current = stateVar.now().sampleQueries
              saveSampleQueries(current.patch(idx, Nil, 1))
            },
          ),
          onBookmark = () => toggleBookmarkDialog(),
          onOpenTapModal = () => openTapModal(),
        )
      } else emptyNode,
      // Canvas region: everything below the TopBar (VisNetwork plus every overlay anchored to
      // it — Loader, node-properties popup, result cards, the ephemeral count indicator, the
      // context menu). `position: relative` here — not on the outer flex container above, which
      // spans the TopBar too — is what makes `.cards-drawer` / `.card-popup` / the loader's
      // `right: 0` etc. resolve against the visible canvas box instead of a box that starts at
      // the TopBar's top edge. `flex: 1` + `min-height: 0` lets it fill exactly the space the
      // TopBar doesn't take (flex-basis 0 avoids the intrinsic min-content height a plain flex
      // item would otherwise impose, which is what let this region overflow its own container).
      div(
        cls := Styles.canvasRegion,
        position := "relative",
        flex := "1 1 0%",
        minHeight := "0",
        width := "100%",
        overflow := "hidden",
        // Loader — shows for running queries and during graph restore
        child <-- stateVar.signal.map(_.runningQueryCount).combineWith(restoringVar.signal).map {
          case (queryCount, restoring) =>
            val pendingCount = queryCount + (if (restoring) 1 else 0)
            Loader(
              pendingCount,
              if (
                queryCount > 0 &&
                (props.queryMethod == QueryMethod.WebSocket || props.queryMethod == QueryMethod.WebSocketV2)
              )
                Some(() => cancelQueries())
              else None,
            )
        },
        // VisNetwork
        VisNetwork(
          data = props.graphData,
          afterNetworkInit = afterNetworkInit,
          clickHandler = _ => contextMenuVar.set(None),
          contextMenuHandler = _.preventDefault(),
          keyDownHandler = networkKeyDown,
          options = networkOptions,
        ),
        stateVar.signal.map(_.uiNodeAppearances).distinct.updates --> { _ =>
          reapplyAppearances()
        },
        // Physics toggle
        // `.distinct` is essential: `Signal.map` does not dedupe, so without it this
        // subscriber fires on every unrelated `stateVar` update and overrides the
        // physics state that `withPhysics` is trying to maintain.
        stateVar.signal.map(_.animating).distinct.updates --> { animating =>
          network.foreach(_.setOptions(new vis.Network.Options {
            override val physics = new vis.Network.Options.Physics {
              override val enabled = animating
            }
          }))
        },
        // Node properties popup
        child <-- nodePopupVar.signal.map {
          case Some(state) =>
            val canWrite = props.permissions match {
              case Some(perms) => Set("GraphWrite").subsetOf(perms)
              case None => true
            }
            NodePropertiesPopup(
              state,
              closePopup = () => nodePopupVar.set(None),
              submitQuery =
                query => submitQuery(UiQueryType.Node, queryOverride = Some(query)).getOrElse(Future.successful(())),
              onBack = (bx, by) => nodePopupBackVar.now().fold(nodePopupVar.set(None))(_(bx, by)),
              onRefreshProperties = (rx, ry) => nodePopupRefreshVar.now().foreach(_(rx, ry)),
              canWrite = canWrite,
            )
          case None => emptyNode
        },
        // Result cards (exploration UI redesign): right-edge minimized drawer + the one
        // expanded popup, floating over the canvas. Adhoc query results auto-capture into
        // cards via cardsStore.lifecycleMods; the old ResultsPanel drawer/CanvasDoor no
        // longer mount here, so adhoc runs land only in cards (design doc §3, phase 1).
        MinimizedDrawer(
          cardsStore.minimizedCards,
          cardsStore.drawerSearch,
          cardsStore.dispatch,
          hasCards = cardsStore.cards.map(_.nonEmpty),
        ),
        CardPopup(
          cardsStore.expandedCard,
          cardsStore.dispatch,
        ),
        // Bottom-left graph-feed pills: the live on/off switch for each feed the
        // user keeps in their graph menu (see GraphFeedChips). Mounted unconditionally:
        // it needs only tap-query read, and `tapQueriesSignal` self-gates to `Pot.Empty`
        // without that permission, so no unpermitted polling starts.
        GraphFeedChips(
          tapQueries = props.dataService.tapQueriesSignal,
          wiretap = props.dataService,
          prefs = graphMenuPrefs,
          currentNamespace = props.dataService.currentNamespaceSignal.map(_.namespaceId),
        ),
        graphMenuPrefs.mods,
        cardsStore.lifecycleMods,
        resolvePendingTabularOpens,
        // Ephemeral result-count overlay (design doc §5), same layer/positioning family as
        // the Loader above.
        ResultCountIndicator(
          resultCountBus.events,
          // `.distinct` so the indicator's counters DOM isn't rebuilt on every unrelated
          // stateVar tick (e.g. per keystroke in the query bar) — same reason the other
          // stateVar projections below distinct.
          nodeCount = stateVar.signal.map(_.foundNodesCount).distinct,
          edgeCount = stateVar.signal.map(_.foundEdgesCount).distinct,
          fadeMs = CardDefaults.IndicatorFadeMs,
        ),
        // ContextMenu
        child <-- contextMenuVar.signal.map {
          case Some(ContextMenuState(x, y, model)) => ContextMenu.fromModel(x, y, model)
          case None => emptyNode
        },
      ), // end canvas region
      // Standing-query tap modal (design doc §2): opened from the query split button's
      // "Standing query results…" item. Fixed-position overlay (`.tap-modal-overlay`
      // is `position: fixed; inset: 0`), so it is mounted outside the canvas region rather
      // than anchored to it — a true viewport-level modal, not a canvas overlay.
      TapModal(
        openSignal = tapModalOpenVar.signal,
        setOpen = tapModalOpenVar.writer,
        catalog = tapCatalogSignal,
        tappedKeys = tappedKeys,
        onOpenTabular = (target: TapTarget) => openTabularTap(target),
        onFocusExisting = (target: TapTarget) => cardsStore.focusTarget(target),
      ),
      // Explorer Settings modal (junk drawer → Configure → "Explorer settings…"): the
      // tap-query / sample-query / quick-query / node-appearance catalogs, overlaying the
      // explorer instead of navigating to the old dedicated settings page. Same
      // viewport-level fixed overlay as the tap modal above. Mounted only for users
      // allowed to edit settings: the modal's feed binders subscribe on mount (the modal
      // is always mounted, only display-toggled), and an unpermitted user's subscription
      // would start the very polls `canViewUiData` gates off above.
      if (canEditExplorerSettings)
        ExplorerSettingsModal(
          openSignal = settingsModalOpenVar.signal,
          setOpen = settingsModalOpenVar.writer,
          dataService = props.dataService,
          editorConfig = editorConfig,
          menuPrefs = graphMenuPrefs,
        )
      else emptyNode,
      // WiretapStore lifecycle + tap-query dispatch host. Mounted as long as QueryUi is
      // mounted so subscriptions survive across UI state; the namespace-keyed inner child
      // renews the WiretapStore on namespace change and tears down all open WebSockets
      // on unmount.
      wiretapLifecycle,
      // Toast notifications
      Toast(toastVar),
    )
  }

  /** Create a QueryUi from options (replacing makeQueryUi) */
  def fromOptions(
    options: QueryUiOptions,
    routes: ClientRoutes,
    dataService: DataService,
    permissions: Option[Set[String]] = None,
    namespaceSignal: Option[Signal[NamespaceParameter]] = None,
    initialNamespace: NamespaceParameter = NamespaceParameter.defaultNamespaceParameter,
    refreshSignal: Option[EventStream[Unit]] = None,
    queryBarTrailing: Option[HtmlElement] = None,
    invalidatedNamespaces: Option[EventStream[NamespaceParameter]] = None,
  ): HtmlElement = {
    val nodeSet = options.visNodeSet.getOrElse(new vis.DataSet(js.Array[vis.Node]()))
    val edgeSet = options.visEdgeSet.getOrElse(new vis.DataSet(js.Array[vis.Edge]()))
    val visData = new vis.Data {
      override val nodes = nodeSet
      override val edges = edgeSet
    }

    val queryMethod = QueryMethod.parseQueryMethod(options)

    apply(
      Props(
        routes = routes,
        dataService = dataService,
        graphData = VisData(visData, nodeSet, edgeSet),
        initialQuery = options.initialQuery.getOrElse(""),
        nodeResultSizeLimit = options.nodeResultSizeLimit.getOrElse(100).toLong,
        onNetworkCreate = options.onNetworkCreate.toOption,
        isQueryBarVisible = options.isQueryBarVisible.getOrElse(true),
        showEdgeLabels = options.showEdgeLabels.getOrElse(true),
        showHostInTooltip = options.showHostInTooltip.getOrElse(true),
        initialAtTime = options.queryHistoricalTime.toOption.map(_.toLong),
        initialLayout = options.layout.getOrElse("graph").toLowerCase match {
          case "tree" => NetworkLayout.Tree
          case "graph" | _ => NetworkLayout.Graph
        },
        queryMethod = queryMethod,
        qpEnabled = options.qpEnabled.getOrElse(false),
        permissions = permissions,
        namespaceSignal = namespaceSignal,
        initialNamespace = initialNamespace,
        refreshSignal = refreshSignal,
        queryBarTrailing = queryBarTrailing,
        invalidatedNamespaces = invalidatedNamespaces,
      ),
    )
  }
}
