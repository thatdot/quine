package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import org.scalajs.dom.window

import com.thatdot.quine.routes._
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.streams.EmbeddedEditorConfig
import com.thatdot.quine.webapp.components.{Toast, ToastMessage, ToastVariant}
import com.thatdot.quine.webapp.dataservice.{
  DataService,
  QueryUiConfigService,
  SaveFailed,
  SaveResult,
  SaveSucceeded,
  TapQueryService,
  WiretapOwner,
  WiretapService,
}
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2TapQuery}

/** Explorer Settings as a modal overlaying the explorer (replacing the dedicated
  * Explorer Settings nav page): the tap-query / sample-query / quick-query /
  * node-appearance catalogs, opened from the junk drawer's Configure section. Reuses the
  * tap modal's shell classes (overlay, dialog, header, body) so both explorer modals
  * share one visual language; only the width differs (`.explorer-settings-dialog`).
  */
object ExplorerSettingsModal {

  /** Owner of the taps the service opens from "Show on my graph" intent; the per-handler
    * `key` within it is the tap query's `name`. Re-exported for the explorer's dispatch
    * host, which joins the service's wiretaps with the enabled-tap metadata by this owner.
    */
  val TapQueryOwner: WiretapOwner = WiretapService.TapQueryOwner

  private val ConfigModalCss =
    """.config-manager-wrap .manager-header { display: none; }
      |.config-manager-wrap .manager-list { max-height: 560px; overflow-y: auto; }
      |.explorer-settings-dialog .editor-input,
      |.explorer-settings-dialog .editor-textarea,
      |.explorer-settings-dialog .form-select { background-color: #fff; }
      |.explorer-settings-dialog .embedded-query-editor { background-color: #fff; }""".stripMargin

  def apply(
    openSignal: Signal[Boolean],
    setOpen: Observer[Boolean],
    dataService: DataService,
    editorConfig: EmbeddedEditorConfig,
    menuPrefs: GraphMenuPrefs,
  ): HtmlElement = {
    val sampleQueriesVar: Var[Vector[SampleQuery]] = Var(Vector.empty)
    val tapQueriesVar: Var[Vector[V2TapQuery]] = Var(Vector.empty)
    // Quick queries are modeled in the v1 shape end to end: v1's UiNodeQuickQuery is a strict
    // superset of V2's (it adds `queryLanguage`; the V2 API is Cypher-only), so a v1 Gremlin
    // entry never loses its language on the display/edit round trip. The lossy projection to
    // V2 happens only at the wire boundary, in QuineApiClient.
    val quickQueriesVar: Var[Vector[UiNodeQuickQuery]] = Var(Vector.empty)
    val appearancesVar: Var[Vector[UiNodeAppearance]] = Var(Vector.empty)
    val toastVar: Var[Option[ToastMessage]] = Var(None)

    val sqEditVar: Var[Option[SampleQueryEditorMode]] = Var(None)
    val qqEditVar: Var[Option[QuickQueryEditorMode]] = Var(None)
    val naEditVar: Var[Option[NodeAppearanceEditorMode]] = Var(None)
    val tapQueryEditVar: Var[Option[TapQueryEditorMode]] = Var(None)

    val tapQueriesExpandedVar = Var(true)
    val sqExpandedVar = Var(true)
    val qqExpandedVar = Var(true)
    val naExpandedVar = Var(true)

    // Latest feed values, tracked unconditionally: unlike the old settings page (remounted
    // fresh on each navigation), this modal stays mounted, so a feed refresh that lands
    // mid-edit — dropped below to protect the edit — must still be recoverable when the
    // modal is next opened.
    var latestSampleQueries: Vector[SampleQuery] = Vector.empty
    var latestQuickQueries: Vector[UiNodeQuickQuery] = Vector.empty
    var latestAppearances: Vector[UiNodeAppearance] = Vector.empty
    var latestTapQueries: Vector[V2TapQuery] = Vector.empty
    // Whether the tap-query feed reflects a completed fetch: SaveTapQueries replaces the
    // whole per-graph list, so a save built from a never-loaded list would delete every
    // other definition. Mutations are refused until this is true.
    var tapQueriesLoaded: Boolean = false

    def close(): Unit = setOpen.onNext(false)

    // Whether any catalog editor holds uncommitted input. The light dismissal paths
    // (Escape, overlay click) are ignored while one is open — same protocol as
    // TapModal's config-step guard — so a stray Escape (including one aimed at Monaco's
    // autocomplete widget, which propagates to `document`) doesn't silently discard the
    // edit. The header × still closes.
    def editorOpen(): Boolean =
      sqEditVar.now().isDefined || qqEditVar.now().isDefined || naEditVar.now().isDefined ||
      tapQueryEditVar.now().isDefined

    def saveSampleQueries(sqs: Vector[SampleQuery]): Unit = {
      val previous = sampleQueriesVar.now()
      sampleQueriesVar.set(sqs)
      dataService.queryUiConfigDispatch.onNext(
        QueryUiConfigService.SaveSampleQueries(
          sqs,
          replyTo = Observer[SaveResult] {
            case SaveSucceeded =>
              toastVar.set(Some(ToastMessage("Sample queries saved", ToastVariant.Success)))
            case SaveFailed(message) =>
              sampleQueriesVar.set(previous)
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    def saveQuickQueries(qqs: Vector[UiNodeQuickQuery]): Unit = {
      val previous = quickQueriesVar.now()
      quickQueriesVar.set(qqs)
      dataService.queryUiConfigDispatch.onNext(
        QueryUiConfigService.SaveQuickQueries(
          qqs,
          replyTo = Observer[SaveResult] {
            case SaveSucceeded =>
              toastVar.set(Some(ToastMessage("Quick queries saved", ToastVariant.Success)))
            case SaveFailed(message) =>
              quickQueriesVar.set(previous)
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    // Tap queries are scoped per-graph, replacing only the current graph's list; no need
    // to preserve other graphs' entries client-side. The service targets the graph the
    // user is viewing and refetches the shared feed on success.
    def saveTapQueries(tqs: Vector[V2TapQuery]): Unit = {
      val previous = tapQueriesVar.now()
      tapQueriesVar.set(tqs)
      dataService.tapQueryDispatch.onNext(
        TapQueryService.SaveTapQueries(
          tqs,
          replyTo = Observer[SaveResult] {
            case SaveSucceeded =>
              toastVar.set(Some(ToastMessage("Graph feeds saved (shared with everyone)", ToastVariant.Success)))
            case SaveFailed(message) =>
              tapQueriesVar.set(previous)
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    def saveNodeAppearances(apps: Vector[UiNodeAppearance]): Unit = {
      val previous = appearancesVar.now()
      appearancesVar.set(apps)
      // The service refetches on success, so the shared feed delivers the
      // server-canonicalized list without a bespoke re-GET here.
      dataService.queryUiConfigDispatch.onNext(
        QueryUiConfigService.SaveNodeAppearances(
          apps,
          replyTo = Observer[SaveResult] {
            case SaveSucceeded =>
              toastVar.set(Some(ToastMessage("Node appearances saved", ToastVariant.Success)))
            case SaveFailed(message) =>
              appearancesVar.set(previous)
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    div(
      cls := TapModalStyles.overlay,
      display <-- openSignal.map(if (_) "flex" else "none"),
      // Outside click: only when the click landed on the overlay itself, not bubbled up from
      // the dialog (the dialog's own onClick.stopPropagation below is a second line of defense).
      onClick.filter(e => e.target == e.currentTarget) --> (_ => if (!editorOpen()) close()),
      // Gated on `openSignal`: this binder lives on the always-mounted overlay (only
      // `display` toggles), so without the gate every app-wide Escape would write `false`
      // into the host's open observer even while the modal is closed.
      documentEvents(_.onKeyDown)
        .filter(_.key == "Escape")
        .withCurrentValueOf(openSignal) --> { case (_, open) =>
        if (open && !editorOpen()) close()
      },
      // The shared DataService feeds populate the working Vars while the modal is mounted
      // (which is always — visibility is display-toggled). Namespace switching needs no
      // handling here: the graph-scoped feeds key off the service's current namespace.
      // Each editable catalog is guarded by its editor; refreshes landing mid-edit are
      // dropped so in-progress edits aren't clobbered, then recovered on the next open.
      dataService.sampleQueriesSignal --> { sqs =>
        latestSampleQueries = sqs
        if (sqEditVar.now().isEmpty) sampleQueriesVar.set(sqs)
      },
      dataService.quickQueriesSignal --> { qqs =>
        latestQuickQueries = qqs
        if (qqEditVar.now().isEmpty) quickQueriesVar.set(qqs)
      },
      dataService.nodeAppearancesSignal --> { nas =>
        latestAppearances = nas
        if (naEditVar.now().isEmpty) appearancesVar.set(nas)
      },
      // The tap-query feed is a Pot (graph-scoped, refetched per namespace): the list
      // renders from the last known value, while mutations gate on `tapQueriesLoaded` —
      // see its declaration. A namespace switch resets the Pot to Pending, so a save
      // against the wrong graph's list can't slip through mid-switch.
      dataService.tapQueriesSignal --> { pot =>
        tapQueriesLoaded = pot.toOption.isDefined
        pot.toOption.foreach { tqs =>
          latestTapQueries = tqs
          if (tapQueryEditVar.now().isEmpty) tapQueriesVar.set(tqs)
        }
      },
      // Each open starts fresh: abandon any editor left open when the modal was last
      // dismissed and re-seed the catalogs from the latest feed values (which may have
      // been dropped while that editor was open).
      openSignal.updates.filter(identity) --> { _ =>
        sqEditVar.set(None)
        qqEditVar.set(None)
        naEditVar.set(None)
        tapQueryEditVar.set(None)
        sampleQueriesVar.set(latestSampleQueries)
        quickQueriesVar.set(latestQuickQueries)
        appearancesVar.set(latestAppearances)
        tapQueriesVar.set(latestTapQueries)
      },
      div(
        cls := s"${TapModalStyles.dialog} explorer-settings-dialog",
        onClick.stopPropagation --> (_ => ()), // dialog clicks must not bubble to the overlay's outside-click handler
        htmlTag("style")(ConfigModalCss),
        div(
          cls := TapModalStyles.header,
          span(cls := TapModalStyles.title, "Exploration UI Settings"),
          button(
            tpe := "button",
            cls := TapModalStyles.closeButton,
            title := "Close",
            onClick --> (_ => close()),
            "×",
          ),
        ),
        div(
          cls := TapModalStyles.body,
          tapQueriesCard(
            tapQueriesVar,
            standingQueries = dataService.standingQueriesSignal.map(_.toOption.map(_.toVector).getOrElse(Vector.empty)),
            currentNamespace = dataService.currentNamespaceSignal.map(_.namespaceId),
            editVar = tapQueryEditVar,
            editorConfig = editorConfig,
            save = saveTapQueries,
            canMutate = () =>
              if (tapQueriesLoaded) true
              else {
                toastVar.set(
                  Some(ToastMessage("Graph feeds are still loading, try again in a moment", ToastVariant.Error)),
                )
                false
              },
            notifyError = message => toastVar.set(Some(ToastMessage(message, ToastVariant.Error))),
            expandedVar = tapQueriesExpandedVar,
            menuPrefs = menuPrefs,
          ),
          div(cls := "mt-4"),
          sampleQueriesCard(sampleQueriesVar, sqEditVar, editorConfig, saveSampleQueries, sqExpandedVar),
          div(cls := "mt-4"),
          quickQueriesCard(quickQueriesVar, qqEditVar, editorConfig, saveQuickQueries, qqExpandedVar),
          div(cls := "mt-4"),
          nodeAppearancesCard(appearancesVar, naEditVar, saveNodeAppearances, naExpandedVar),
        ),
        Toast(toastVar),
      ),
    )
  }

  // ---------------------------------------------------------------------------
  // Collapsible card shell: header toggles collapse, button area stops propagation
  // ---------------------------------------------------------------------------

  private def collapsibleCard(
    title: String,
    expandedVar: Var[Boolean],
    headerButton: Signal[HtmlElement],
    body: Signal[HtmlElement],
  ): HtmlElement =
    div(
      cls := "card",
      div(
        cls := "card-header d-flex justify-content-between align-items-center",
        cursor := "pointer",
        onClick --> { _ => expandedVar.update(!_) },
        h5(cls := "mb-0", title),
        div(
          cls := "d-flex align-items-center gap-2",
          div(onClick.stopPropagation --> (_ => ()), child <-- headerButton),
          i(cls <-- expandedVar.signal.map(e => if (e) "cil-chevron-top" else "cil-chevron-bottom")),
        ),
      ),
      div(
        cls("d-none") <-- expandedVar.signal.map(!_),
        div(cls := "card-body config-manager-wrap", child <-- body),
      ),
    )

  // ---------------------------------------------------------------------------
  // Tap Queries
  // ---------------------------------------------------------------------------

  /** The graph-feed catalog (saved V2TapQuery definitions — "tap query" in the code
    * and the API, "graph feed" in the UI) for the current graph, with a
    * list/create/edit/delete surface plus the per-row "Show in graph menu" preference
    * (whether the feed gets a pill in the canvas's bottom-left menu, where its
    * live drawing is toggled — see [[GraphFeedChips]]). The
    * editor picks its source on the Standing Query Inspection pipeline diagram instead
    * of dropdowns. `canMutate` gates every
    * whole-list save behind the feed having loaded (see `tapQueriesLoaded`); `editVar`
    * keys edits by the definition's name at edit-entry time — the list keeps refetching
    * underneath an open edit, so a captured index could point at the wrong entry by
    * save/delete time.
    */
  private def tapQueriesCard(
    dataVar: Var[Vector[V2TapQuery]],
    standingQueries: Signal[Vector[V2StandingQueryInfo]],
    currentNamespace: Signal[String],
    editVar: Var[Option[TapQueryEditorMode]],
    editorConfig: EmbeddedEditorConfig,
    save: Vector[V2TapQuery] => Unit,
    canMutate: () => Boolean,
    notifyError: String => Unit,
    expandedVar: Var[Boolean],
    menuPrefs: GraphMenuPrefs,
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Feed",
          onClick --> { _ => expandedVar.set(true); editVar.set(Some(TapQueryEditorMode.Creating)) },
        )
      case Some(_) =>
        button(
          cls := "btn btn-secondary btn-sm",
          "Back to List",
          onClick --> { _ => editVar.set(None) },
        )
    }

    def confirmAndDelete(name: String): Unit =
      if (canMutate()) {
        val visible = dataVar.now()
        if (
          visible.exists(_.name == name) &&
          window.confirm(
            s"""Delete feed "$name"? Graph feeds are shared, so this will remove it for everyone.""",
          )
        ) save(visible.filterNot(_.name == name))
      }

    // The modal overlays the explorer, so the graph these definitions belong to is
    // whatever the explorer is on — shown here (rather than a picker) because tap
    // queries are the modal's one per-graph catalog.
    def graphLabel(): HtmlElement =
      div(
        cls := "d-flex align-items-center mb-3 gap-2 small text-body-secondary",
        span("Graph:"),
        b(child.text <-- currentNamespace),
      )

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        div(
          graphLabel(),
          p(
            cls := "small",
            color := "#56618f",
            marginBottom := "8px",
            "A feed watches a point in a standing query's pipeline and draws every matching result " +
            "onto the graph, live. Feeds are saved to the server and shared with everyone.",
          ),
          tapQueriesList(
            dataVar.signal.distinct,
            onEdit = name => editVar.set(Some(TapQueryEditorMode.Editing(name))),
            onNew = () => editVar.set(Some(TapQueryEditorMode.Creating)),
            onDelete = confirmAndDelete,
            menuPrefs = menuPrefs,
          ),
        )
      case Some(mode) =>
        val priorName: Option[String] = mode match {
          case TapQueryEditorMode.Editing(name) => Some(name)
          case _ => None
        }
        div(
          graphLabel(),
          TapQueryEditor(
            mode = mode,
            initialValue = priorName.flatMap(name => dataVar.now().find(_.name == name)),
            standingQueries = standingQueries,
            editorConfig = editorConfig,
            onSave = { newTapQuery =>
              if (canMutate()) {
                val visible = dataVar.now()
                // Names are the identity everywhere downstream (delete-by-name, "Enable
                // locally", reconcile's byName map), so a create — or an edit renaming
                // onto *another* entry — must not introduce a duplicate.
                if (visible.exists(d => d.name == newTapQuery.name && !priorName.contains(d.name)))
                  notifyError(s"""A feed named "${newTapQuery.name}" already exists""")
                else {
                  val updated = priorName
                    .flatMap { name =>
                      visible.indexWhere(_.name == name) match {
                        case -1 => None
                        case idx => Some(visible.updated(idx, newTapQuery))
                      }
                    }
                    .getOrElse(visible :+ newTapQuery)
                  save(updated)
                  editVar.set(None)
                }
              }
            },
            onDelete = priorName.map { name => () =>
              confirmAndDelete(name)
              if (!dataVar.now().exists(_.name == name)) editVar.set(None)
            },
            onCancel = () => editVar.set(None),
          ),
        )
    }

    collapsibleCard("Graph Feeds", expandedVar, headerButton, body)
  }

  private def tapQueriesList(
    tapQueries: Signal[Vector[V2TapQuery]],
    onEdit: String => Unit,
    onNew: () => Unit,
    onDelete: String => Unit,
    menuPrefs: GraphMenuPrefs,
  ): HtmlElement = {
    val searchVar = Var("")

    val filtered: Signal[Vector[V2TapQuery]] =
      tapQueries.combineWith(searchVar.signal).map { case (tqs, search) =>
        val needle = search.trim.toLowerCase
        if (needle.isEmpty) tqs
        else
          // Match only the projection's own identity — the name and description the user
          // authored and can see in the row. Searching the underlying Cypher body (or the
          // derived standing-query name) matched on text the user never sees, which made
          // results look random: "al" hitting a projection named "bleh" because "al" happened
          // to appear somewhere in its query text.
          tqs.filter { t =>
            t.name.toLowerCase.contains(needle) ||
            t.description.exists(_.toLowerCase.contains(needle))
          }
      }

    div(
      div(
        cls := Styles.managerHeader,
        span("Graph Feeds"),
        button("New", onClick --> (_ => onNew())),
      ),
      div(
        cls := Styles.managerSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := "Search feeds…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
        ),
      ),
      // An empty catalog and an empty search result are different states: the first
      // deserves a pointer at what a feed is for, the second just says no match.
      child <-- filtered.combineWith(tapQueries).map { case (items, all) =>
        if (items.nonEmpty)
          div(
            cls := Styles.managerList,
            items.map(t => tapQueryListItem(t, onEdit, onDelete, menuPrefs)),
          )
        else if (all.isEmpty)
          div(
            cls := Styles.managerListEmpty,
            div(b("No graph feeds yet.")),
            div("Create one to watch a standing query and draw its results onto the graph as they happen."),
          )
        else div(cls := Styles.managerListEmpty, "No feeds match your search.")
      },
      div(
        cls := Styles.managerFooter,
        child.text <-- tapQueries.map(tqs => s"${tqs.size} ${if (tqs.size == 1) "feed" else "feeds"}"),
      ),
    )
  }

  private def tapQueryListItem(
    t: V2TapQuery,
    onEdit: String => Unit,
    onDelete: String => Unit,
    menuPrefs: GraphMenuPrefs,
  ): HtmlElement = {
    val idStr = s"settings-tap-query-menu-${t.name}"

    // Provenance in the inspection modal's own words — the same point names the pipeline
    // diagram uses, so a row reads as "where it draws from".
    val sourceLabel: String =
      t.outputName.fold(s"${t.standingQueryName} · SQ matches") { o =>
        s"${t.standingQueryName}/$o · ${if (t.preEnrichment) "transformed" else "enriched"}"
      }

    div(
      cls := Styles.managerListItem,
      flexDirection := "column",
      alignItems := "stretch",
      // Top row: name/source + edit/delete actions
      div(
        cls := "d-flex align-items-center w-100",
        onClick --> (_ => onEdit(t.name)),
        cursor := "pointer",
        div(
          cls := Styles.managerListItemMain,
          span(cls := Styles.managerListItemName, t.name),
          span(cls := Styles.managerListItemDetail, sourceLabel),
          t.description.fold(emptyNode: Node)(desc => span(cls := "graph-feed-list-desc", desc)),
        ),
        div(
          cls := Styles.managerListItemActions,
          htmlTag("i")(
            cls := "ion-edit",
            title := "Edit",
            onClick --> { e =>
              e.stopPropagation()
              onEdit(t.name)
            },
          ),
          htmlTag("i")(
            cls := "ion-ios-trash-outline",
            title := "Delete",
            onClick --> { e =>
              e.stopPropagation()
              onDelete(t.name)
            },
          ),
        ),
      ),
      // Show-in-graph-menu row: whether this feed gets a pill in the canvas's
      // bottom-left menu (where its live drawing is toggled — see [[GraphFeedChips]]).
      // Per-browser preference, stored locally; it does not start or stop the
      // feed itself.
      div(
        cls := "form-check form-switch small mt-2 mb-0",
        onClick.stopPropagation --> (_ => ()),
        input(
          cls := "form-check-input",
          typ := "checkbox",
          role := "switch",
          idAttr := idStr,
          checked <-- menuPrefs.hiddenSignal.map(!_.contains(t.name)),
          onChange.mapToChecked --> (isChecked => menuPrefs.setShown(t.name, isChecked)),
        ),
        label(
          cls := "form-check-label",
          forId := idStr,
          title := "Show this feed as a pill in the graph's bottom-left menu, where you can turn its " +
          "live drawing on and off. Only affects this browser; others keep their own setting.",
          "Show in graph menu",
        ),
      ),
    )
  }

  // ---------------------------------------------------------------------------
  // Sample Queries
  // ---------------------------------------------------------------------------

  private def sampleQueriesCard(
    dataVar: Var[Vector[SampleQuery]],
    editVar: Var[Option[SampleQueryEditorMode]],
    editorConfig: EmbeddedEditorConfig,
    save: Vector[SampleQuery] => Unit,
    expandedVar: Var[Boolean],
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Sample Query",
          onClick --> { _ => expandedVar.set(true); editVar.set(Some(SampleQueryEditorMode.Creating)) },
        )
      case Some(_) =>
        button(
          cls := "btn btn-secondary btn-sm",
          "Back to List",
          onClick --> { _ => editVar.set(None) },
        )
    }

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        SampleQueryManager(
          sampleQueries = dataVar.signal.distinct,
          onEdit = idx => editVar.set(Some(SampleQueryEditorMode.Editing(idx))),
          onNew = () => editVar.set(Some(SampleQueryEditorMode.Creating)),
          onSelect = _ => (),
          onDelete = idx => save(dataVar.now().patch(idx, Nil, 1)),
        )
      case Some(mode) =>
        SampleQueryEditor(
          mode = mode,
          initialValue = mode match {
            case SampleQueryEditorMode.Editing(idx) => dataVar.now().lift(idx)
            case _ => None
          },
          editorConfig = editorConfig,
          onSave = { newSq =>
            val current = dataVar.now()
            val updated = mode match {
              case SampleQueryEditorMode.Editing(idx) if idx < current.size => current.updated(idx, newSq)
              case _ => current :+ newSq
            }
            save(updated)
            editVar.set(None)
          },
          onDelete = mode match {
            case SampleQueryEditorMode.Editing(idx) =>
              Some { () =>
                save(dataVar.now().patch(idx, Nil, 1))
                editVar.set(None)
              }
            case _ => None
          },
          onCancel = () => editVar.set(None),
        )
    }

    collapsibleCard("Sample Queries", expandedVar, headerButton, body)
  }

  // ---------------------------------------------------------------------------
  // Quick Queries
  // ---------------------------------------------------------------------------

  private def quickQueriesCard(
    dataVar: Var[Vector[UiNodeQuickQuery]],
    editVar: Var[Option[QuickQueryEditorMode]],
    editorConfig: EmbeddedEditorConfig,
    save: Vector[UiNodeQuickQuery] => Unit,
    expandedVar: Var[Boolean],
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Quick Query",
          onClick --> { _ => expandedVar.set(true); editVar.set(Some(QuickQueryEditorMode.Creating(None))) },
        )
      case Some(_) =>
        button(
          cls := "btn btn-secondary btn-sm",
          "Back to List",
          onClick --> { _ => editVar.set(None) },
        )
    }

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        QuickQueryManager(
          quickQueries = dataVar.signal.distinct,
          onEdit = idx => editVar.set(Some(QuickQueryEditorMode.Editing(idx))),
          onNew = () => editVar.set(Some(QuickQueryEditorMode.Creating(None))),
          onDelete = idx => save(dataVar.now().patch(idx, Nil, 1)),
        )
      case Some(mode) =>
        QuickQueryEditor(
          mode = mode,
          initialValue = mode match {
            case QuickQueryEditorMode.Editing(idx) => dataVar.now().lift(idx)
            case _ => None
          },
          currentNodes = Val(Seq.empty),
          editorConfig = editorConfig,
          onSave = { newQq =>
            val current = dataVar.now()
            val updated = mode match {
              case QuickQueryEditorMode.Editing(idx) if idx < current.size => current.updated(idx, newQq)
              case _ => current :+ newQq
            }
            save(updated)
            editVar.set(None)
          },
          onDelete = mode match {
            case QuickQueryEditorMode.Editing(idx) =>
              Some { () =>
                save(dataVar.now().patch(idx, Nil, 1))
                editVar.set(None)
              }
            case _ => None
          },
          onCancel = () => editVar.set(None),
        )
    }

    collapsibleCard("Quick Queries", expandedVar, headerButton, body)
  }

  // ---------------------------------------------------------------------------
  // Node Appearances
  // ---------------------------------------------------------------------------

  private def nodeAppearancesCard(
    dataVar: Var[Vector[UiNodeAppearance]],
    editVar: Var[Option[NodeAppearanceEditorMode]],
    save: Vector[UiNodeAppearance] => Unit,
    expandedVar: Var[Boolean],
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Appearance",
          onClick --> { _ => expandedVar.set(true); editVar.set(Some(NodeAppearanceEditorMode.Creating(Seq.empty))) },
        )
      case Some(_) =>
        button(
          cls := "btn btn-secondary btn-sm",
          "Back to List",
          onClick --> { _ => editVar.set(None) },
        )
    }

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        NodeAppearanceManager(
          appearances = dataVar.signal.distinct,
          onEdit = idx => editVar.set(Some(NodeAppearanceEditorMode.Editing(idx))),
          onNew = () => editVar.set(Some(NodeAppearanceEditorMode.Creating(Seq.empty))),
          onDelete = idx => save(dataVar.now().patch(idx, Nil, 1)),
        )
      case Some(mode) =>
        NodeAppearanceEditor(
          mode = mode,
          initialValue = mode match {
            case NodeAppearanceEditorMode.Editing(idx) => dataVar.now().lift(idx)
            case _ => None
          },
          currentNodes = Val(Seq.empty),
          onSave = { newApps =>
            val current = dataVar.now()
            val updated = mode match {
              case NodeAppearanceEditorMode.Editing(idx) if idx < current.size =>
                newApps.headOption.fold(current)(a => current.updated(idx, a))
              case _ => current ++ newApps
            }
            save(updated)
            editVar.set(None)
          },
          onDelete = mode match {
            case NodeAppearanceEditorMode.Editing(idx) =>
              Some { () =>
                save(dataVar.now().patch(idx, Nil, 1))
                editVar.set(None)
              }
            case _ => None
          },
          onCancel = () => editVar.set(None),
        )
    }

    collapsibleCard("Node Appearances", expandedVar, headerButton, body)
  }

}
