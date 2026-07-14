package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import org.scalajs.dom.window

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.v2api.routes.V2UiNodeQuickQuery
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.{Toast, ToastMessage, ToastVariant}
import com.thatdot.quine.webapp.dataservice.{
  DataService,
  NamespaceService,
  QueryUiConfigService,
  SaveFailed,
  SaveResult,
  SaveSucceeded,
  TapQueryService,
  WiretapOwner,
  WiretapService,
}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2TapQuery}

object ExplorerSettingsPage {

  /** Owner of the taps the service opens from "Enable locally" intent; the per-handler
    * `key` within it is the tap query's `name`. Re-exported for the explorer's dispatch
    * host, which joins the service's wiretaps with the enabled-tap metadata by this owner.
    */
  val TapQueryOwner: WiretapOwner = WiretapService.TapQueryOwner

  private val ConfigPageCss =
    """.config-manager-wrap .manager-header { display: none; }
      |.config-manager-wrap .manager-list { max-height: 560px; overflow-y: auto; }""".stripMargin

  /** @param showGraphPicker when true (Enterprise multi-graph mode), the tap-queries card
    *                        shows a Graph dropdown driven by the service's namespace list;
    *                        selecting there dispatches [[NamespaceService.SetNamespace]], so the
    *                        Explorer's graph switches too, and vice versa. When false the
    *                        page follows the service's (fixed) current graph.
    *
    * Each tap query row gets an "Enable locally" toggle that registers the tap query with
    * the service and opens/closes its wiretap; matches keep firing while the user is on
    * other pages because the explorer's dispatch host reads the same service signals.
    */
  def apply(
    dataService: DataService,
    showGraphPicker: Boolean = false,
  ): HtmlElement = {
    val sampleQueriesVar: Var[Vector[SampleQuery]] = Var(Vector.empty)
    val quickQueriesVar: Var[Vector[V2UiNodeQuickQuery]] = Var(Vector.empty)
    val appearancesVar: Var[Vector[UiNodeAppearance]] = Var(Vector.empty)
    val tapQueriesVar: Var[Vector[V2TapQuery]] = Var(Vector.empty)
    val toastVar: Var[Option[ToastMessage]] = Var(None)

    val sqEditVar: Var[Option[SampleQueryEditorMode]] = Var(None)
    val qqEditVar: Var[Option[QuickQueryEditorMode]] = Var(None)
    val naEditVar: Var[Option[NodeAppearanceEditorMode]] = Var(None)
    val tapQueryEditVar: Var[Option[TapQueryEditorMode]] = Var(None)

    val tapQueriesExpandedVar = Var(true)
    val sqExpandedVar = Var(true)
    val qqExpandedVar = Var(true)
    val naExpandedVar = Var(true)

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

    def saveQuickQueries(qqs: Vector[V2UiNodeQuickQuery]): Unit = {
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

    // Tap queries are scoped per-graph, replacing only the current graph's list — no need
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
              toastVar.set(Some(ToastMessage("Tap queries saved — shared with everyone", ToastVariant.Success)))
            case SaveFailed(message) =>
              tapQueriesVar.set(previous)
              toastVar.set(Some(ToastMessage(s"Save failed: $message", ToastVariant.Error)))
          },
        ),
      )
    }

    div(
      cls := "container-fluid px-3",
      htmlTag("style")(ConfigPageCss),
      // The shared DataService feeds populate the page's working Vars while this page is
      // mounted. Namespace switching needs no handling here: the graph-scoped feeds
      // (tapQueries, standingQueries) key off the service's current namespace, which tracks
      // the graph picker. Each editable catalog is guarded by its editor — refreshes landing
      // mid-edit are dropped so in-progress edits aren't clobbered (a change swallowed this
      // way surfaces on the feed's next data change).
      dataService.sampleQueriesSignal --> { sqs =>
        if (sqEditVar.now().isEmpty) sampleQueriesVar.set(sqs)
      },
      dataService.quickQueriesSignal --> { qqs =>
        if (qqEditVar.now().isEmpty) quickQueriesVar.set(qqs)
      },
      dataService.nodeAppearancesSignal --> { nas =>
        if (naEditVar.now().isEmpty) appearancesVar.set(nas)
      },
      dataService.tapQueriesSignal --> { tqs =>
        if (tapQueryEditVar.now().isEmpty) tapQueriesVar.set(tqs)
      },
      div(
        cls := "d-flex align-items-center",
        height := "var(--cui-sidebar-header-height, 4rem)",
        h2(cls := "h2 mb-0 px-3", "Explorer Settings"),
      ),
      div(
        tapQueriesCard(
          tapQueriesVar,
          dataService.standingQueriesSignal.map(_.toOption.map(_.toVector).getOrElse(Vector.empty)),
          currentNamespace = dataService.currentNamespaceSignal.map(_.namespaceId),
          showGraphPicker = showGraphPicker,
          knownNamespaces = dataService.namespacesSignal.map(_.map(_.namespaceId)),
          onSelectNamespace = Observer[String] { name =>
            NamespaceParameter(name).foreach(ns =>
              dataService.namespaceDispatch.onNext(NamespaceService.SetNamespace(ns)),
            )
          },
          tapQueryEditVar,
          saveTapQueries,
          tapQueriesExpandedVar,
          dataService,
        ),
        div(cls := "mt-4"),
        sampleQueriesCard(sampleQueriesVar, sqEditVar, saveSampleQueries, sqExpandedVar),
        div(cls := "mt-4"),
        quickQueriesCard(quickQueriesVar, qqEditVar, saveQuickQueries, qqExpandedVar),
        div(cls := "mt-4"),
        nodeAppearancesCard(appearancesVar, naEditVar, saveNodeAppearances, naExpandedVar),
      ),
      Toast(toastVar),
    )
  }

  // ---------------------------------------------------------------------------
  // Collapsible card shell — header toggles collapse, button area stops propagation
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

  private def tapQueriesCard(
    dataVar: Var[Vector[V2TapQuery]],
    standingQueries: Signal[Vector[V2StandingQueryInfo]],
    currentNamespace: Signal[String],
    showGraphPicker: Boolean,
    knownNamespaces: Signal[Seq[String]],
    onSelectNamespace: Observer[String],
    editVar: Var[Option[TapQueryEditorMode]],
    save: Vector[V2TapQuery] => Unit,
    expandedVar: Var[Boolean],
    wiretap: WiretapService,
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Tap Query",
          onClick --> { _ => editVar.set(Some(TapQueryEditorMode.Creating)) },
        )
      case Some(_) =>
        button(
          cls := "btn btn-secondary btn-sm",
          "Back to List",
          onClick --> { _ => editVar.set(None) },
        )
    }

    def confirmAndDelete(idx: Int): Unit = {
      val visible = dataVar.now()
      val name = visible.lift(idx).map(_.name).getOrElse("this tap query")
      if (
        window.confirm(
          s"""Delete tap query "$name"? Tap queries are shared, so this will remove it for everyone.""",
        )
      ) save(visible.patch(idx, Nil, 1))
    }

    // Graph (namespace) dropdown — selections dispatch to the DataService, so switching
    // here changes the Explorer's graph too. Only shown in Enterprise multi-graph mode.
    def graphPicker(): HtmlElement =
      if (!showGraphPicker) div(display := "none")
      else
        div(
          cls := "d-flex align-items-center mb-3 gap-2",
          label(cls := "form-label mb-0 small text-body-secondary", "Graph"),
          select(
            cls := "form-select form-select-sm",
            width := "auto",
            onChange.mapToValue --> onSelectNamespace,
            children <-- knownNamespaces.combineWith(currentNamespace).map { case (known, current) =>
              val options =
                if (known.contains(current) || known.isEmpty) known
                else current +: known
              options.toList.map(ns => option(value := ns, selected := ns == current, ns))
            },
          ),
        )

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        div(
          graphPicker(),
          tapQueriesList(
            dataVar.signal.distinct,
            onEdit = idx => editVar.set(Some(TapQueryEditorMode.Editing(idx))),
            onNew = () => editVar.set(Some(TapQueryEditorMode.Creating)),
            onDelete = confirmAndDelete,
            wiretap = wiretap,
          ),
        )
      case Some(mode) =>
        div(
          graphPicker(),
          TapQueryEditor(
            mode = mode,
            initialValue = mode match {
              case TapQueryEditorMode.Editing(idx) => dataVar.now().lift(idx)
              case _ => None
            },
            standingQueries = standingQueries,
            onSave = { newTapQuery =>
              val visible = dataVar.now()
              val updated = mode match {
                case TapQueryEditorMode.Editing(idx) if idx < visible.size => visible.updated(idx, newTapQuery)
                case _ => visible :+ newTapQuery
              }
              save(updated)
              editVar.set(None)
            },
            onDelete = mode match {
              case TapQueryEditorMode.Editing(idx) =>
                Some { () =>
                  val visible = dataVar.now()
                  val name = visible.lift(idx).map(_.name).getOrElse("this tap query")
                  if (
                    window.confirm(
                      s"""Delete tap query "$name"? Tap queries are shared, so this will remove it for everyone.""",
                    )
                  ) {
                    save(visible.patch(idx, Nil, 1))
                    editVar.set(None)
                  }
                }
              case _ => None
            },
            onCancel = () => editVar.set(None),
          ),
        )
    }

    collapsibleCard("Tap Queries", expandedVar, headerButton, body)
  }

  private def tapQueriesList(
    tapQueries: Signal[Vector[V2TapQuery]],
    onEdit: Int => Unit,
    onNew: () => Unit,
    onDelete: Int => Unit,
    wiretap: WiretapService,
  ): HtmlElement = {
    val searchVar = Var("")

    val filtered: Signal[Vector[(V2TapQuery, Int)]] =
      tapQueries.combineWith(searchVar.signal).map { case (tqs, search) =>
        val needle = search.trim.toLowerCase
        val indexed = tqs.zipWithIndex
        if (needle.isEmpty) indexed
        else
          indexed.filter { case (t, _) =>
            t.name.toLowerCase.contains(needle) ||
              t.standingQueryName.toLowerCase.contains(needle) ||
              t.query.toLowerCase.contains(needle)
          }
      }

    div(
      div(
        cls := Styles.managerHeader,
        span("Tap Queries"),
        button("New", onClick --> (_ => onNew())),
      ),
      div(
        cls := Styles.managerSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := "Search tap queries…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
        ),
      ),
      child <-- filtered.map { items =>
        if (items.isEmpty)
          div(cls := Styles.managerListEmpty, "No tap queries match your search.")
        else
          div(
            cls := Styles.managerList,
            items.map { case (t, idx) =>
              tapQueryListItem(t, idx, onEdit, onDelete, wiretap)
            },
          )
      },
      div(
        cls := Styles.managerFooter,
        child.text <-- tapQueries.map(tqs => s"${tqs.size} ${if (tqs.size == 1) "tap query" else "tap queries"}"),
      ),
    )
  }

  private def tapQueryListItem(
    t: V2TapQuery,
    idx: Int,
    onEdit: Int => Unit,
    onDelete: Int => Unit,
    wiretap: WiretapService,
  ): HtmlElement = {
    val idStr = s"settings-tap-query-enable-${t.name}"
    // When this row's tap query is locally enabled, a handler with key=t.name is present
    // under TapQueryOwner in the service's wiretaps. Toggling here observes the same
    // service as anywhere else, so independent UIs stay in sync.
    val enabledSignal: Signal[Boolean] =
      wiretap.wiretapsSignal.map(_.get(TapQueryOwner).exists(_.exists(_.key == t.name)))

    div(
      cls := Styles.managerListItem,
      flexDirection := "column",
      alignItems := "stretch",
      // Top row — name/detail + edit/delete actions
      div(
        cls := "d-flex align-items-center w-100",
        onClick --> (_ => onEdit(idx)),
        cursor := "pointer",
        div(
          cls := Styles.managerListItemMain,
          span(cls := Styles.managerListItemName, t.name),
          span(
            cls := Styles.managerListItemDetail,
            s"${t.standingQueryName}${t.outputName.fold("")(o => s" / $o")}",
          ),
        ),
        div(
          cls := Styles.managerListItemActions,
          htmlTag("i")(
            cls := "ion-edit",
            title := "Edit",
            onClick --> { e =>
              e.stopPropagation()
              onEdit(idx)
            },
          ),
          htmlTag("i")(
            cls := "ion-ios-trash-outline",
            title := "Delete",
            onClick --> { e =>
              e.stopPropagation()
              onDelete(idx)
            },
          ),
        ),
      ),
      // Enable Locally row — record the enable/disable intent with the service. It opens
      // or closes the wiretap, persists the intent per graph for this tab, and restores
      // it on reload, so the toggle itself never manages handlers.
      div(
        cls := "form-check form-switch small mt-2 mb-0",
        onClick.stopPropagation --> (_ => ()),
        input(
          cls := "form-check-input",
          typ := "checkbox",
          role := "switch",
          idAttr := idStr,
          checked <-- enabledSignal,
          onChange.mapToChecked --> { isChecked =>
            if (isChecked) wiretap.wiretapDispatch.onNext(WiretapService.EnableTapQuery(t))
            else wiretap.wiretapDispatch.onNext(WiretapService.DisableTapQuery(t.name))
          },
        ),
        label(
          cls := "form-check-label",
          forId := idStr,
          "Enable locally",
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
    save: Vector[SampleQuery] => Unit,
    expandedVar: Var[Boolean],
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Sample Query",
          onClick --> { _ => editVar.set(Some(SampleQueryEditorMode.Creating)) },
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
    dataVar: Var[Vector[V2UiNodeQuickQuery]],
    editVar: Var[Option[QuickQueryEditorMode]],
    save: Vector[V2UiNodeQuickQuery] => Unit,
    expandedVar: Var[Boolean],
  ): HtmlElement = {
    val headerButton: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        button(
          cls := "btn btn-primary btn-sm",
          i(cls := "cil-plus me-1"),
          "New Quick Query",
          onClick --> { _ => editVar.set(Some(QuickQueryEditorMode.Creating(None))) },
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
          onClick --> { _ => editVar.set(Some(NodeAppearanceEditorMode.Creating(Seq.empty))) },
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
