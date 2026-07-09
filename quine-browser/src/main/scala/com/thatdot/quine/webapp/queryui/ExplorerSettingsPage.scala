package com.thatdot.quine.webapp.queryui

import scala.util.{Failure, Success}

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.window
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.v2api.routes.{V2QuerySort, V2QuickQuery, V2UiNodePredicate, V2UiNodeQuickQuery}
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.{Toast, ToastMessage, ToastVariant}
import com.thatdot.quine.webapp.util.{Pot, QuineApiClient}
import com.thatdot.quine.webapp.v2api.V2ApiTypes
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2StandingQueryInfo, V2TapQuery}

object ExplorerSettingsPage {

  private val ConfigPageCss =
    """.config-manager-wrap .manager-header { display: none; }
      |.config-manager-wrap .manager-list { max-height: 560px; overflow-y: auto; }""".stripMargin

  /** @param selectedNamespaceVar when provided, the tap-queries card binds its Graph
    *                             dropdown to this Var — so changing the graph here switches
    *                             the Explorer's graph too, and vice versa. When None, the
    *                             page operates on the default graph only.
    * @param knownNamespacesSignal options for the Graph dropdown. When None (e.g. OSS),
    *                              the dropdown is hidden and the page is locked to the
    *                              default graph.
    * @param enabledTapsVar shared "Enable locally" intent, keyed by namespace. Each tap
    *                       query row's toggle reads and flips this graph's entry; the
    *                       always-mounted [[TapQueryRuntime]] persists it and applies it
    *                       to the wiretap store, so this page just renders and mutates it.
    */
  def apply(
    routes: ClientRoutes,
    useV2Api: Boolean,
    selectedNamespaceVar: Option[Var[Option[String]]] = None,
    knownNamespacesSignal: Option[Signal[Seq[String]]] = None,
    enabledTapsVar: Var[Map[String, Set[String]]],
  ): HtmlElement = {
    val defaultNs = NamespaceParameter.defaultNamespaceParameter.namespaceId
    // Internal mirror of the active graph. When `selectedNamespaceVar` is provided it
    // tracks that Var (so the dropdown and Explorer stay in sync); otherwise it defaults
    // to `Some(default)`. `None` means "not yet known" (Enterprise pre-fetch) — every
    // URL-constructing site short-circuits on that rather than string-interpolating an
    // empty namespace into `/api/v2/graph/…` and 404-ing.
    val currentNamespaceVar: Var[Option[String]] = selectedNamespaceVar.getOrElse(Var(Some(defaultNs)))

    val sampleQueriesVar: Var[Vector[SampleQuery]] = Var(Vector.empty)
    val quickQueriesVar: Var[Vector[V2UiNodeQuickQuery]] = Var(Vector.empty)
    val appearancesVar: Var[Vector[UiNodeAppearance]] = Var(Vector.empty)
    val tapQueriesVar: Var[Vector[V2TapQuery]] = Var(Vector.empty)
    val standingQueriesVar: Var[Vector[V2StandingQueryInfo]] = Var(Vector.empty)
    val toastVar: Var[Option[ToastMessage]] = Var(None)
    val pageState: Var[Pot[Unit]] = Var(Pot.Pending)
    var initialLoadDone: Boolean = false

    val sqEditVar: Var[Option[SampleQueryEditorMode]] = Var(None)
    val qqEditVar: Var[Option[QuickQueryEditorMode]] = Var(None)
    val naEditVar: Var[Option[NodeAppearanceEditorMode]] = Var(None)
    val tapQueryEditVar: Var[Option[TapQueryEditorMode]] = Var(None)

    val tapQueriesExpandedVar = Var(true)
    val sqExpandedVar = Var(true)
    val qqExpandedVar = Var(true)
    val naExpandedVar = Var(true)

    val autoRefreshStream: EventStream[Unit] = EventStream
      .periodic(intervalMs = 5000)
      .mapTo(())

    def loadStandingQueries(namespace: String): Unit =
      QuineApiClient
        .fetchV2[V2ApiTypes.V2Page[V2StandingQueryInfo]](
          s"api/v2/graph/$namespace/standingQueries",
          routes,
        )
        .onComplete {
          case Success(page) => standingQueriesVar.set(page.items.toVector)
          case Failure(_) => standingQueriesVar.set(Vector.empty)
        }

    // Fetches this graph's tap-query list for display only. Enabling/restoring the taps
    // themselves is owned by the always-mounted TapQueryRuntime.
    def loadTapQueries(namespace: String): Unit =
      QuineApiClient
        .fetchV2[Vector[V2TapQuery]](s"api/v2/graph/$namespace/queryUi/tapQueries", routes)
        .onComplete {
          case Success(tqs) => tapQueriesVar.set(tqs)
          case Failure(err) =>
            dom.console.warn(s"[Explorer] Failed to load tap queries for namespace '$namespace': ${err.getMessage}")
        }

    def loadData(): Unit = {
      val sqFut =
        if (useV2Api) routes.queryUiSampleQueriesV2(()).future
        else routes.queryUiSampleQueries(()).future
      val qqFut =
        if (useV2Api) routes.queryUiQuickQueriesV2(()).future
        else routes.queryUiQuickQueries(()).future.map(_.map(v1ToV2QuickQuery))
      val naFut =
        if (useV2Api) routes.queryUiAppearanceV2(()).future
        else routes.queryUiAppearance(()).future

      sqFut.zip(qqFut).zip(naFut).onComplete {
        case Success(((sqs, qqs), nas)) =>
          sampleQueriesVar.set(sqs)
          quickQueriesVar.set(qqs)
          appearancesVar.set(nas)
          if (!initialLoadDone) {
            pageState.set(Pot.Ready(()))
            initialLoadDone = true
          }
        case Failure(err) =>
          if (!initialLoadDone) pageState.set(Pot.Failed(err.getMessage))
      }
    }

    // Stale-response guard: each refresh tick increments the generation before
    // firing requests. When a response arrives it checks whether its generation
    // is still current; if a newer tick has fired in the meantime the response
    // is discarded, preventing a slow tick-N response from overwriting a faster
    // tick-N+1 result. Edit-var checks are also deferred to response time so
    // that an editor opened while a request is in flight is not clobbered.
    var refreshGeneration: Long = 0L

    def refreshData(): Unit = if (initialLoadDone) {
      refreshGeneration += 1
      val myGeneration = refreshGeneration

      val sqFut =
        if (useV2Api) routes.queryUiSampleQueriesV2(()).future
        else routes.queryUiSampleQueries(()).future
      sqFut.foreach { sqs =>
        if (myGeneration == refreshGeneration && sqEditVar.now().isEmpty) sampleQueriesVar.set(sqs)
      }

      val qqFut =
        if (useV2Api) routes.queryUiQuickQueriesV2(()).future
        else routes.queryUiQuickQueries(()).future.map(_.map(v1ToV2QuickQuery))
      qqFut.foreach { qqs =>
        if (myGeneration == refreshGeneration && qqEditVar.now().isEmpty) quickQueriesVar.set(qqs)
      }

      val naFut =
        if (useV2Api) routes.queryUiAppearanceV2(()).future
        else routes.queryUiAppearance(()).future
      naFut.foreach { nas =>
        if (myGeneration == refreshGeneration && naEditVar.now().isEmpty) appearancesVar.set(nas)
      }

      // Tap queries and the SQ catalog are both per-namespace; refresh them against the
      // currently-selected graph so new entries surface without a namespace switch.
      // Skip when the namespace hasn't been resolved yet — `loadStandingQueries` etc.
      // will fire once it lands.
      currentNamespaceVar.now().foreach { ns =>
        QuineApiClient
          .fetchV2[Vector[V2TapQuery]](s"api/v2/graph/$ns/queryUi/tapQueries", routes)
          .foreach { tqs =>
            if (myGeneration == refreshGeneration && tapQueryEditVar.now().isEmpty)
              tapQueriesVar.set(tqs)
          }
        QuineApiClient
          .fetchV2[V2ApiTypes.V2Page[V2StandingQueryInfo]](
            s"api/v2/graph/$ns/standingQueries",
            routes,
          )
          .foreach { page =>
            if (myGeneration == refreshGeneration) standingQueriesVar.set(page.items.toVector)
          }
      }
    }

    def saveSampleQueries(sqs: Vector[SampleQuery]): Unit = {
      val previous = sampleQueriesVar.now()
      sampleQueriesVar.set(sqs)
      val fut =
        if (useV2Api) routes.updateQueryUiSampleQueriesV2(sqs).future
        else routes.updateQueryUiSampleQueries(sqs).future
      fut.onComplete {
        case Success(_) =>
          toastVar.set(Some(ToastMessage("Sample queries saved", ToastVariant.Success)))
        case Failure(err) =>
          sampleQueriesVar.set(previous)
          toastVar.set(Some(ToastMessage(s"Save failed: ${err.getMessage}", ToastVariant.Error)))
      }
    }

    def saveQuickQueries(qqs: Vector[V2UiNodeQuickQuery]): Unit = {
      val previous = quickQueriesVar.now()
      quickQueriesVar.set(qqs)
      val fut =
        if (useV2Api) routes.updateQueryUiQuickQueriesV2(qqs).future
        else routes.updateQueryUiQuickQueries(qqs.map(v2ToV1QuickQuery)).future
      fut.onComplete {
        case Success(_) =>
          toastVar.set(Some(ToastMessage("Quick queries saved", ToastVariant.Success)))
        case Failure(err) =>
          quickQueriesVar.set(previous)
          toastVar.set(Some(ToastMessage(s"Save failed: ${err.getMessage}", ToastVariant.Error)))
      }
    }

    def saveNodeAppearances(apps: Vector[UiNodeAppearance]): Unit = {
      val previous = appearancesVar.now()
      appearancesVar.set(apps)
      val fut =
        if (useV2Api) routes.updateQueryUiAppearanceV2(apps).future
        else routes.updateQueryUiAppearance(apps).future
      fut.onComplete {
        case Success(_) =>
          toastVar.set(Some(ToastMessage("Node appearances saved", ToastVariant.Success)))
          val refreshFut =
            if (useV2Api) routes.queryUiAppearanceV2(()).future
            else routes.queryUiAppearance(()).future
          refreshFut.foreach(rendered => appearancesVar.set(rendered))
        case Failure(err) =>
          appearancesVar.set(previous)
          toastVar.set(Some(ToastMessage(s"Save failed: ${err.getMessage}", ToastVariant.Error)))
      }
    }

    // Tap queries are scoped per-graph by the URL, so the PUT here replaces only this
    // graph's list — no need to preserve other graphs' entries client-side.
    def saveTapQueriesForNamespace(ns: String, tqs: Vector[V2TapQuery]): Unit = {
      val previous = tapQueriesVar.now()
      tapQueriesVar.set(tqs)
      QuineApiClient
        .putV2(s"api/v2/graph/$ns/queryUi/tapQueries", tqs, routes)
        .onComplete {
          case Success(_) =>
            toastVar.set(Some(ToastMessage("Tap queries saved — shared with everyone", ToastVariant.Success)))
            loadTapQueries(ns)
          case Failure(err) =>
            tapQueriesVar.set(previous)
            toastVar.set(Some(ToastMessage(s"Save failed: ${err.getMessage}", ToastVariant.Error)))
        }
    }

    div(
      cls := "container-fluid px-3",
      htmlTag("style")(ConfigPageCss),
      onMountCallback(_ => loadData()),
      autoRefreshStream --> Observer[Unit](_ => refreshData()),
      // Reload SQ and tap-query lists whenever the active namespace changes — also fires
      // on first mount so the lists are populated before the user opens the editor.
      currentNamespaceVar.signal --> {
        case Some(ns) =>
          loadStandingQueries(ns)
          loadTapQueries(ns)
        case None => ()
      },
      div(
        cls := "d-flex align-items-center",
        height := "var(--cui-sidebar-header-height, 4rem)",
        h5(cls := "mb-0", "Explorer Settings"),
      ),
      child <-- pageState.signal.map {
        case Pot.Empty | Pot.Pending =>
          div(
            cls := "text-center py-5",
            div(cls := "spinner-border text-primary", role := "status"),
            p(cls := "mt-3 text-body-secondary", "Loading settings..."),
          )
        case Pot.Failed(msg) =>
          div(cls := "alert alert-danger", msg)
        case Pot.Ready(_) =>
          div(
            tapQueriesCard(
              tapQueriesVar,
              standingQueriesVar,
              currentNamespaceVar,
              externallyBoundNamespace = selectedNamespaceVar.isDefined,
              knownNamespacesSignal,
              tapQueryEditVar,
              saveTapQueriesForNamespace,
              tapQueriesExpandedVar,
              enabledTapsVar,
            ),
            div(cls := "mt-4"),
            sampleQueriesCard(sampleQueriesVar, sqEditVar, saveSampleQueries, sqExpandedVar),
            div(cls := "mt-4"),
            quickQueriesCard(quickQueriesVar, qqEditVar, saveQuickQueries, qqExpandedVar),
            div(cls := "mt-4"),
            nodeAppearancesCard(appearancesVar, naEditVar, saveNodeAppearances, naExpandedVar),
          )
        case _ => emptyNode
      },
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
    standingQueriesVar: Var[Vector[V2StandingQueryInfo]],
    selectedNamespaceVar: Var[Option[String]],
    externallyBoundNamespace: Boolean,
    knownNamespacesSignal: Option[Signal[Seq[String]]],
    editVar: Var[Option[TapQueryEditorMode]],
    save: (String, Vector[V2TapQuery]) => Unit,
    expandedVar: Var[Boolean],
    enabledTapsVar: Var[Map[String, Set[String]]],
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
      selectedNamespaceVar.now().foreach { ns =>
        if (
          window.confirm(
            s"""Delete tap query "$name"? Tap queries are shared, so this will remove it for everyone.""",
          )
        ) save(ns, visible.patch(idx, Nil, 1))
      }
    }

    // Graph (namespace) dropdown — bound to the same Var as the Explorer's GraphSelector,
    // so switching here changes the Explorer's graph too. Only shown when the host
    // actually has a graph picker to bind to (Enterprise multi-graph mode).
    def graphPicker(): HtmlElement = (externallyBoundNamespace, knownNamespacesSignal) match {
      case (true, Some(knownSignal)) =>
        div(
          cls := "d-flex align-items-center mb-3 gap-2",
          label(cls := "form-label mb-0 small text-body-secondary", "Graph"),
          select(
            cls := "form-select form-select-sm",
            width := "auto",
            onChange.mapToValue --> selectedNamespaceVar.writer.contramap[String](Some(_)),
            children <-- knownSignal.combineWith(selectedNamespaceVar.signal).map { case (known, current) =>
              val currentStr = current.getOrElse("")
              val options =
                if (current.forall(known.contains) || known.isEmpty) known
                else currentStr +: known
              options.toList.map(ns => option(value := ns, selected := current.contains(ns), ns))
            },
          ),
        )
      case _ => div(display := "none")
    }

    val body: Signal[HtmlElement] = editVar.signal.map {
      case None =>
        div(
          graphPicker(),
          tapQueriesList(
            dataVar.signal.distinct,
            selectedNamespaceVar = selectedNamespaceVar,
            onEdit = idx => editVar.set(Some(TapQueryEditorMode.Editing(idx))),
            onNew = () => editVar.set(Some(TapQueryEditorMode.Creating)),
            onDelete = confirmAndDelete,
            enabledTapsVar = enabledTapsVar,
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
            standingQueries = standingQueriesVar.signal,
            onSave = { newTapQuery =>
              selectedNamespaceVar.now().foreach { ns =>
                val visible = dataVar.now()
                val updated = mode match {
                  case TapQueryEditorMode.Editing(idx) if idx < visible.size => visible.updated(idx, newTapQuery)
                  case _ => visible :+ newTapQuery
                }
                save(ns, updated)
                editVar.set(None)
              }
            },
            onDelete = mode match {
              case TapQueryEditorMode.Editing(idx) =>
                Some { () =>
                  selectedNamespaceVar.now().foreach { ns =>
                    val visible = dataVar.now()
                    val name = visible.lift(idx).map(_.name).getOrElse("this tap query")
                    if (
                      window.confirm(
                        s"""Delete tap query "$name"? Tap queries are shared, so this will remove it for everyone.""",
                      )
                    ) {
                      save(ns, visible.patch(idx, Nil, 1))
                      editVar.set(None)
                    }
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
    selectedNamespaceVar: Var[Option[String]],
    onEdit: Int => Unit,
    onNew: () => Unit,
    onDelete: Int => Unit,
    enabledTapsVar: Var[Map[String, Set[String]]],
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
              tapQueryListItem(
                t,
                idx,
                selectedNamespaceVar,
                onEdit,
                onDelete,
                enabledTapsVar,
              )
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
    selectedNamespaceVar: Var[Option[String]],
    onEdit: Int => Unit,
    onDelete: Int => Unit,
    enabledTapsVar: Var[Map[String, Set[String]]],
  ): HtmlElement = {
    val idStr = s"settings-tap-query-enable-${t.name}"
    // The toggle reflects intent (`enabledTapsVar`), not live store handlers, so it
    // stays on across graph switches even though the store's handlers are torn down
    // and reopened underneath it by TapQueryRuntime.
    val enabledSignal: Signal[Boolean] =
      enabledTapsVar.signal.combineWith(selectedNamespaceVar.signal).map {
        case (enabled, Some(ns)) => enabled.getOrElse(ns, Set.empty).contains(t.name)
        case (_, None) => false
      }

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
      // Enable Locally row
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
            selectedNamespaceVar.now().foreach { ns =>
              // Record intent only. TapQueryRuntime observes this, opens/closes the
              // wiretap handler, and persists to sessionStorage.
              enabledTapsVar.update { m =>
                val cur = m.getOrElse(ns, Set.empty)
                m.updated(ns, if (isChecked) cur + t.name else cur - t.name)
              }
            }
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

  // ---------------------------------------------------------------------------
  // V1 / V2 quick query conversion
  // ---------------------------------------------------------------------------

  def v1ToV2QuickQuery(v1: UiNodeQuickQuery): V2UiNodeQuickQuery =
    V2UiNodeQuickQuery(
      predicate = V2UiNodePredicate(v1.predicate.propertyKeys, v1.predicate.knownValues, v1.predicate.dbLabel),
      quickQuery = V2QuickQuery(
        name = v1.quickQuery.name,
        querySuffix = v1.quickQuery.querySuffix,
        sort = v1.quickQuery.sort match {
          case QuerySort.Node => V2QuerySort.Node
          case QuerySort.Text => V2QuerySort.Text
        },
        edgeLabel = v1.quickQuery.edgeLabel,
      ),
    )

  def v2ToV1QuickQuery(v2: V2UiNodeQuickQuery): UiNodeQuickQuery =
    UiNodeQuickQuery(
      predicate = UiNodePredicate(v2.predicate.propertyKeys, v2.predicate.knownValues, v2.predicate.dbLabel),
      quickQuery = QuickQuery(
        name = v2.quickQuery.name,
        querySuffix = v2.quickQuery.querySuffix,
        queryLanguage = QueryLanguage.Cypher,
        sort = v2.quickQuery.sort match {
          case V2QuerySort.Node => QuerySort.Node
          case V2QuerySort.Text => QuerySort.Text
        },
        edgeLabel = v2.quickQuery.edgeLabel,
      ),
    )
}
