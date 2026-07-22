package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.dataservice.WiretapService
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2TapQuery

/** Which graph feeds get a pill in the canvas's bottom-left graph menu — a per-browser
  * preference, not a property of the (server-shared) feed itself. Only the
  * *hidden* names are recorded, per graph namespace in localStorage: a feed is in
  * the menu by default, so a definition newly appearing server-side is immediately
  * discoverable as a pill (its drawing still starts off — the wiretap runtime is
  * opt-in, see `OssDataService.WiretapRuntime`). localStorage rather than
  * sessionStorage because this is a user preference — it should survive across tabs
  * and sessions, unlike the per-tab tap opt-ins.
  */
final class GraphMenuPrefs(currentNamespace: Signal[String]) {
  import GraphMenuPrefs.Storage

  private val hiddenVar: Var[Set[String]] = Var(Set.empty)
  private var currentNs: String = NamespaceParameter.defaultNamespaceParameter.namespaceId

  val hiddenSignal: Signal[Set[String]] = hiddenVar.signal

  def setShown(name: String, shown: Boolean): Unit =
    hiddenVar.update(hidden => if (shown) hidden - name else hidden + name)

  /** Drop hidden names for feeds the server no longer has, so a later re-create
    * shows by default (the same pruning the wiretap runtime applies to its opt-ins).
    * Call only with a fully-loaded list — pruning against a failed or pending fetch
    * would wipe the preference.
    */
  def pruneTo(existing: Set[String]): Unit =
    hiddenVar.update(hidden => if (hidden.subsetOf(existing)) hidden else hidden.intersect(existing))

  /** Persistence binders, mounted once by the host: the namespace binder fires
    * synchronously with the current namespace on mount (loading that graph's hidden
    * set), and `.updates` on the save binder keeps the initial empty Var from
    * clobbering storage before that load lands.
    */
  val mods: Seq[Modifier[HtmlElement]] = Seq(
    currentNamespace.distinct --> { ns =>
      currentNs = ns
      hiddenVar.set(Storage.load(ns))
    },
    hiddenVar.signal.updates --> (names => Storage.save(currentNs, names)),
  )
}

object GraphMenuPrefs {

  /** One localStorage entry per graph namespace, holding the hidden feed names as
    * a JSON array — same shape and failure handling as the wiretap runtime's
    * `DisabledTapsStorage`, on localStorage instead of sessionStorage.
    */
  private object Storage {
    private val KeyPrefix = "thatdot.explorer.graphMenuHidden."

    def load(namespace: String): Set[String] =
      try Option(dom.window.localStorage.getItem(KeyPrefix + namespace))
        .filter(_.nonEmpty)
        .flatMap(io.circe.parser.decode[Vector[String]](_).toOption)
        .map(_.toSet)
        .getOrElse(Set.empty)
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"Failed to load graph-menu preferences from localStorage: ${e.getMessage}")
          Set.empty
      }

    def save(namespace: String, names: Set[String]): Unit =
      try if (names.isEmpty) dom.window.localStorage.removeItem(KeyPrefix + namespace)
      else
        dom.window.localStorage.setItem(
          KeyPrefix + namespace,
          Json.fromValues(names.toList.sorted.map(Json.fromString)).noSpaces,
        )
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"Failed to save graph-menu preferences to localStorage: ${e.getMessage}")
      }
  }
}

/** Bottom-left floating pill stack over the canvas: one pill per graph feed whose
  * "Show in graph menu" preference is on, each carrying the live on/off switch for
  * drawing that feed's results (the control that used to live in the Explorer
  * Settings modal). Deliberately minimal by default: at most [[MaxCollapsedChips]]
  * pills, an overflow pill expanding the rest into a scrollable stack, and a search
  * pill only once the menu is genuinely large.
  */
object GraphFeedChips {

  /** Pills shown before the stack folds behind a "+K more" pill. */
  private val MaxCollapsedChips = 4

  /** Menu size past which the expanded stack grows a search pill. */
  private val SearchThreshold = 8

  private val chips = "graph-feed-chips"
  private val list = "graph-feed-chips-list"
  private val chip = "graph-feed-chip"
  private val chipOn = "is-on"
  private val chipDot = "graph-feed-chip-dot"
  private val chipName = "graph-feed-chip-name"
  private val chipSwitch = "graph-feed-chip-switch"
  private val more = "graph-feed-chips-more"
  private val searchWrap = "graph-feed-chips-search"
  private val searchInput = "graph-feed-chips-search-input"

  def apply(
    tapQueries: Signal[Pot[Vector[V2TapQuery]]],
    wiretap: WiretapService,
    prefs: GraphMenuPrefs,
    currentNamespace: Signal[String],
  ): HtmlElement = {
    val expandedVar = Var(false)
    val searchVar = Var("")

    // Renders the last known list through refetches (`PendingStale`/`FailedStale`),
    // nothing before the first load — the same policy as the settings modal's list.
    val all: Signal[Vector[V2TapQuery]] = tapQueries.map(_.toOption.getOrElse(Vector.empty))

    // Server order, unfiltered by search: a switch flip never reshuffles the stack.
    val menuItems: Signal[Vector[V2TapQuery]] =
      all.combineWith(prefs.hiddenSignal).map { case (tqs, hidden) => tqs.filterNot(t => hidden(t.name)) }

    // Same match fields as the settings modal's feed search, minus the query text
    // (a pill search is for finding a name, not grepping Cypher).
    val filtered: Signal[Vector[V2TapQuery]] =
      menuItems.combineWith(searchVar.signal).map { case (tqs, search) =>
        val needle = search.trim.toLowerCase
        if (needle.isEmpty) tqs
        else
          tqs.filter { t =>
            t.name.toLowerCase.contains(needle) ||
            t.standingQueryName.toLowerCase.contains(needle) ||
            t.description.exists(_.toLowerCase.contains(needle))
          }
      }

    val visible: Signal[Vector[V2TapQuery]] =
      expandedVar.signal.combineWith(menuItems, filtered).map {
        case (false, items, _) => items.take(MaxCollapsedChips)
        case (true, _, matches) => matches
      }

    div(
      cls := chips,
      cls("d-none") <-- menuItems.map(_.isEmpty),
      // Expanded, the list scrolls — it must take pointer events itself (see the
      // .is-expanded CSS), or wheel gestures in the gaps between pills and on the
      // scrollbar fall through to the canvas and zoom the graph instead.
      cls("is-expanded") <-- expandedVar.signal,
      // A graph switch collapses back to the minimal default and clears the search;
      // the feeds themselves re-scope via the per-namespace tap-query list.
      currentNamespace.distinct.updates --> { _ =>
        expandedVar.set(false)
        searchVar.set("")
      },
      // Prune hidden names only against a fully-loaded list — never a pending or
      // failed fetch (see GraphMenuPrefs.pruneTo).
      tapQueries --> {
        case Pot.Ready(tqs) => prefs.pruneTo(tqs.iterator.map(_.name).toSet)
        case _ => ()
      },
      child <-- expandedVar.signal.combineWith(menuItems.map(_.size)).map {
        case (true, total) if total > SearchThreshold => searchPill(searchVar)
        case _ => emptyNode
      },
      div(
        cls := list,
        children <-- visible.splitSeq(_.name)(tSig => pill(tSig.key, tSig, wiretap)),
      ),
      child <-- expandedVar.signal.combineWith(menuItems.map(_.size)).map {
        case (false, total) if total > MaxCollapsedChips =>
          div(
            cls := s"$chip $more",
            title := "Show all feeds",
            onClick --> (_ => expandedVar.set(true)),
            s"+${total - MaxCollapsedChips} more ▾",
          )
        case (true, _) =>
          div(
            cls := s"$chip $more",
            title := "Show fewer feeds",
            onClick --> { _ =>
              expandedVar.set(false)
              searchVar.set("")
            },
            "Less ▴",
          )
        case _ => emptyNode
      },
    )
  }

  /** One pill, keyed by feed name (`splitSeq` keeps it mounted across list
    * refreshes). The mini switch is the only control — the pill body is inert — and its
    * dispatch samples the row's signal so an enable always carries the freshest server
    * definition (the wiretap runtime's reconcile keeps it current from there).
    */
  private def pill(name: String, tSig: Signal[V2TapQuery], wiretap: WiretapService): HtmlElement = {
    // Same source of truth as every other tap-query surface: a handler open under
    // TapQueryOwner with this name means "drawing on the graph".
    val enabledSig: Signal[Boolean] =
      wiretap.wiretapsSignal.map(_.get(WiretapService.TapQueryOwner).exists(_.exists(_.key == name)))

    div(
      cls := chip,
      cls(chipOn) <-- enabledSig,
      // Full name leads the tooltip: the pill ellipsizes long names, so hover is where
      // the whole thing is readable.
      title <-- tSig.map { t =>
        val source = t.outputName.fold(s"${t.standingQueryName} · SQ matches") { o =>
          s"${t.standingQueryName}/$o · ${if (t.preEnrichment) "transformed" else "enriched"}"
        }
        t.description.fold(s"${t.name}\n$source")(desc => s"${t.name}\n$source\n$desc")
      },
      span(cls := chipDot),
      span(cls := chipName, name),
      input(
        cls := chipSwitch,
        typ := "checkbox",
        role := "switch",
        title := "Draw this feed's results on your graph. Only affects this browser.",
        checked <-- enabledSig,
        onChange.mapToChecked.compose(_.withCurrentValueOf(tSig)) --> { case (isChecked, t) =>
          if (isChecked) wiretap.wiretapDispatch.onNext(WiretapService.EnableTapQuery(t))
          else wiretap.wiretapDispatch.onNext(WiretapService.DisableTapQuery(t.name))
        },
      ),
    )
  }

  private def searchPill(searchVar: Var[String]): HtmlElement =
    div(
      cls := searchWrap,
      input(
        tpe := "text",
        cls := searchInput,
        placeholder := "Search feeds…",
        controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
      ),
    )
}
