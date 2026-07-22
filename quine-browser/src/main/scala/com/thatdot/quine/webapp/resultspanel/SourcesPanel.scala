package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.util.Pot

/** The switcher, dropped into the results viewer area beneath the header (the chip / pager is its
  * disclosure; the top bar is its search header). One chronological list: an **Add a tap** entry,
  * a **Taps · Live** section, and a **Query results** section with an All / Kept filter and a
  * per-row bookmark (retention). A GC footer explains that unbookmarked runs clear automatically.
  * The [[AddTapChooser]] replaces the list while active. Reads state as `Signal`s and emits
  * [[ResultsCommand]]s; picker-local view state (All/Kept) is held here and resets when the picker
  * unmounts.
  */
object SourcesPanel {

  def apply(
    taps: TapsReads,
    session: SessionReads,
    catalog: Signal[Pot[Vector[TapCatalogEntry]]],
    addingTap: Signal[Boolean],
    search: Signal[String],
    sd: Observer[ResultsCommand],
  ): HtmlElement = {
    val keptOnly = Var(false) // the Query results All / Kept filter (picker-local)
    // Already-watching set: the panel's own target keys for the live taps, so a chosen tap
    // point is "open" when `TapTarget(sq, point).key` is among them (never the producer's id).
    val watching: Signal[Set[String]] = taps.entries.map(_.flatMap(_.target.map(_.key)).toSet)
    // The history entry currently on display — only when no tap is selected (a selected tap shows
    // instead). Lets the picker mark "you're viewing this" on the matching row, tying selection to
    // what the surface shows.
    val viewingIdx: Signal[Option[Int]] =
      taps.selectedId.combineWith(session.currentIdx).map { case (sel, idx) => if (sel.isEmpty) Some(idx) else None }

    div(
      cls := Styles.sourcesPanel,
      documentEvents(_.onKeyDown).filter(_.key == "Escape") --> (_ => sd.onNext(ResultsCommand.CloseHistory)),
      // The add-tap chooser vs. the switcher list is store state, so the header "+ Tap" can open
      // straight to the chooser.
      child <-- addingTap.map { isAdding =>
        if (isAdding) AddTapChooser(catalog, watching, sd)
        else switcher(taps, session.entries, viewingIdx, search, keptOnly, sd)
      },
    )
  }

  // --- Switcher list (add-a-tap · taps · query results · GC) ------------------

  private def switcher(
    taps: TapsReads,
    entries: Signal[Vector[HistoryEntry]],
    viewingIdx: Signal[Option[Int]],
    search: Signal[String],
    keptOnly: Var[Boolean],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.sourcesSwitcher,
      addTapEntry(sd),
      tapsSection(taps, search, sd),
      queryResultsSection(entries, viewingIdx, search, keptOnly, sd),
      gcFooter(),
    )

  /** The teal "Add a tap" entry — the one path into the add-tap chooser now the band's button is gone. */
  private def addTapEntry(sd: Observer[ResultsCommand]): HtmlElement =
    div(
      cls := Styles.switcherAddTap,
      onClick --> (_ => sd.onNext(ResultsCommand.ToggleTapChooser)),
      span(cls := Styles.switcherAddTapIcon, ResultsIcons.plus),
      span(cls := Styles.switcherAddTapLabel, "Add a tap"),
      span(cls := Styles.switcherAddTapChevron, "›"),
    )

  /** Live taps, filtered by the bar's search. Hidden entirely when nothing matches. */
  private def tapsSection(taps: TapsReads, search: Signal[String], sd: Observer[ResultsCommand]): HtmlElement =
    div(
      child <-- Signal.combine(taps.entries, taps.selectedId, search).map { case (es, sel, q) =>
        val shown = es.filter(e => matches(e.provenance.label, q))
        if (shown.isEmpty) emptyNode
        else
          div(
            cls := Styles.switcherSection,
            div(cls := Styles.switcherSectionHead, span(cls := Styles.switcherSectionLabel, "Taps · Live")),
            div(shown.map(e => tapRow(e, sel.contains(e.id), sd))),
          )
      },
    )

  private def tapRow(entry: TapEntry, selected: Boolean, sd: Observer[ResultsCommand]): HtmlElement =
    div(
      cls := Styles.switcherTapRow,
      cls := Styles.kindTap,
      cls(Styles.resultsViewing) := selected,
      onClick --> (_ => sd.onNext(ResultsCommand.SelectTap(entry.id))),
      span(
        cls := Styles.sourceKindIcon,
        cls(Styles.sourceDotLive) <-- entry.status.map(s => s == SourceStatus.Live || s == SourceStatus.Connecting),
        ResultsIcons.tap,
      ),
      span(cls := Styles.switcherRowName, entry.provenance.label),
      if (selected) span(cls := Styles.viewingPill, span(cls := Styles.viewingPillDot), "Viewing") else emptyNode,
      span(cls := Styles.switcherRowMeta, child.text <-- entry.stream.rows.signal.map(r => s"${r.size} captured")),
      div(
        cls := Styles.switcherRowActions,
        rowActionButton("Remove this tap", ResultsIcons.close, ResultsCommand.RemoveTap(entry.id), sd),
      ),
    )

  /** Query runs, newest first, narrowed by All / Kept and the bar's search. */
  private def queryResultsSection(
    entries: Signal[Vector[HistoryEntry]],
    viewingIdx: Signal[Option[Int]],
    search: Signal[String],
    keptOnly: Var[Boolean],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.switcherSection,
      div(
        cls := Styles.switcherSectionHead,
        span(cls := Styles.switcherSectionLabel, "Query results"),
        div(cls := Styles.tapsHeadingSpacer),
        allKeptToggle(keptOnly),
      ),
      children <-- Signal.combine(entries, search, keptOnly.signal).map { case (es, q, kept) =>
        val rows = es.zipWithIndex.reverse.filter { case (e, _) =>
          (!kept || e.pinned) && matches(e.content.queryEcho, q)
        }
        if (rows.isEmpty) List(emptyHint(kept, q))
        else rows.map { case (entry, idx) => row(sd, entry, idx, viewingIdx) }.toList
      },
    )

  /** All / Kept — a mini segmented control over the retention filter. */
  private def allKeptToggle(keptOnly: Var[Boolean]): HtmlElement =
    div(
      cls := Styles.switcherFilterSeg,
      keptSeg("All", target = false, keptOnly),
      keptSeg("Kept", target = true, keptOnly),
    )

  private def keptSeg(label: String, target: Boolean, keptOnly: Var[Boolean]): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.switcherFilterSegBtn,
      cls(Styles.resultsViewSegActive) <-- keptOnly.signal.map(_ == target),
      onClick --> (_ => keptOnly.set(target)),
      label,
    )

  private def gcFooter(): HtmlElement =
    div(
      cls := Styles.switcherGcFooter,
      span(cls := Styles.switcherGcIcon, ResultsIcons.recycle),
      span("Older runs clear automatically · bookmarked ones stay"),
    )

  private def emptyHint(kept: Boolean, q: String): HtmlElement =
    div(
      cls := Styles.sourcesColEmpty,
      if (q.trim.nonEmpty) s"""No runs match "${q.trim}""""
      else if (kept) "Nothing bookmarked yet"
      else "No runs yet",
    )

  /** A subtle row-trailing action (compare / remove) — stops propagation so it doesn't also select
    * the row.
    */
  private def rowActionButton(
    titleText: String,
    glyph: HtmlElement,
    cmd: ResultsCommand,
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.switcherRowAction,
      title := titleText,
      onClick --> { ev =>
        ev.stopPropagation()
        sd.onNext(cmd)
      },
      glyph,
    )

  private def matches(text: String, q: String): Boolean = {
    val needle = q.trim.toLowerCase
    needle.isEmpty || text.toLowerCase.contains(needle)
  }

  // --- Query-run rows --------------------------------------------------------

  private def row(
    sd: Observer[ResultsCommand],
    entry: HistoryEntry,
    idx: Int,
    viewingIdx: Signal[Option[Int]],
  ): HtmlElement = {
    val viewing: Signal[Boolean] = viewingIdx.map(_.contains(idx))
    div(
      cls := Styles.historyRow,
      cls := SourceFace.queryClass,
      cls(Styles.resultsViewing) <-- viewing, // current run: left accent bar + tint (the design's marker)
      onClick --> (_ => sd.onNext(ResultsCommand.OpenEntry(idx))),
      statusMark(entry.content.outcome),
      queryEchoCell(entry.content.queryEcho),
      span(cls := Styles.historyRowMeta, metaLabel(entry)),
      button(
        tpe := "button",
        cls := Styles.historyRowPin,
        cls(Styles.resultsKeepActive) := entry.pinned,
        title := (if (entry.pinned) "Bookmarked (click to remove)" else "Bookmark, keep this run"),
        onClick --> { ev =>
          ev.stopPropagation()
          sd.onNext(ResultsCommand.TogglePinAt(idx))
        },
        ResultsIcons.bookmark(filled = entry.pinned),
      ),
    )
  }

  /** The history row's status mark: a colored status dot. */
  private def statusMark(outcome: ResultOutcome): HtmlElement =
    span(cls := Styles.resultsStatusDot, cls := dotClass(outcome))

  private def dotClass(outcome: ResultOutcome): String = outcome match {
    case _: ResultOutcome.EmptyResult => Styles.resultsStatusEmpty
    case _ => Styles.resultsStatusOk
  }

  /** The run's query, middle-truncated: the tail (where iterative refinements usually differ) always
    * shows, while the head ellipsizes — so two near-identical queries don't look the same. Whitespace
    * is collapsed for the one-line display; the full text is on hover.
    */
  private def queryEchoCell(raw: String): HtmlElement = {
    val (head, tail) = ResultsData.middleSplit(raw)
    span(
      cls := Styles.historyRowQuery,
      title := raw,
      span(cls := Styles.historyRowQueryHead, head),
      span(cls := Styles.historyRowQueryTail, tail),
    )
  }

  private def metaLabel(entry: HistoryEntry): String = {
    val rows = entry.content.outcome match {
      case ResultOutcome.Tabular(result) => s"${result.results.size} rows"
      case ResultOutcome.TextResults(values) => s"${values.size} results"
      case _: ResultOutcome.EmptyResult => "0 rows"
      case _: ResultOutcome.Restored => "restored"
    }
    s"$rows · ${entry.timeLabel}"
  }
}
