package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** The fixed-height control row above the results body — laid out like a browser's toolbar so it
  * triggers the same intuitions.
  *
  * Left: adjacent **back/forward** arrows over the visited-source stack (where you just were —
  * query runs and taps alike; no `k/n`, since GC renumbers chronology out from under any count).
  * Then the wide **identity bar** — the URL-bar analog: kind glyph + name (queries middle-truncate
  * so trailing refinements stay visible) + one action (✎ edit a query, ✕ stop a tap) + a caret
  * that drops the switcher, like a location bar's history dropdown. Then the **count**. Right
  * cluster (content only, hidden while the picker is open): the result-value controls (filter /
  * Table·JSON / export) for a query or a **Live** pulse for a tap. The bar carries no collapse
  * control — hiding the panel is the canvas door's job (an in-place toggle at the corner) — and
  * tap creation deliberately has no bar button: it lives in the switcher (taps carry a cost).
  */
object ResultsHeader {

  // ── Whole-result headers ───────────────────────────────────────────────────

  def standard(
    content: ResultsContent,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
    nav: NavReads,
    sd: Observer[ResultsCommand],
    sourcesOpen: Signal[Boolean],
    addingTap: Signal[Boolean],
    switcherSearch: Signal[String],
    hasLiveTaps: Signal[Boolean],
  ): HtmlElement =
    div(
      cls := Styles.resultsHeader,
      navArrows(nav, sd),
      queryChip(content.queryEcho, hasLiveTaps, sd),
      resultMeta(content.outcome, reads.search),
      // While the switcher is open the bar is its header (search + Cancel); otherwise the result-value
      // controls, which act on the content the picker covers.
      headerActions(sourcesOpen, addingTap, switcherSearch, sd, ViewerControls(content.outcome, reads, vd)),
    )

  def tap(
    entry: TapEntry,
    nav: NavReads,
    sd: Observer[ResultsCommand],
    sourcesOpen: Signal[Boolean],
    addingTap: Signal[Boolean],
    switcherSearch: Signal[String],
  ): HtmlElement = {
    val face = chipFace(entry)
    div(
      cls := Styles.resultsHeader,
      navArrows(nav, sd),
      tapChip(entry, face, sd),
      tapMeta(entry, face),
      headerActions(sourcesOpen, addingTap, switcherSearch, sd, liveChip(face)),
    )
  }

  /** The right cluster. When the switcher is open the bar becomes the switcher's header — a search
    * field + Cancel; when the add-tap chooser is open the chooser owns its own header, so the cluster
    * is empty; otherwise it shows `normal` (the per-kind controls).
    */
  private def headerActions(
    sourcesOpen: Signal[Boolean],
    addingTap: Signal[Boolean],
    switcherSearch: Signal[String],
    sd: Observer[ResultsCommand],
    normal: => Modifier[HtmlElement],
  ): HtmlElement =
    div(
      cls := Styles.resultsHeaderActions,
      child <-- sourcesOpen.combineWith(addingTap).map {
        case (true, false) => switcherSearchField(switcherSearch, sd)
        case (true, true) => emptyNode
        case (false, _) => span(display := "contents", normal)
      },
    )

  /** The switcher's search, hosted in the bar: a magnifier + input (autofocused) + a clear ✕, then a
    * Cancel that closes the switcher. Narrows both the taps and query-run lists in the body.
    */
  private def switcherSearchField(search: Signal[String], sd: Observer[ResultsCommand]): HtmlElement =
    span(
      display := "contents",
      div(
        cls := Styles.resultsSwitcherSearch,
        span(cls := Styles.resultsFilterIcon, ResultsIcons.magnifier),
        input(
          tpe := "text",
          cls := Styles.resultsFilterField,
          placeholder := "Search taps & query runs",
          controlled(value <-- search, onInput.mapToValue --> (v => sd.onNext(ResultsCommand.SetSwitcherSearch(v)))),
          onMountCallback(ctx => ctx.thisNode.ref.focus()),
          onKeyDown --> (e => if (e.key == "Escape") sd.onNext(ResultsCommand.CloseHistory)),
        ),
        child <-- search.map {
          case q if q.nonEmpty =>
            span(cursor := "pointer", "✕", onClick --> (_ => sd.onNext(ResultsCommand.SetSwitcherSearch(""))))
          case _ => emptyNode
        },
      ),
      button(
        tpe := "button",
        cls := Styles.resultsPickerCancel,
        onClick --> (_ => sd.onNext(ResultsCommand.CloseHistory)),
        "Cancel",
      ),
    )

  // ── Browser nav: back/forward arrows · the identity bar · count ────────────

  /** Adjacent browser-style back/forward arrows over the visited-source stack. */
  private def navArrows(nav: NavReads, sd: Observer[ResultsCommand]): HtmlElement =
    div(
      cls := Styles.resultsNav,
      navArrow(ResultsIcons.arrowLeft, nav.canBack, ResultsCommand.Back, "Back", sd),
      navArrow(ResultsIcons.arrowRight, nav.canForward, ResultsCommand.Forward, "Forward", sd),
    )

  private def navArrow(
    glyph: HtmlElement,
    enabled: Signal[Boolean],
    cmd: ResultsCommand,
    titleText: String,
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.resultsNavArrow,
      disabled <-- enabled.map(!_),
      title := titleText,
      onClick --> (_ => sd.onNext(cmd)),
      glyph,
    )

  /** Identity bar for a query — the URL-bar analog: kind glyph + the query text (middle-truncated,
    * so trailing refinements stay visible) + a trailing pencil (edit) + a hairline + a caret that
    * drops the switcher (carrying a live-dot when taps are streaming). The whole bar opens the
    * switcher; the pencil stops propagation and loads the query into the editor.
    */
  private def queryChip(
    text: String,
    hasLiveTaps: Signal[Boolean],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.resultsChip,
      cls := SourceFace.queryClass,
      title := "Browse queries & taps",
      onClick --> (_ => sd.onNext(ResultsCommand.ToggleHistory)),
      span(cls := Styles.sourceKindIcon, SourceFace.queryIcon),
      queryChipName(text),
      if (text.nonEmpty) chipAction("Edit query", ResultsIcons.pencil, ResultsCommand.UseQuery(text), sd)
      else emptyNode,
      span(cls := Styles.resultsChipRule),
      chipCaret(hasLiveTaps),
    )

  /** Identity bar for a tap: broadcast glyph (pulses while live) + name + a trailing action — ✕ to
    * stop while following, or a reload to restart once ended — then a hairline + switcher caret.
    */
  private def tapChip(
    entry: TapEntry,
    face: Signal[TapFace],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.resultsChip,
      cls := Styles.kindTap,
      title := "Browse queries & taps",
      onClick --> (_ => sd.onNext(ResultsCommand.ToggleHistory)),
      span(
        cls := Styles.sourceKindIcon,
        cls(Styles.sourceDotLive) <-- face.map(_ == TapFace.Following),
        ResultsIcons.tap,
      ),
      tapChipName(entry.provenance.label),
      child <-- face.map {
        case TapFace.Following =>
          chipAction("Stop tap", ResultsIcons.close, ResultsCommand.StopTap(entry.id), sd, stop = true)
        case TapFace.Ended => chipAction("Restart tap", ResultsIcons.reload, ResultsCommand.RestartTap(entry.id), sd)
        case TapFace.Errored => emptyNode
      },
      span(cls := Styles.resultsChipRule),
      chipCaret(Signal.fromValue(false)), // the tap is already in view — no live-dot cue needed
    )

  /** The bar's trailing caret — the switcher affordance (a location bar's history dropdown).
    * Carries a teal live-dot (top-right) while taps are streaming, so their existence is
    * discoverable from a query focus.
    */
  private def chipCaret(hasLiveTaps: Signal[Boolean]): HtmlElement =
    span(
      cls := Styles.resultsChipCaret,
      "▾",
      child <-- hasLiveTaps.map(live => if (live) span(cls := Styles.resultsChipLiveDot) else emptyNode),
    )

  /** A query's bar text, middle-truncated: the head ellipsizes while the tail (where iterative
    * refinements usually differ) always shows. Whitespace collapses for the one-line display; the
    * full text is on hover. Fills the bar's flexible middle, pushing the action + caret to the edge.
    */
  private def queryChipName(raw: String): HtmlElement = {
    val (head, tail) = ResultsData.middleSplit(raw)
    div(
      cls := Styles.resultsChipFill,
      title := raw,
      if (head.isEmpty && tail.isEmpty) span(cls := Styles.resultsChipNameTail, "(no query)")
      else
        Seq(
          span(cls := Styles.resultsChipNameHead, head),
          span(cls := Styles.resultsChipNameTail, tail),
        ),
    )
  }

  /** A tap's bar text: the standing-query name, then a muted ` · point` suffix (the label bakes the
    * tap point in as `"sq · enriched"`). The name ellipsizes; the suffix always shows so the tap point
    * stays legible. Fills the bar's flexible middle.
    */
  private def tapChipName(label: String): HtmlElement = {
    val sep = " · "
    div(
      cls := Styles.resultsChipFill,
      label.lastIndexOf(sep) match {
        case at if at >= 0 =>
          Seq(
            span(cls := Styles.resultsChipName, label.take(at)),
            span(cls := Styles.resultsChipSuffix, label.drop(at)),
          )
        case _ => Seq(span(cls := Styles.resultsChipName, label))
      },
    )
  }

  /** A chip's trailing action button — faint at rest, gaining a filled tile on hover/focus, so it's
    * the one interactive mark in the chip (the kind-glyph never reacts). Stops propagation so it
    * edits/stops rather than opening the switcher. `kind-error` re-skins it red for the stop ✕.
    */
  private def chipAction(
    titleText: String,
    glyph: Modifier[HtmlElement],
    cmd: ResultsCommand,
    sd: Observer[ResultsCommand],
    stop: Boolean = false,
  ): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.resultsChipAction,
      cls(Styles.kindError) := stop,
      title := titleText,
      onClick --> { ev =>
        ev.stopPropagation()
        sd.onNext(cmd)
      },
      glyph,
    )

  private def liveChip(face: Signal[TapFace]): Modifier[HtmlElement] =
    child <-- face.map {
      case TapFace.Following => span(cls := Styles.resultsLiveChip, span(cls := Styles.resultsLiveDot), "Live")
      case TapFace.Ended | TapFace.Errored => emptyNode
    }

  /** A tap's display state — a closed ADT (not strings) so a typo in a match is a compile error. */
  sealed abstract private class TapFace
  private object TapFace {
    case object Following extends TapFace
    case object Ended extends TapFace
    case object Errored extends TapFace
  }

  private def chipFace(entry: TapEntry): Signal[TapFace] =
    entry.status.map {
      case SourceStatus.Ended => TapFace.Ended
      case SourceStatus.Error(_) => TapFace.Errored
      case _ => TapFace.Following
    }

  // ── Count / status ─────────────────────────────────────────────────────────

  /** The result's count/status: a row/result count (reactive to the filter for tabular results), or
    * a short status word for empty and errored runs.
    */
  private def resultMeta(outcome: ResultOutcome, search: Signal[String]): HtmlElement = outcome match {
    case ResultOutcome.Tabular(result) =>
      val total = result.results.size
      span(
        cls := Styles.resultsRowCount,
        child.text <-- search.map { q =>
          if (q.trim.isEmpty) s"$total rows"
          else s"${ResultsData.filter(result, q).results.size} of $total rows"
        },
      )
    case ResultOutcome.TextResults(values) => span(cls := Styles.resultsRowCount, s"${values.size} results")
    case _: ResultOutcome.EmptyResult => span(cls := Styles.resultsRowCount, "no rows")
    case ResultOutcome.Restored(Some(_)) => span(cls := Styles.resultsRowCount, "error (restored)")
    case _: ResultOutcome.Restored => span(cls := Styles.resultsRowCount, "restored")
  }

  /** A tap's live capture count, plus a terminal word when the stream is no longer following. */
  private def tapMeta(entry: TapEntry, face: Signal[TapFace]): HtmlElement =
    span(
      cls := Styles.resultsRowCount,
      child.text <-- entry.stream.rows.signal.combineWith(face).map { case (rows, f) =>
        val captured = s"${rows.size} captured"
        f match {
          case TapFace.Ended => s"$captured · ended"
          case TapFace.Errored => s"$captured · error"
          case TapFace.Following => captured
        }
      },
    )

}
