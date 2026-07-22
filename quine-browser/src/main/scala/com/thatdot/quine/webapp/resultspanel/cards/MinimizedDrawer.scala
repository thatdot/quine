package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.{ResultOutcome, ResultsIcons, SourceStatus, TapPoint}

/** Bottom-right floating vertical stack of minimized cards over the canvas (design doc §3
  * "Minimized cards — right-edge drawer"). Host-agnostic: takes the signals/observer it
  * needs rather than a concrete [[CardsStore]], so it can be mounted by any host that
  * wires those up (see the integration notes for the concrete wiring).
  *
  * Per-row content is deliberately minimal — kind color strip, truncated title, one status
  * line — nothing else, per the design. The list itself scrolls when there are more cards
  * than fit, with a search field pinned above it filtering by title/query text.
  */
object MinimizedDrawer {

  /** List height capping the drawer at roughly [[CardDefaults.MaxVisibleMinimizedCards]]
    * visible rows before it scrolls. Row math mirrors the CSS: `.mini-card` has a 38px
    * *minimum* height (rows with a wrapped two-line title grow taller), with a 1px
    * divider between adjacent rows — so this is an estimate, not an exact cap.
    */
  private val MiniCardRowPx = 38
  private val RowDividerPx = 1
  private val listMaxHeightPx: Int =
    CardDefaults.MaxVisibleMinimizedCards * MiniCardRowPx +
    (CardDefaults.MaxVisibleMinimizedCards - 1) * RowDividerPx

  /** @param hasCards whether any card exists at all (expanded or minimized) — distinct from
    *   `minimized` being empty, which also happens when the sole card is currently expanded.
    *   No cards at all renders nothing; any cards keep the drawer shell mounted.
    */
  def apply(
    minimized: Signal[Vector[CardState]],
    search: Signal[String],
    dispatch: Observer[CardCommand],
    hasCards: Signal[Boolean],
  ): HtmlElement = {
    val collapsed = Var(false)

    val filtered: Signal[Vector[CardState]] =
      minimized.combineWith(search).map { case (cards, needle) =>
        val n = needle.trim.toLowerCase
        if (n.isEmpty) cards else cards.filter(matches(_, n))
      }

    div(
      cls := CardStyles.drawer,
      // Zero cards at all: render nothing. Any cards existing (even if all are currently
      // expanded, so `minimized` alone is empty) keeps the normal panel shell mounted, just
      // with nothing in `drawerList` to show. The whole panel itself collapses to a small
      // corner button and back via `collapsed`.
      child <-- hasCards.distinct.combineWith(collapsed.signal).map {
        case (false, _) => emptyNode
        case (true, true) => collapsedButton(minimized, collapsed)
        case (true, false) =>
          div(
            cls := CardStyles.drawerPanel,
            searchField(search, dispatch, collapsed),
            div(
              cls := CardStyles.drawerList,
              maxHeight := s"${listMaxHeightPx}px",
              children <-- filtered.splitSeq(_.id)(rowSig => miniCardRow(rowSig.key, rowSig, dispatch)),
            ),
          )
      },
    )
  }

  /** Small corner button the collapsed drawer folds into: the docked-panel glyph with a
    * minimized-card count badge, expanding the panel again on click.
    */
  private def collapsedButton(minimized: Signal[Vector[CardState]], collapsed: Var[Boolean]): HtmlElement =
    div(
      cls := CardStyles.drawerCollapsedButton,
      title := "Show results drawer",
      onClick --> (_ => collapsed.set(false)),
      ResultsIcons.dockedPanel,
      span(cls := CardStyles.drawerCollapsedCount, child.text <-- minimized.map(_.size.toString)),
    )

  private def matches(card: CardState, needle: String): Boolean = {
    val query = card.kind match {
      case CardKind.AdhocCard(queryText, _, _) => queryText
      case CardKind.TapTableCard(target, _, _) => target.sqName
    }
    card.title.toLowerCase.contains(needle) || query.toLowerCase.contains(needle)
  }

  private def searchField(
    search: Signal[String],
    dispatch: Observer[CardCommand],
    collapsed: Var[Boolean],
  ): HtmlElement =
    div(
      cls := CardStyles.drawerSearchWrap,
      ResultsIcons.magnifier,
      input(
        tpe := "text",
        cls := CardStyles.drawerSearchInput,
        placeholder := "Search cards",
        controlled(value <-- search, onInput.mapToValue --> (t => dispatch.onNext(CardCommand.Search(t)))),
      ),
      span(
        cls := CardStyles.drawerCollapse,
        title := "Minimize drawer",
        onClick --> (_ => collapsed.set(true)),
        ResultsIcons.windowMinimize,
      ),
    )

  /** Whether the tap underlying this card is currently in [[SourceStatus.Error]] — the
    * "errored" overlay this row should carry (design doc: "tap cards whose underlying tap is
    * errored"). Tap-table cards read this straight off their [[TapEntry.status]].
    * Adhoc cards are never "tap-errored" (a failed adhoc run surfaces as a toast, not a card).
    */
  private def tapErrored(kind: CardKind): Signal[Boolean] = kind match {
    case CardKind.TapTableCard(_, entry, _) => entry.status.map { case SourceStatus.Error(_) => true; case _ => false }
    case _: CardKind.AdhocCard => Val(false)
  }

  private def kindClass(kind: CardKind, tapErrored: Boolean): String = kind match {
    case _: CardKind.AdhocCard => Styles.kindQuery
    case _: CardKind.TapTableCard => if (tapErrored) Styles.kindError else Styles.kindTap
  }

  /** The leading kind glyph for a row: a query card reads as a run (terminal glyph); a tap
    * card reads as which pipeline stage it's watching — SQ matches (broadcast), a
    * transformation step (ƒ), or the post-enrichment stage (the enrichment's graph glyph) —
    * echoing the same glyph language as the tap-point selector (see
    * [[com.thatdot.quine.webapp.resultspanel.tapmodal.SqPipelineTree]]).
    */
  private def kindIcon(kind: CardKind): HtmlElement = kind match {
    case _: CardKind.AdhocCard => span(cls := Styles.sourceKindIcon, ResultsIcons.query)
    case CardKind.TapTableCard(target, _, _) =>
      target.tapPoint match {
        case TapPoint.Raw => span(cls := Styles.sourceKindIcon, ResultsIcons.tap)
        case _: TapPoint.PreEnrichment =>
          span(cls := Styles.sourceKindIcon, span(cls := CardStyles.miniCardKindFx, "ƒ"))
        case _: TapPoint.PostEnrichment => span(cls := Styles.sourceKindIcon, ResultsIcons.cypher)
      }
  }

  private def rowCountOf(kind: CardKind): Option[Int] = kind match {
    case CardKind.AdhocCard(_, _, outcomeOpt) =>
      outcomeOpt.map(_.outcome).collect { case ResultOutcome.Tabular(r) => r.results.size }
    case _ => None
  }

  private def statusLine(card: CardState, tapErrored: Boolean): HtmlElement = card.kind match {
    case CardKind.AdhocCard(_, _, _) =>
      // No stopped badge: adhoc cards have nothing running, so `stopped` is never set for
      // them (see `CardsStore.autoStopIfTapTable`).
      span(
        cls := CardStyles.miniCardStatus,
        rowCountOf(card.kind).fold("…")(n => s"$n rows"),
      )
    case CardKind.TapTableCard(_, entry, _) if tapErrored =>
      span(
        cls := CardStyles.miniCardStatus,
        child.text <-- entry.status.map {
          case SourceStatus.Error(message) => s"⚠ error: $message"
          case _ => "⚠ error"
        },
      )
    case CardKind.TapTableCard(_, entry, _) =>
      // No stopped/paused dot: a stopped tap already reads as paused via its frozen row
      // count, and the standalone amber dot (design doc's streams-page badge color) read
      // as an error/warning at this size — the live dot alone (pulsing green) is enough to
      // tell "still running" from "not".
      span(
        cls := CardStyles.miniCardStatus,
        child.text <-- entry.stream.rows.signal.map(rs => s"${rs.size} rows"),
        child <-- entry.ended.signal.map { ended =>
          if (ended || card.stopped) emptyNode
          else span(cls := CardStyles.miniCardStatusDot, cls(CardStyles.miniCardStatusLive) := true, " ●")
        },
      )
  }

  /** One minimized row, keyed by `id` (stable across `splitSeq` re-renders — see the docs on
    * [[com.thatdot.quine.webapp.components.streams.StandingQueryTable]] for the same pattern).
    * `cardSig` tracks the card reactively for its content (status line, kind, stopped flag); `id`
    * itself never changes for a mounted row, so click handlers close over it directly instead of
    * reading it off the signal.
    */
  private def miniCardRow(id: CardId, cardSig: Signal[CardState], dispatch: Observer[CardCommand]): HtmlElement = {
    // Re-derives the tap-error signal whenever the card's kind changes (e.g. a restart
    // swapping in a fresh TapEntry) rather than once at mount — `.flatMapSwitch` re-runs
    // `tapErrored` for the current kind on every `cardSig` update.
    val tapErroredSig: Signal[Boolean] = cardSig.map(_.kind).flatMapSwitch(tapErrored)
    val rowSig: Signal[(CardState, Boolean)] = cardSig.combineWith(tapErroredSig)

    div(
      cls := CardStyles.miniCard,
      cls <-- rowSig.map { case (c, errored) => kindClass(c.kind, errored) },
      title <-- cardSig.map(_.title),
      onClick --> (_ => dispatch.onNext(CardCommand.Expand(id))),
      div(cls := CardStyles.miniCardStrip),
      child <-- cardSig.map(_.kind).map(kindIcon),
      div(
        cls := CardStyles.miniCardBody,
        div(cls := CardStyles.miniCardTitle, child.text <-- cardSig.map(_.title)),
        child <-- rowSig.map { case (c, errored) => statusLine(c, errored) },
      ),
      span(
        cls := CardStyles.miniCardClose,
        title := "Close",
        ResultsIcons.close,
        onClick --> { e =>
          e.stopPropagation()
          dispatch.onNext(CardCommand.Close(id))
        },
      ),
    )
  }
}
