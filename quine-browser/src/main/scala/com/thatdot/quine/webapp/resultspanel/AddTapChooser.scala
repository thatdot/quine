package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.cards.TapCardQuery
import com.thatdot.quine.webapp.util.Pot

/** The add-a-tap chooser — the switcher's create mode. A flat, always-visible list of standing
  * queries (no accordions): each block shows the SQ's Raw tap point and, per output, an
  * enrich—Post pipeline (or a "no enrichment" note when the output isn't enriched, since its
  * stream would equal Raw). A back header returns to the switcher; a legend explains the two
  * pill states. Chooser view state (the list filter) is local and resets on each entry.
  */
object AddTapChooser {

  def apply(
    catalog: Signal[Pot[Vector[TapCatalogEntry]]],
    watching: Signal[Set[String]],
    sd: Observer[ResultsCommand],
  ): HtmlElement = {
    // Chooser-local: each entry into the chooser starts with a clear filter (fresh mount).
    val filter = Var("")
    div(
      cls := Styles.tapChooser,
      backHeader(sd),
      legend(),
      children <-- Signal.combine(catalog, filter.signal, watching).map { case (pot, ftext, watch) =>
        chooserBody(pot, ftext, filter, watch, sd)
      },
    )
  }

  private def backHeader(sd: Observer[ResultsCommand]): HtmlElement =
    div(
      cls := Styles.chooserBackHeader,
      button(
        tpe := "button",
        cls := Styles.chooserBack,
        title := "Back to sources",
        onClick --> (_ => sd.onNext(ResultsCommand.ToggleTapChooser)), // chooser open: back to the switcher
        "‹",
      ),
      div(
        cls := Styles.chooserTitleWrap,
        span(cls := Styles.chooserTitle, "Add a tap"),
        span(
          cls := Styles.chooserSubtitle,
          "Watch any point in a standing query's flow. A tap adds load to its query, so go easy on busy ones in production",
        ),
      ),
    )

  /** Legend: what the two pill states mean. */
  private def legend(): HtmlElement =
    div(
      cls := Styles.chooserLegend,
      span(cls := Styles.chooserLegendItem, span(cls := Styles.chooserSwatch), "tap point, click to view it live"),
      span(
        cls := Styles.chooserLegendItem,
        span(cls := Styles.chooserSwatch, cls := Styles.chooserSwatchTapped),
        "already tapped",
      ),
    )

  private def chooserBody(
    pot: Pot[Vector[TapCatalogEntry]],
    ftext: String,
    filter: Var[String],
    watch: Set[String],
    sd: Observer[ResultsCommand],
  ): List[HtmlElement] = pot match {
    case Pot.Empty | Pot.Pending => List(loadingSkeleton())
    case Pot.Failed(_) => List(errorState())
    case other =>
      other.toOption.filter(_.nonEmpty) match {
        case None => List(emptyState())
        case Some(sqs) =>
          val filtered = applyFilter(sqs, ftext)
          val filterEl = if (sqs.size > 6) List(filterInput(filter, sqs.size, filtered.size, ftext)) else Nil
          val list =
            if (filtered.isEmpty) List(noMatch(ftext))
            else filtered.map(sq => sqBlock(sq, watch, sd)).toList
          filterEl ++ list
      }
  }

  private def applyFilter(sqs: Vector[TapCatalogEntry], text: String): Vector[TapCatalogEntry] = {
    val q = text.trim.toLowerCase
    if (q.isEmpty) sqs
    else sqs.filter(sq => sq.sqName.toLowerCase.contains(q) || sq.outputs.exists(_.name.toLowerCase.contains(q)))
  }

  private def filterInput(filter: Var[String], total: Int, shown: Int, text: String): HtmlElement =
    div(
      cls := Styles.tapChooserFilterWrap,
      input(
        tpe := "text",
        cls := Styles.tapChooserFilter,
        placeholder := "Filter standing queries…",
        controlled(value <-- filter.signal, onInput.mapToValue --> filter.writer),
      ),
      if (text.trim.nonEmpty) span(cls := Styles.tapChooserCount, s"$shown of $total standing queries")
      else emptyNode,
    )

  /** One standing query: a header (name · output count · Matches pill) over a row per output
    * (its enrich—Enriched pipeline).
    */
  private def sqBlock(
    sq: TapCatalogEntry,
    watch: Set[String],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.sqBlock,
      div(
        cls := Styles.sqBlockHead,
        span(cls := Styles.sourceKindIcon, cls := Styles.kindQuery, ResultsIcons.query),
        span(cls := Styles.sqBlockName, title := sq.sqName, sq.sqName),
        span(cls := Styles.sqBlockCount, outputsMeta(sq)),
        div(cls := Styles.tapsHeadingSpacer),
        span(cls := Styles.sqAllMatches, "all matches"),
        tapControl(sq.sqName, TapPoint.Raw, "SQ Matches", sq.patternQuery, watch, sd),
      ),
      div(sq.outputs.map(out => outputRow(sq.sqName, out, watch, sd))),
    )

  private def outputsMeta(sq: TapCatalogEntry): String = sq.outputs.size match {
    case 0 => "no outputs"
    case 1 => "1 output"
    case n => s"$n outputs"
  }

  /** One output's tap-point pipeline. When the output is enriched, show enrich › Post around the
    * enrichment query; otherwise the output's stream equals Raw, so show only a muted
    * "no enrichment" note (tap Raw instead).
    */
  private def outputRow(
    sqName: String,
    out: TapOutput,
    watch: Set[String],
    sd: Observer[ResultsCommand],
  ): HtmlElement =
    div(
      cls := Styles.tapOutputRow,
      span(cls := Styles.tapOutputName, title := out.name, out.name),
      div(
        cls := Styles.prePostGroup,
        if (out.hasEnrichment)
          List[Modifier[HtmlElement]](
            enrichNode(),
            tapControl(sqName, TapPoint.PostEnrichment(out.name), "Enriched", out.enrichmentQuery, watch, sd),
          )
        else
          List[Modifier[HtmlElement]](
            span(cls := Styles.noEnrichment, "no enrichment, results match Raw"),
          ),
      ),
    )

  /** The dashed "enrich" node before the Post pill — the enrichment query Post observes the
    * results of.
    */
  private def enrichNode(): Modifier[HtmlElement] =
    Seq(
      div(cls := Styles.enrichNode, title := "resultEnrichment query", ResultsIcons.gear, span("enrich")),
      span(cls := Styles.prePostConnector, "›"),
    )

  /** A tap point's control: a solid "SQ Matches/Enriched" button, or an outlined "✓" pill when
    * that location is already tapped (picking it again just re-focuses the existing tap).
    */
  private def tapControl(
    sqName: String,
    tapPoint: TapPoint,
    label: String,
    queryText: Option[String],
    watch: Set[String],
    sd: Observer[ResultsCommand],
  ): HtmlElement = {
    val target = TapTarget(sqName, tapPoint)
    // Tooltip: what the point observes plus (when known) the query that produced the data —
    // shared with the pipeline tree via TapCardQuery.hoverDesc.
    if (watch.contains(target.key))
      span(cls := Styles.viewingPill, title := TapCardQuery.hoverDesc(tapPoint, queryText), s"✓ $label")
    else
      button(
        tpe := "button",
        cls := Styles.tapButton,
        title := TapCardQuery.hoverDesc(tapPoint, queryText),
        onClick --> (_ => sd.onNext(ResultsCommand.OpenTap(target))),
        label,
      )
  }

  private def loadingSkeleton(): HtmlElement =
    div(cls := Styles.chooserSkeleton, (0 until 4).toList.map(_ => div(cls := Styles.chooserSkeletonRow)))

  private def emptyState(): HtmlElement =
    div(
      cls := Styles.chooserMessage,
      div(cls := Styles.chooserMessageTitle, "No standing queries defined"),
      div(
        cls := Styles.chooserMessageDesc,
        "A tap watches a standing query's output. Define one to start tapping it here.",
      ),
    )

  private def errorState(): HtmlElement =
    div(
      cls := Styles.chooserError,
      div(
        cls := Styles.chooserErrorHead,
        span(cls := Styles.resultsStatusDot, cls := Styles.resultsStatusError),
        span("Couldn't load standing queries"),
      ),
      div(cls := Styles.chooserMessageDesc, "The request didn't complete. Check the connection and try again."),
    )

  private def noMatch(text: String): HtmlElement =
    div(cls := Styles.sourcesColEmpty, s"""No standing queries match "${text.trim}"""")
}
