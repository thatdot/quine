package com.thatdot.quine.webapp.resultspanel.tapmodal

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.cards.TapCardQuery
import com.thatdot.quine.webapp.resultspanel.{ResultsIcons, TapCatalogEntry, TapOutput, TapPoint, TapTarget}
import com.thatdot.quine.webapp.util.Pot

/** Step 1 of the tap modal ("Inspect Standing Queries"): a standing-query picker rail on the
  * left, and — for the selected query — a compact left-to-right flow of its pipeline on the
  * right. The tappable points (Raw, plus per-output Transformed and Enriched points where the
  * output defines those steps) are filled ⊕ buttons — the only interactive, only filled
  * elements. The steps between them shrink to icon "beads" sitting on the connector lines
  * (ƒ = JavaScript transformation, a graph triad = Cypher enrichment); their words live in
  * tooltips and in the tap captions. Neither the query nor a lone output is named in the flow:
  * the rail already names the query, and an output name only appears (as quiet text) when
  * multiple outputs need telling apart.
  *
  * {{{
  * ┌ wire-fraud ┐   Click a ⊕ to open a live view of results at that point.
  * │ login-sq ● │
  * │ audit-sq   │   (⊕ Matches) ──┬─ alerts ─ƒ─▶ (⊕ Transformed) ─◬─▶ (⊕ Enriched)
  * └ search…    ┘                 └─ audit   delivers Matches as-is, watch it above
  * }}}
  *
  * An already-tapped point renders as a teal ✓ with a "viewing" tag — clicking it focuses the
  * existing card instead of re-tapping (design doc §2: "✓ = already tapped (click to focus its
  * card)").
  *
  * Host-agnostic: takes the catalog and the already-tapped key set as Signals, and reports picks
  * via callbacks — no store or command-ADT dependency.
  */
object SqPipelineTree {

  /** How the tree is being used; the structure is shared, the copy and the offered points are
    * not.
    *   - [[Variant.Inspect]] — the Standing Query Inspection modal: every distinct point (Raw,
    *     Transformed, Enriched) is offered, a marked point means "already inspected" and
    *     clicking it focuses the existing card.
    *   - [[Variant.PickPoint]] — the graph-feed editor's point picker: every distinct point
    *     (Raw, Transformed, Enriched) is offered, and the marked point is the editor's
    *     current single selection.
    */
  sealed trait Variant
  object Variant {
    case object Inspect extends Variant
    case object PickPoint extends Variant
  }

  /** @param catalog available standing queries + their outputs
    * @param tappedKeys `TapTarget.key`s rendered as teal ✓ nodes — active inspections in
    *                   [[Variant.Inspect]], the current (≤1) selection in [[Variant.PickPoint]]
    * @param onPickNew an unmarked tap point was clicked — proceed (open a card / select it)
    * @param onFocusExisting an already-marked point was clicked — focus its card (Inspect) or
    *                        keep the selection (PickPoint)
    * @param variant which surface is hosting the tree; see [[Variant]]
    * @param initialSqName preselect this standing query in the rail (used when editing an
    *                      existing feed); falls back to the first visible query
    */
  def apply(
    catalog: Signal[Pot[Vector[TapCatalogEntry]]],
    tappedKeys: Signal[Set[String]],
    onPickNew: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
    variant: Variant = Variant.Inspect,
    initialSqName: Option[String] = None,
  ): HtmlElement = {
    val filterVar = Var("")
    val selectedVar = Var[Option[String]](initialSqName)

    val loadedSqs: Signal[Vector[TapCatalogEntry]] = catalog.map(_.toOption.getOrElse(Vector.empty))
    val filtered: Signal[Vector[TapCatalogEntry]] =
      loadedSqs.combineWith(filterVar.signal).map { case (sqs, text) => applyFilter(sqs, text) }
    // The selection survives filtering only while its SQ is still visible; otherwise
    // (and on first render) fall back to the first visible query.
    val currentName: Signal[Option[String]] =
      filtered
        .combineWith(selectedVar.signal)
        .map { case (f, sel) => sel.filter(name => f.exists(_.sqName == name)).orElse(f.headOption.map(_.sqName)) }
        .distinct
    val currentEntry: Signal[Option[TapCatalogEntry]] =
      filtered
        .combineWith(currentName)
        .map { case (f, name) => name.flatMap(n => f.find(_.sqName == n)) }
        .distinct

    // Built once and reused by reference across `children <--` recomputes below: the
    // rail hosts the search input, and a rebuilt input loses focus after every
    // keystroke. The rail's own list (and each row's active/● state) updates
    // reactively inside the stable element instead.
    val railEl = rail(filtered, filterVar, currentName, tappedKeys, selectedVar, variant)

    div(
      cls := TapModalStyles.inspectRoot,
      // Keyed on load state + the selected entry only — NOT the filter text or tapped
      // keys, which previously rebuilt every SQ row and pipeline pill per keystroke /
      // tap tick (and remounted the search input mid-typing). `.distinct`: a filter
      // keystroke that doesn't change the selected entry re-emits an equal pair and
      // must not touch the DOM at all.
      children <-- catalog
        .combineWith(currentEntry)
        .distinct
        .map { case (pot, entry) =>
          pot match {
            case Pot.Empty | Pot.Pending => List(loadingSkeleton())
            case Pot.Failed(_) => List(errorState())
            case other if other.toOption.exists(_.nonEmpty) =>
              List(
                railEl,
                entry match {
                  case None => noMatchesPanel(filterVar.signal)
                  case Some(sq) => diagramPanel(sq, tappedKeys, onPickNew, onFocusExisting, variant)
                },
              )
            case _ => List(emptyState(variant))
          }
        },
    )
  }

  private def applyFilter(sqs: Vector[TapCatalogEntry], text: String): Vector[TapCatalogEntry] = {
    val q = text.trim.toLowerCase
    if (q.isEmpty) sqs
    else sqs.filter(sq => sq.sqName.toLowerCase.contains(q) || sq.outputs.exists(_.name.toLowerCase.contains(q)))
  }

  // ── left rail: search + one row per standing query ─────────────────────────────────

  private def rail(
    filtered: Signal[Vector[TapCatalogEntry]],
    filterVar: Var[String],
    currentName: Signal[Option[String]],
    tappedKeys: Signal[Set[String]],
    selectedVar: Var[Option[String]],
    variant: Variant,
  ): HtmlElement =
    div(
      cls := TapModalStyles.inspectRail,
      div(
        cls := TapModalStyles.inspectRailSearchWrap,
        ResultsIcons.magnifier,
        input(
          tpe := "text",
          cls := TapModalStyles.inspectRailSearch,
          // The picker rail is narrower than the inspection modal's, so the long
          // placeholder would clip mid-word there.
          placeholder := (variant match {
            case Variant.Inspect => "Search standing queries…"
            case Variant.PickPoint => "Search…"
          }),
          controlled(value <-- filterVar.signal, onInput.mapToValue --> filterVar.writer),
        ),
      ),
      div(
        cls := TapModalStyles.inspectRailList,
        // Keyed by SQ name: filtering only mounts/unmounts rows, and each row's
        // active highlight / ● dot binds its own narrowed `.distinct` signal.
        children <-- filtered.splitSeq(_.sqName) { strictSignal =>
          val name = strictSignal.key
          railItem(
            name,
            isActive = currentName.map(_.contains(name)).distinct,
            // Key prefix match is safe: `TapTarget.key` is colon-delimited and SQ
            // names cannot contain colons (see [[TapTarget.key]]).
            hasTap = tappedKeys.map(_.exists(_.startsWith(s"$name:"))).distinct,
            selectedVar,
            variant,
          )
        },
      ),
    )

  private def railItem(
    sqName: String,
    isActive: Signal[Boolean],
    hasTap: Signal[Boolean],
    selectedVar: Var[Option[String]],
    variant: Variant,
  ): HtmlElement = {
    val dotTitle = variant match {
      case Variant.Inspect => "Has an active inspection"
      case Variant.PickPoint => "Contains the selected point"
    }
    button(
      tpe := "button",
      cls := TapModalStyles.inspectRailItem,
      cls(TapModalStyles.inspectRailItemActive) <-- isActive,
      onClick --> (_ => selectedVar.set(Some(sqName))),
      span(cls := TapModalStyles.inspectRailItemName, title := sqName, sqName),
      child <-- hasTap.map {
        case true => span(cls := TapModalStyles.inspectRailItemDot, title := dotTitle, "●")
        case false => emptyNode
      },
    )
  }

  // ── right panel: the selected SQ's stage diagram ───────────────────────────────────

  private def noMatchesPanel(ftext: Signal[String]): HtmlElement =
    div(
      cls := TapModalStyles.diagramEmpty,
      child.text <-- ftext.map(text => s"""No standing queries match "${text.trim}""""),
    )

  private def diagramPanel(
    sq: TapCatalogEntry,
    tapped: Signal[Set[String]],
    onPickNew: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
    variant: Variant,
  ): HtmlElement = {
    // The Raw caption names what it precedes, most specific description first.
    val rawCaption =
      if (sq.outputs.exists(out => out.hasTransformation || out.hasEnrichment))
        "every match before any enrichment or transformation"
      else "every match the query produces"
    def rawTap: HtmlElement =
      tapNode(
        sq.sqName,
        TapPoint.Raw,
        "SQ Matches",
        rawCaption,
        sq.patternQuery,
        tapped,
        onPickNew,
        onFocusExisting,
        variant,
      )

    def hint: HtmlElement = div(
      cls := TapModalStyles.diagramHint,
      variant match {
        case Variant.Inspect =>
          "Select one or more inspection points to view the data a standing query and its outputs are processing, and inspect it live."
        case Variant.PickPoint =>
          "Select a single point to draw from. Every result arriving there runs this feed's query, " +
            "and what it returns is added to the graph."
      },
    )

    // One continuous flow starting at Raw: Raw feeds the outputs, whose steps carry the
    // stream to their Transformed/Enriched points. A single output stays entirely on the
    // horizontal line (and goes unnamed, there is nothing to tell apart); only multiple
    // outputs drop a branch trunk out of the Raw node, naming each row.
    sq.outputs match {
      case Nil =>
        div(
          cls := TapModalStyles.diagram,
          hint,
          div(cls := TapModalStyles.diagramFlow, rawTap),
          div(
            cls := TapModalStyles.diagramNote,
            "No outputs defined. The match stream is the only place to watch.",
          ),
        )
      case single :: Nil =>
        div(
          cls := TapModalStyles.diagram,
          hint,
          div(
            cls := TapModalStyles.diagramFlow,
            rawTap,
            outputStages(
              sq.sqName,
              single,
              sq.patternQuery,
              tapped,
              onPickNew,
              onFocusExisting,
              inBranch = false,
              variant,
            ),
          ),
        )
      case _ =>
        div(
          cls := TapModalStyles.diagram,
          hint,
          div(
            cls := TapModalStyles.diagramFlow,
            div(
              cls := TapModalStyles.diagramRawStack,
              rawTap,
              div(
                cls := TapModalStyles.diagramBranch,
                sq.outputs.map(out =>
                  branchRow(sq.sqName, out, sq.patternQuery, tapped, onPickNew, onFocusExisting, variant),
                ),
              ),
            ),
          ),
        )
    }
  }

  /** One output's row, branching off the trunk: an arrowed stub, the output's name as quiet
    * text, then its stages ([[outputStages]]).
    */
  private def branchRow(
    sqName: String,
    out: TapOutput,
    patternQuery: Option[String],
    tapped: Signal[Set[String]],
    onPickNew: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
    variant: Variant,
  ): HtmlElement =
    div(
      cls := TapModalStyles.diagramBranchRow,
      div(cls := TapModalStyles.diagramBranchStub),
      span(cls := TapModalStyles.diagramBranchName, title := out.name, out.name),
      outputStages(sqName, out, patternQuery, tapped, onPickNew, onFocusExisting, inBranch = true, variant),
    )

  /** Everything downstream of the output itself: a step bead and tap point for the
    * transformation (when defined), then a step bead and tap point for the enrichment (when
    * defined). An output with neither step delivers the Raw stream unchanged, so no point is
    * offered; a muted trailing note says so (and, in the branch layout, points back up at the
    * Matches node that carries the same stream). Shared between the single-output inline flow
    * and the multi-output branch rows; captions keep the fuller "every match, …" phrasing in
    * both layouts, so a point reads the same regardless of how many outputs the query has.
    */
  private def outputStages(
    sqName: String,
    out: TapOutput,
    patternQuery: Option[String],
    tapped: Signal[Set[String]],
    onPickNew: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
    inBranch: Boolean,
    variant: Variant,
  ): List[Modifier[HtmlElement]] = {
    val prefix = "every match, "
    val transformed: List[Modifier[HtmlElement]] =
      if (!out.hasTransformation) Nil
      else
        List(
          stepBead("JavaScript transformation", span(cls := TapModalStyles.diagramBeadFx, "ƒ")),
          tapNode(
            sqName,
            TapPoint.PreEnrichment(out.name),
            "Transformed",
            s"${prefix}after the transformation",
            patternQuery,
            tapped,
            onPickNew,
            onFocusExisting,
            variant,
          ),
        )
    val enriched: List[Modifier[HtmlElement]] =
      if (out.hasEnrichment)
        List(
          stepBead("Cypher enrichment query", ResultsIcons.cypher),
          tapNode(
            sqName,
            TapPoint.PostEnrichment(out.name),
            "Enriched",
            s"${prefix}after the enrichment query",
            out.enrichmentQuery,
            tapped,
            onPickNew,
            onFocusExisting,
            variant,
          ),
        )
      else Nil
    val endNote: Option[String] =
      if (transformed.nonEmpty || enriched.nonEmpty) None
      else
        Some(variant match {
          case Variant.Inspect =>
            if (inBranch) "delivers Matches as-is, watch it above" else "delivers Matches as-is"
          case Variant.PickPoint =>
            if (inBranch) "delivers Matches as-is, select SQ Matches above" else "delivers Matches as-is"
        })
    endNote match {
      case Some(note) => transformed :+ span(cls := TapModalStyles.diagramEnds, note)
      case None => transformed ++ enriched
    }
  }

  /** An arrowed connector segment carrying a workflow step as a small icon bead sitting on
    * the line; the step's name lives in the bead's tooltip.
    */
  private def stepBead(stepName: String, icon: Modifier[HtmlElement]): HtmlElement =
    div(
      cls := TapModalStyles.diagramConnector,
      span(cls := TapModalStyles.diagramBead, title := stepName, icon),
    )

  /** A tappable point on the pipeline: a filled ⊕ circle (teal ✓ once tapped) with the
    * point's name and a plain-English caption stacked to its right, so caption length never
    * moves the circle the connectors join. Clicking a fresh point starts the destination
    * flow; clicking a tapped one focuses its existing card. The ⊕/✓ state binds its own
    * narrowed `.distinct` signal, so a tapped-keys tick flips glyphs in place instead of
    * rebuilding the diagram.
    */
  private def tapNode(
    sqName: String,
    tapPoint: TapPoint,
    label: String,
    caption: String,
    queryText: Option[String],
    tapped: Signal[Set[String]],
    onPickNew: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
    variant: Variant,
  ): HtmlElement = {
    val target = TapTarget(sqName, tapPoint)
    val isTapped: Signal[Boolean] = tapped.map(_.contains(target.key)).distinct
    val (chipText, tappedTooltipSuffix) = variant match {
      case Variant.Inspect => ("viewing", "Already inspected, click to focus its card.")
      case Variant.PickPoint => ("selected", "This feed draws from here.")
    }
    button(
      tpe := "button",
      cls := TapModalStyles.diagramTap,
      cls(TapModalStyles.diagramTapTapped) <-- isTapped,
      // Tooltip: what the point observes plus (when known) the query that produced the data —
      // shared with AddTapChooser via TapCardQuery.hoverDesc.
      title <-- isTapped.map(t =>
        if (t) s"${TapCardQuery.hoverDesc(tapPoint, queryText)} $tappedTooltipSuffix"
        else TapCardQuery.hoverDesc(tapPoint, queryText),
      ),
      onClick.compose(_.sample(isTapped)) --> { tappedNow =>
        if (tappedNow) onFocusExisting(target) else onPickNew(target)
      },
      span(
        cls := TapModalStyles.diagramTapGlyph,
        child <-- isTapped.map(t => if (t) ResultsIcons.check else ResultsIcons.plus),
      ),
      span(
        cls := TapModalStyles.diagramTapText,
        span(
          cls := TapModalStyles.diagramTapHead,
          span(cls := TapModalStyles.diagramTapLabel, label),
          child <-- isTapped.map {
            case true => span(cls := TapModalStyles.diagramTapViewing, chipText)
            case false => emptyNode
          },
        ),
        span(cls := TapModalStyles.diagramTapCaption, caption),
      ),
    )
  }

  private def loadingSkeleton(): HtmlElement =
    div(cls := Styles.chooserSkeleton, (0 until 4).toList.map(_ => div(cls := Styles.chooserSkeletonRow)))

  private def emptyState(variant: Variant): HtmlElement =
    div(
      cls := Styles.chooserMessage,
      div(cls := Styles.chooserMessageTitle, "No standing queries defined"),
      div(
        cls := Styles.chooserMessageDesc,
        variant match {
          case Variant.Inspect =>
            "An inspection watches a standing query's output. Define one to start inspecting it here."
          case Variant.PickPoint =>
            "A graph feed draws a standing query's results onto the graph. Define a standing query first."
        },
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
}
