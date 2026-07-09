package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.spaces2
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.streaming.StreamingView

/** The Explore results surface: a raised inset over the graph canvas.
  *
  * Mounted once (persistently). It auto-captures each live result into `history`,
  * renders whatever entry the history is currently pointed at (so navigating history
  * is decoupled from the live query signal), and hides itself when history is empty.
  * The canvas door is a separate component ([[CanvasDoor]]) mounted on the canvas layer,
  * reading only the slice it needs via [[DoorReads]].
  *
  * `surface` takes the concrete [[ResultsStore]] — it's the trusted root that wires the whole
  * panel and owns the store's `lifecycleMods`; read-only-ness still holds because the store's
  * mutable state is `private` (the only writer path is `dispatch`).
  *
  * All state changes flow through a single [[ResultsCommand]] dispatcher: components
  * read `Signal`s and emit commands; this object owns the slices and applies them.
  */
object ResultsPanel {

  def surface(store: ResultsStore): HtmlElement = {
    // The surface's own height in the Explore area is DOM-derived, so the "comfortable" target for
    // the picker-open grow is measured here (the store has no DOM).
    var rootRef: Option[dom.html.Element] = None
    def containerHeight(): Option[Double] =
      rootRef.flatMap(r => Option(r.parentNode)).collect { case el: dom.Element => el.clientHeight.toDouble }

    div(
      cls := Styles.resultsSurface,
      cls(Styles.resultsCollapsed) <-- store.collapsed,
      cls(Styles.resultsSurfaceAnimating) <-- store.heightAnimating,
      // Show when there's content, or when the picker is open (so a tap can be created from an
      // otherwise-empty panel via the canvas door).
      display <-- store.mainContent.combineWith(store.sourcesOpen).map { case (c, open) =>
        if (c.isDefined || open) "flex" else "none"
      },
      height <-- store.height.map(h => s"${h.toInt}px"),
      right <-- store.effectiveInset.map(px => s"${px}px"),
      onMountCallback(ctx => rootRef = Some(ctx.thisNode.ref)),
      // Opening the source picker on a short surface is cramped — grow to a comfortable height
      // (never shrink) so the menu has room.
      store.sourcesOpen.updates.filter(identity) --> { _ =>
        containerHeight().foreach(ch =>
          store.dispatch.onNext(ResultsCommand.EnsureHeight(ResultsLayout.comfortableHeightPx(ch))),
        )
      },
      // The store's event bindings (tap sync, result capture, viewer resets); element-owned.
      store.lifecycleMods,
      grabRail(store.dispatch),
      // The header rebuilds when the shown source/content changes.
      child <-- store.mainContent.map {
        case Some(Right(tap)) =>
          ResultsHeader.tap(
            tap,
            store.navReads,
            store.dispatch,
            store.sourcesOpen,
            store.addingTap,
            store.switcherSearch,
          )
        case Some(Left(curr)) =>
          ResultsHeader.standard(
            curr,
            store.readsPrimary,
            store.primaryDispatch,
            store.navReads,
            store.dispatch,
            store.sourcesOpen,
            store.addingTap,
            store.switcherSearch,
            store.tapsReads.entries.map(_.nonEmpty),
          )
        case None => emptyNode
      },
      // Stable content stack: the body swaps with the content; the picker overlay is bound only
      // to `sourcesOpen`, so it stays mounted across content/history changes.
      div(
        cls := Styles.resultsContentStack,
        // Thread the body's left edge to the shown source's kind (same accent as the chip).
        cls <-- store.mainContent.map(SourceFace.mainClass),
        child <-- store.mainContent.map {
          case Some(Right(tap)) => StreamingView.tapBody(tap)
          case Some(Left(curr)) => bodyFor(curr, store.readsPrimary, store.primaryDispatch)
          case None => emptyNode
        },
        child <-- store.sourcesOpen.map { open =>
          if (open)
            SourcesPanel(
              store.tapsReads,
              store.sessionReads,
              store.effectiveCatalog,
              store.addingTap,
              store.switcherSearch,
              store.dispatch,
            )
          else emptyNode
        },
      ),
    )
  }

  private def bodyFor(c: ResultsContent, reads: ViewerReads, vd: Observer[ViewerCommand]): HtmlElement =
    c.outcome match {
      case ResultOutcome.Tabular(result) =>
        div(
          cls := Styles.resultsContentArea,
          div(
            cls := Styles.resultsBody,
            child <-- Signal
              .combine(reads.view, reads.search, reads.sortCol, reads.sortDir)
              .map { case (view, needle, sortCol, sortDir) =>
                val derived = ResultsData.derive(result, needle, sortCol, sortDir)
                view match {
                  case ResultsView.Table =>
                    ResultsTable(derived, reads.sortCol, reads.sortDir, reads.colWidths, vd)
                  case ResultsView.Json => pre(cls := Styles.resultsJson, ResultsExport.toJson(derived)): HtmlElement
                }
              },
          ),
          RowDrawer(result.columns, reads.selectedRow, vd),
        )
      case ResultOutcome.TextResults(values) =>
        div(cls := Styles.resultsBody, pre(cls := Styles.resultsJson, textJson(values)))
      case ResultOutcome.EmptyResult(_, _) =>
        div(cls := Styles.resultsBody, ResultsPlaceholder.empty(c.queryEcho))
      case ResultOutcome.ErrorResult(err, actions) =>
        div(cls := Styles.resultsBody, ResultsPlaceholder.error(err, actions))
      case ResultOutcome.Restored(errorMessage) =>
        div(cls := Styles.resultsBody, ResultsPlaceholder.restored(c.queryEcho, errorMessage))
    }

  private def textJson(values: Seq[Json]): String = spaces2.print(Json.fromValues(values))

  /** Top grab rail: drag vertically to resize the surface (up to nearly the full
    * Explore area, leaving room for the query bar); double-click to collapse. Start
    * height and the live cap are read from the DOM so the cap adapts to the viewport.
    */
  private def grabRail(dispatch: Observer[ResultsCommand]): HtmlElement = {
    var railRef: Option[dom.html.Element] = None
    var start: Option[(Double, Double)] = None // (startPageY, startHeight)

    def surfaceEl: Option[dom.html.Element] =
      railRef.flatMap(r => Option(r.parentNode)).collect { case el: dom.html.Element => el }

    def container: Option[dom.Element] =
      surfaceEl.flatMap(s => Option(s.parentNode)).collect { case el: dom.Element => el } // the Explore container

    def maxHeight(): Double =
      container
        .map(c => (c.clientHeight.toDouble - ResultsLayout.queryBarReservePx).max(ResultsLayout.minSurfaceHeightPx))
        .getOrElse(Double.MaxValue)

    div(
      cls := Styles.resultsGrabRail,
      title := "Drag to resize · double-click to fit",
      onMountCallback(ctx => railRef = Some(ctx.thisNode.ref)),
      DragGesture.handle(
        onStart = e => {
          // clientHeight (not offsetHeight) excludes the surface's 1px border, so the
          // captured start height matches the model height the `height` binding renders.
          start = surfaceEl.map(s => (e.pageY, s.clientHeight.toDouble))
          start.isDefined
        },
        onMove = e =>
          start.foreach { case (startY, startHeight) =>
            dispatch.onNext(
              ResultsCommand.SetHeight(
                (startHeight + (startY - e.pageY)).max(ResultsLayout.minSurfaceHeightPx).min(maxHeight()),
              ),
            )
          },
      ),
      onDblClick --> (_ =>
        container.foreach(c =>
          dispatch.onNext(ResultsCommand.ToggleHeight(ResultsLayout.comfortableHeightPx(c.clientHeight.toDouble))),
        ),
      ),
    )
  }
}
