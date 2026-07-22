package com.thatdot.quine.webapp.resultspanel.tapmodal

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.resultspanel.{TapCatalogEntry, TapTarget}
import com.thatdot.quine.webapp.util.Pot

/** The standing-query tap modal (design doc §2), replacing `AddTapChooser`: a single
  * [[SqPipelineTree]] step — pick a standing query + tap point (or focus an already-tapped
  * one), which opens a tabular tap card immediately. Graph-canvas taps are not created
  * here — the persistent tap-query (graph tap) catalog lives in the Explorer Settings
  * modal's Graph Feeds card.
  *
  * Open state is entirely host-driven — a `Signal[Boolean]` in, an `Observer[Boolean]` out — so a
  * host can back it with a Var, a route, or a larger junk-drawer-adjacent UI state machine
  * without this component knowing which.
  *
  * Every data dependency arrives as a parameter; nothing here reaches into a global store. See
  * `laneC-integration.md` for exactly which service calls the host should wire each callback to.
  */
object TapModal {

  /** @param openSignal whether the modal is currently shown
    * @param setOpen close (`false`) or (re)open (`true`) the modal; Escape and outside-click
    *                both call this with `false`
    * @param catalog standing-query catalog for the pipeline tree
    * @param tappedKeys `TapTarget.key`s with an active tap (✓ badge / focus-instead-of-retap)
    * @param onOpenTabular a tap point was picked — open the tap and spawn a tabular tap card
    * @param onFocusExisting an already-tapped point (✓) was clicked — bring its card to front
    *                        instead of tapping again
    */
  def apply(
    openSignal: Signal[Boolean],
    setOpen: Observer[Boolean],
    catalog: Signal[Pot[Vector[TapCatalogEntry]]],
    tappedKeys: Signal[Set[String]],
    onOpenTabular: TapTarget => Unit,
    onFocusExisting: TapTarget => Unit,
  ): HtmlElement = {

    def close(): Unit = setOpen.onNext(false)

    def openTabular(target: TapTarget): Unit = {
      onOpenTabular(target)
      close()
    }

    def focusExisting(target: TapTarget): Unit = {
      onFocusExisting(target)
      close()
    }

    div(
      cls := TapModalStyles.overlay,
      display <-- openSignal.map(if (_) "flex" else "none"),
      // Outside click: only when the click landed on the overlay itself, not bubbled up from the
      // dialog (the dialog's own onClick.stopPropagation below is a second line of defense).
      onClick.filter(e => e.target == e.currentTarget) --> (_ => close()),
      // Gated on `openSignal`: this binder lives on the always-mounted overlay (only
      // `display` toggles), so without the gate every app-wide Escape would write `false`
      // into the host's open observer even while the modal is closed.
      documentEvents(_.onKeyDown)
        .filter(_.key == "Escape")
        .withCurrentValueOf(openSignal) --> { case (_, open) =>
        if (open) close()
      },
      div(
        cls := TapModalStyles.dialog,
        onClick.stopPropagation --> (_ => ()), // dialog clicks must not bubble to the overlay's outside-click handler
        div(
          cls := TapModalStyles.header,
          span(cls := TapModalStyles.title, "Standing Query Inspection"),
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
          // Gated on `openSignal`: the overlay itself stays mounted (only `display` toggles),
          // so without the gate the body's subtree — SqPipelineTree binders — would stay
          // subscribed and retained for the app's lifetime while the modal is closed.
          // Reopening rebuilds the body fresh.
          child <-- openSignal.map {
            case false => emptyNode
            case true =>
              SqPipelineTree(catalog, tappedKeys, openTabular, focusExisting)
          },
        ),
      ),
    )
  }
}
