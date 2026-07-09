package com.thatdot.quine.webapp.resultspanel

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.document

/** Shared pointer-drag plumbing for the resize handles in the results surface — the
  * vertical grab rail, the compare-pane divider, and the column-resize handles. All
  * three need the same gesture: begin on a left-button mousedown, track `mousemove`
  * at the document level (so the pointer can leave the thin handle mid-drag), end on
  * `mouseup`, and tear the document listeners down on unmount. Callers supply only
  * the domain mapping — what a press begins and what each move does — and read the
  * pointer geometry off the event; this object owns the listener lifecycle.
  */
object DragGesture {

  /** Modifiers wiring a drag gesture onto the handle element they are applied to.
    *
    * @param onStart   called on a left-button mousedown; return `true` to begin the
    *                  drag (capture any start state — e.g. the pointer origin — here)
    * @param onMove    called on every `mousemove` while dragging; `preventDefault` is
    *                  applied for you afterwards
    * @param bodyClass a class toggled on `<body>` for the drag's duration (e.g. a
    *                  `col-resize` cursor + document-wide text-selection suppression)
    */
  def handle(
    onStart: dom.MouseEvent => Boolean,
    onMove: dom.MouseEvent => Unit,
    bodyClass: Option[String] = None,
  ): Modifier[HtmlElement] = {
    val dragging = Var(false)

    val move: js.Function1[dom.MouseEvent, Unit] = (e: dom.MouseEvent) => {
      onMove(e)
      e.preventDefault()
    }
    val up: js.Function1[dom.MouseEvent, Unit] = (_: dom.MouseEvent) => dragging.set(false)

    def listen(active: Boolean): Unit =
      if (active) {
        bodyClass.foreach(c => document.body.classList.add(c))
        document.addEventListener("mousemove", move)
        document.addEventListener("mouseup", up)
      } else {
        bodyClass.foreach(c => document.body.classList.remove(c))
        document.removeEventListener("mousemove", move)
        document.removeEventListener("mouseup", up)
      }

    Seq[Modifier[HtmlElement]](
      dragging.signal --> (active => listen(active)),
      onMouseDown --> { e =>
        if (e.button == 0 && onStart(e)) {
          dragging.set(true)
          e.preventDefault()
        }
      },
      onUnmountCallback(_ => listen(false)),
    )
  }
}
