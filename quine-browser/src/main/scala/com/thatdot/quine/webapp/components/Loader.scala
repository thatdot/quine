package com.thatdot.quine.webapp.components

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** Spinning loader, with a count of the number of things loading in the center.
  *
  * Only if the count is non-zero will the spinner be visible, and only if the
  * count is greater than 1 will the counter be visible.
  *
  * @param pendingCount number of things that are loading
  * @param onCancel optional handler for a cancellation event (triggered by a click)
  */
object Loader {
  def apply(pendingCount: Long, onCancel: Option[() => Unit] = None): HtmlElement =
    if (pendingCount == 0) span(display := "none")
    else {
      val classes = if (onCancel.nonEmpty) s"${Styles.loader} ${Styles.loaderCancellable}" else Styles.loader
      div(
        cls := classes,
        onCancel.map(handler => onClick --> (_ => handler())),
        onCancel.map(_ => title := "Cancel all queries"),
        div(cls := Styles.loaderSpinner),
        if (pendingCount > 1) Some(div(cls := Styles.loaderCounter, pendingCount.toString)) else None,
      )
    }
}
