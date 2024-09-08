package com.thatdot.quine.webapp.components

import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.web.html._

import com.thatdot.quine.webapp.Styles

/** Spinning loader, with a count of the number of things loading in the center.
  *
  * Only if the count is non-zero will the spinner be visible, and only if the
  * count is greater than 1 will the counter be visible.
  */
@react object Loader {

  /** @param keyName React key for the outer `div` (when something _is_ displayed)
    * @param pendingCount number of things that are loading
    * @param onCancel handler for a cancellation event (triggered by a click)
    */
  case class Props(keyName: String, pendingCount: Long, onClick: Option[() => Unit])

  val component: FunctionalComponent[Loader.Props] = FunctionalComponent[Props] {
    case Props(_, 0, _) => Nil
    case Props(keyName, n, onClick_) =>
      val classes = if (onClick_.nonEmpty) s"${Styles.loader} ${Styles.loaderCancellable}" else Styles.loader
      val title_ = onClick_.map(_ => "Cancel all queries")
      List(if (n == 1) {
        div(key := keyName, className := classes, onClick := onClick_, title := title_)(
          div(key := "spinner", className := Styles.loaderSpinner)(),
        )
      } else {
        div(key := s"$keyName-$n", className := classes, onClick := onClick_, title := title_)(
          div(key := "spinner", className := Styles.loaderSpinner)(),
          div(key := n.toString, className := Styles.loaderCounter)(n.toString),
        )
      })
  }
}
