package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** A small tag in the top-left corner of the canvas while a time is pinned: the filled clock
  * and "3 hours ago", mostly transparent until the pointer reaches it. A pinned time otherwise
  * shows only in the toolbar's amber clock and its tooltip, and it survives a reload, so
  * without words somewhere at rest a user can query the past without knowing. The corner is
  * the one the other overlays leave free: the loader and the result count sit top right, the
  * feed chips bottom left, the cards drawer bottom right.
  *
  * The wording is relative, as the ruler's bubble is, and ticks while pinned so it keeps
  * describing the same fixed instant; the tooltip carries the exact time. Clicking the tag
  * opens the query-time dialog, as the toolbar's clock does.
  */
object AtTimeTag {

  /** How often "3 hours ago" is brought up to date. */
  private val TickMillis = 1000

  def apply(
    atTime: Signal[Option[Long]],
    canSetTime: Signal[Boolean],
    openAtTimeModal: () => Unit,
  ): Mod[HtmlElement] = {
    val pinned: Signal[Boolean] = atTime.map(_.isDefined).distinct
    val tick: Signal[Unit] = pinned
      .flatMapSwitch(p => if (p) EventStream.periodic(TickMillis) else EventStream.empty)
      .mapToUnit
      .startWith(())
    val label: Signal[String] = atTime
      .combineWith(tick)
      .map(_.fold("")(millis => AtTimeInput.describeRelative(millis, js.Date.now().toLong)))
      .distinct
    val tooltip: Signal[String] = atTime
      .map(t => s"Querying for time: ${t.fold("now")(AtTimeInput.formatIso)}, click to change")
      .distinct
    val open: Observer[Boolean] = Observer(canSet => if (canSet) openAtTimeModal())

    val tag: HtmlElement = span(
      cls := AtTimeModalStyles.canvasTag,
      cls(Styles.clickable) <-- canSetTime.distinct,
      role := "button",
      tabIndex := 0,
      title <-- tooltip,
      i(cls := "ion-ios-time", aria.hidden := true),
      span(child.text <-- label),
      onClick.compose(_.sample(canSetTime)) --> open,
      onKeyDown.filter(e => e.key == "Enter" || e.key == " ").preventDefault.compose(_.sample(canSetTime)) --> open,
    )

    child.maybe <-- pinned.map(p => Option.when(p)(tag))
  }
}
