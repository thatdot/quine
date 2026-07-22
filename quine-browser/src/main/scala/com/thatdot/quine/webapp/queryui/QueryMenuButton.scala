package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles

/** The query bar's split "Query" button: a main region that runs the current buffer (same role
  * as today's single submit button — a host swaps this in as a drop-in replacement) plus a
  * narrow arrow segment that opens a popover menu of secondary query actions.
  *
  * Host-agnostic: every action and every piece of state is injected (callbacks, Signals,
  * Observers). This component owns only the popover's own open/closed Var and its outside-click
  * / Escape dismissal — no application state, no store access.
  *
  * Menu contents (design doc §1):
  *   - "Run as text query" — one-shot force-table run (today's Shift+Click path).
  *   - "Multi-line editing" — one-shot: the host focuses the editor and enters multi-line mode
  *     exactly as Shift+Enter would; the row exists so the keyboard affordance is discoverable.
  *   - "Bookmark this query" — one-shot; the host wires this to the existing
  *     DataService/SampleQueryDropdown bookmark plumbing (replaces the old BookmarkUi icon).
  *   - "Standing query results…" — one-shot; opens the unified tap modal (Lane C). Until that
  *     modal exists, the host may pass a disabled/no-op callback.
  */
object QueryMenuButton {

  /** @param label main-region button label (the run affordance) — "Query" today.
    * @param onRun main-region click / the host's existing submit path. Receives the shift-key
    *              state exactly like today's single button did (`shiftKey` forces a table run),
    *              so a host can wire this straight to its existing `runByVerdict(forceTable)`.
    * @param runDisabled mirrors today's single button's `disabled` binding.
    * @param runTitle mirrors today's single button's tooltip binding.
    * @param onRunAsText "Run as text query" one-shot action (Ctrl+Shift+Enter path).
    * @param onMultiline "Multi-line editing" one-shot action (the Shift+Enter path).
    * @param onBookmark "Bookmark this query" one-shot action.
    * @param onOpenTapModal "Standing query results…" one-shot action.
    * @param menuDisabled disables the arrow segment entirely (e.g. before the tap modal exists,
    *                     a host may instead choose to keep the menu enabled and pass a no-op to
    *                     `onOpenTapModal` — either is supported).
    */
  def apply(
    label: String,
    onRun: Boolean => Unit,
    runDisabled: Signal[Boolean],
    runTitle: Signal[String],
    onRunAsText: () => Unit,
    onMultiline: () => Unit,
    onBookmark: () => Unit,
    onOpenTapModal: () => Unit,
    menuDisabled: Signal[Boolean] = Signal.fromValue(false),
  ): HtmlElement = {
    val menuOpenVar: Var[Boolean] = Var(false)
    var rootEl: Option[dom.html.Element] = None

    def closeMenu(): Unit = menuOpenVar.set(false)
    def toggleMenu(): Unit = menuOpenVar.update(!_)

    val handleMouseDown: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (menuOpenVar.now()) {
        val target = event.target.asInstanceOf[dom.Node]
        val inside = rootEl.exists(_.contains(target))
        if (!inside) closeMenu()
      }
    }
    val handleKeyDown: js.Function1[dom.KeyboardEvent, Unit] = { (event: dom.KeyboardEvent) =>
      if (menuOpenVar.now() && event.key == "Escape") closeMenu()
    }

    div(
      cls := QueryMenuStyles.splitButton,
      onMountCallback { ctx =>
        rootEl = Some(ctx.thisNode.ref)
        dom.document.addEventListener("mousedown", handleMouseDown)
        dom.document.addEventListener("keydown", handleKeyDown)
      },
      onUnmountCallback { _ =>
        rootEl = None
        dom.document.removeEventListener("mousedown", handleMouseDown)
        dom.document.removeEventListener("keydown", handleKeyDown)
      },
      // Main region — same role as today's single submit button, so a host can lift this
      // element's onClick semantics (shiftKey forces a table run) unchanged. Deliberately does
      // NOT also carry `Styles.queryInputButton`: that class's `float: left` / `margin-right`
      // are meant for a lone button in the query bar and fight this component's `inline-flex`
      // shell (see the CSS comment on `.query-menu-split-button`) — `splitButtonMain` restates
      // the same visual look (background via `grayClickable`, padding/font/radius of its own)
      // without the conflicting layout properties.
      button(
        tpe := "button",
        cls := s"${Styles.grayClickable} ${QueryMenuStyles.splitButtonMain}",
        title <-- runTitle,
        disabled <-- runDisabled,
        onClick --> { e => onRun(e.shiftKey) },
        label,
      ),
      // Arrow segment — opens the secondary-actions popover.
      button(
        tpe := "button",
        cls := s"${Styles.grayClickable} ${QueryMenuStyles.splitButtonArrow}",
        title := "More query actions",
        aria.hasPopup := true,
        aria.expanded <-- menuOpenVar.signal,
        disabled <-- menuDisabled,
        onClick --> (_ => toggleMenu()),
        i(cls := s"cil-chevron-bottom ${QueryMenuStyles.splitButtonArrowIcon}"),
      ),
      div(
        cls <-- menuOpenVar.signal.map(open =>
          s"${QueryMenuStyles.menu}${if (open) s" ${QueryMenuStyles.menuOpen}" else ""}",
        ),
        children <-- menuOpenVar.signal.map {
          case false => Seq.empty[HtmlElement]
          case true =>
            Seq(
              runAsTextItem { () => closeMenu(); onRunAsText() },
              multilineItem { () => closeMenu(); onMultiline() },
              bookmarkItem { () => closeMenu(); onBookmark() },
              tapModalItem { () => closeMenu(); onOpenTapModal() },
            )
        },
      ),
    )
  }

  private def runAsTextItem(onClick0: () => Unit): HtmlElement =
    div(
      cls := QueryMenuStyles.menuItem,
      onClick --> (_ => onClick0()),
      span(cls := QueryMenuStyles.menuItemIcon, i(cls := "ion-play")),
      span(cls := QueryMenuStyles.menuItemLabel, "Run as text query"),
      span(cls := QueryMenuStyles.menuItemHint, "Shift+Click / Ctrl+Shift+↵"),
    )

  private def multilineItem(onClick0: () => Unit): HtmlElement =
    div(
      cls := QueryMenuStyles.menuItem,
      onClick --> (_ => onClick0()),
      span(cls := QueryMenuStyles.menuItemIcon, i(cls := "ion-ios-list")),
      span(cls := QueryMenuStyles.menuItemLabel, "Multi-line editing"),
      span(cls := QueryMenuStyles.menuItemHint, "⇧↵ for newline"),
    )

  private def bookmarkItem(onClick0: () => Unit): HtmlElement =
    div(
      cls := QueryMenuStyles.menuItem,
      onClick --> (_ => onClick0()),
      span(cls := QueryMenuStyles.menuItemIcon, i(cls := "ion-star")),
      span(cls := QueryMenuStyles.menuItemLabel, "Bookmark as sample query"),
    )

  private def tapModalItem(onClick0: () => Unit): HtmlElement =
    div(
      cls := QueryMenuStyles.menuItem,
      onClick --> (_ => onClick0()),
      span(cls := QueryMenuStyles.menuItemIcon, i(cls := "ion-radio-waves")),
      span(cls := QueryMenuStyles.menuItemLabel, "Standing Query Inspection"),
    )
}

/** CSS class names for [[QueryMenuButton]]. Rules live in the app's `index.css`; see
  * `laneB-integration.md` for the exact block to append (this file only declares names, per
  * house convention — it does not touch `Styles.scala` or `index.css`).
  */
object QueryMenuStyles {
  val splitButton = "query-menu-split-button"
  val splitButtonMain = "query-menu-split-button-main"
  val splitButtonArrow = "query-menu-split-button-arrow"
  val splitButtonArrowIcon = "query-menu-split-button-arrow-icon"

  val menu = "query-menu"
  val menuOpen = "open"
  val menuItem = "query-menu-item"
  val menuItemIcon = "query-menu-item-icon"
  val menuItemLabel = "query-menu-item-label"
  val menuItemHint = "query-menu-item-hint"
}
