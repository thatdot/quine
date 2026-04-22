package com.thatdot.quine.webapp.components

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles

/** A toolbar button that supports left-click for the default action and
  * right-click or long-press to reveal a context menu of alternative actions.
  */
object ToolbarButton {

  /** An action shown in the right-click context menu.
    *
    * @param content the menu item content (text or rich HTML)
    * @param title tooltip for the menu item
    * @param action callback when the item is clicked
    */
  final case class MenuAction(
    content: Modifier[HtmlElement],
    title: String,
    action: () => Unit,
  )

  private val LongPressDurationMs: Double = 500

  /** Create a toolbar button with an optional right-click context menu.
    *
    * Left-click performs the default action. Right-click (desktop) or
    * long-press (touch) opens a dropdown menu of alternative actions.
    *
    * @param ionClass Ionicons CSS class for the button icon
    * @param tooltipTitle tooltip text shown on hover
    * @param enabled reactive signal controlling the enabled/disabled state
    * @param onClickAction handler for left-click (only fires when enabled)
    * @param menuActions callback returning context menu items (called each time the menu opens)
    */
  def apply(
    ionClass: String,
    tooltipTitle: String,
    enabled: Signal[Boolean] = Val(true),
    onClickAction: dom.MouseEvent => Unit = _ => (),
    menuActions: () => Seq[MenuAction] = () => Seq.empty,
  ): HtmlElement = {
    val menuOpenVar = Var(false)
    val menuTopVar = Var(0.0)
    val menuLeftVar = Var(0.0)
    var wrapperEl: Option[dom.html.Element] = None
    var menuEl: Option[dom.html.Element] = None
    var longPressTimer: Option[Int] = None
    var longPressFired: Boolean = false

    val handleMouseDown: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (menuOpenVar.now()) {
        val target = event.target.asInstanceOf[dom.Node]
        val clickedInsideWrapper = wrapperEl.exists(_.contains(target))
        val clickedInsideMenu = menuEl.exists(_.contains(target))
        if (!clickedInsideWrapper && !clickedInsideMenu) menuOpenVar.set(false)
      }
    }

    def openMenu(): Unit =
      if (menuActions().nonEmpty)
        wrapperEl.foreach { el =>
          val rect = el.getBoundingClientRect()
          menuTopVar.set(rect.bottom)
          menuLeftVar.set(rect.left)
          menuOpenVar.set(true)
        }

    def toggleMenu(): Unit =
      if (menuOpenVar.now()) menuOpenVar.set(false)
      else openMenu()

    def cancelLongPress(): Unit = {
      longPressTimer.foreach(dom.window.clearTimeout)
      longPressTimer = None
    }

    div(
      display := "inline-flex",
      alignItems := "center",
      onMountCallback { ctx =>
        wrapperEl = Some(ctx.thisNode.ref)
        dom.document.addEventListener("mousedown", handleMouseDown)
      },
      onUnmountCallback { _ =>
        dom.document.removeEventListener("mousedown", handleMouseDown)
      },
      // Main icon button
      htmlTag("i")(
        cls <-- enabled.map { e =>
          s"$ionClass ${Styles.navBarButton} ${if (e) Styles.clickable else Styles.disabled}"
        },
        title := tooltipTitle,
        onClick.compose(_.withCurrentValueOf(enabled).collect { case (e, true) => e }) --> { e =>
          if (longPressFired) longPressFired = false
          else onClickAction(e)
        },
        onContextMenu.compose(_.withCurrentValueOf(enabled).collect { case (e, true) => e }) --> { e =>
          e.preventDefault()
          // Skip toggle when the menu was just opened by a long-press, since
          // touch devices fire contextmenu after the long-press timer.
          if (!longPressFired) toggleMenu()
        },
        // Long-press support for touch devices
        onTouchStart --> { _ =>
          longPressFired = false
          longPressTimer = Some(
            dom.window.setTimeout(
              () => {
                longPressFired = true
                openMenu()
              },
              LongPressDurationMs,
            ),
          )
        },
        onTouchEnd --> { _ => cancelLongPress() },
        onTouchMove --> { _ => cancelLongPress() },
        onTouchCancel --> { _ => cancelLongPress() },
      ),
      // Context menu dropdown
      ul(
        cls <-- menuOpenVar.signal.map(open => s"toolbar-context-menu${if (open) " open" else ""}"),
        top <-- menuTopVar.signal.map(t => s"${t}px"),
        left <-- menuLeftVar.signal.map(l => s"${l}px"),
        onMountCallback(ctx => menuEl = Some(ctx.thisNode.ref)),
        children <-- menuOpenVar.signal.map {
          case false => Seq.empty[HtmlElement]
          case true =>
            menuActions().map { menuAction =>
              li(
                title := menuAction.title,
                onClick --> { _ =>
                  menuOpenVar.set(false)
                  menuAction.action()
                },
                menuAction.content,
              )
            }
        },
      ),
    )
  }

  /** Create a simple toolbar button without a context menu. */
  def simple(
    ionClass: String,
    tooltipTitle: String,
    enabled: Signal[Boolean] = Val(true),
    onClickAction: dom.MouseEvent => Unit = _ => (),
  ): HtmlElement =
    htmlTag("i")(
      cls <-- enabled.map { e =>
        s"$ionClass ${Styles.navBarButton} ${if (e) Styles.clickable else Styles.disabled}"
      },
      title := tooltipTitle,
      onClick.compose(_.withCurrentValueOf(enabled).collect { case (e, true) => e }) --> onClickAction,
    )

  /** Create a toolbar button with a dynamically changing icon (e.g., play/pause toggle). */
  def dynamic(
    ionClass: Signal[String],
    tooltipTitle: Signal[String],
    onClickAction: dom.MouseEvent => Unit,
  ): HtmlElement =
    htmlTag("i")(
      cls <-- ionClass.map(icon => s"$icon ${Styles.navBarButton} ${Styles.clickable}"),
      title <-- tooltipTitle,
      onClick --> onClickAction,
    )
}
