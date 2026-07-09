package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.window

import com.thatdot.quine.webapp.Styles

/** Compact graph (namespace) selector that lives at the far right of the query
  * bar: a graph icon, the current graph name, and a chevron. Clicking opens a
  * dropdown to switch graphs. The button has a fixed footprint and ellipsizes
  * long names so it never resizes the query input or the rest of the bar.
  */
object GraphSelector {

  /** Show the filter input once this many graphs can be listed */
  private val FilterThreshold = 8

  def apply(
    selectedNamespaceVar: Var[Option[String]],
    knownNamespaces: Signal[Seq[String]],
    onOpen: Option[() => Unit] = None,
    defaultNamespace: Option[String] = None,
  ): HtmlElement = {
    val menuOpenVar = Var(false)
    val menuTopVar = Var(0.0)
    val menuLeftVar = Var(0.0)
    val filterVar = Var("")
    var buttonEl: Option[dom.html.Element] = None
    var menuEl: Option[dom.html.Element] = None

    val handleMouseDown: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (menuOpenVar.now()) {
        val target = event.target.asInstanceOf[dom.Node]
        val insideButton = buttonEl.exists(_.contains(target))
        val insideMenu = menuEl.exists(_.contains(target))
        if (!insideButton && !insideMenu) menuOpenVar.set(false)
      }
    }

    /** Horizontal position keeping the just-opened fixed-position menu inside
      * the viewport (the menu must already be displayed to be measurable).
      */
    def clampedLeft(desiredLeft: Double): Double = {
      val width = menuEl.map(_.getBoundingClientRect().width).getOrElse(0.0)
      math.max(8.0, math.min(desiredLeft, window.innerWidth - width - 8.0))
    }

    def toggleMenu(): Unit =
      if (menuOpenVar.now()) menuOpenVar.set(false)
      else {
        // Refresh the graph list on open so graphs created in another tab (or
        // by another user) show up without a page reload.
        onOpen.foreach(_())
        buttonEl.foreach { el =>
          val rect = el.getBoundingClientRect()
          menuTopVar.set(rect.bottom + 4)
          filterVar.set("")
          menuOpenVar.set(true)
          // Defer position measurement to the next animation frame so the
          // browser has reflowed the menu (now display:block) and its width
          // is accurate for right-alignment / viewport clamping.
          window.requestAnimationFrame { _ =>
            if (menuOpenVar.now()) {
              val width = menuEl.map(_.getBoundingClientRect().width).getOrElse(0.0)
              menuLeftVar.set(clampedLeft(rect.right - width))
            }
          }
        }
      }

    div(
      cls := Styles.graphSelector,
      onMountCallback(_ => dom.document.addEventListener("mousedown", handleMouseDown)),
      onUnmountCallback(_ => dom.document.removeEventListener("mousedown", handleMouseDown)),
      button(
        tpe := "button",
        cls := Styles.graphSelectorButton,
        title <-- selectedNamespaceVar.signal.map(_.fold("Select a graph")(ns => s"Graph: $ns (click to switch)")),
        aria.hasPopup := true,
        aria.expanded <-- menuOpenVar.signal,
        onMountCallback(ctx => buttonEl = Some(ctx.thisNode.ref)),
        onClick --> { _ => toggleMenu() },
        i(cls := s"ion-android-share-alt ${Styles.graphSelectorIcon}"),
        span(cls := Styles.graphSelectorName, child.text <-- selectedNamespaceVar.signal.map(_.getOrElse(""))),
        i(cls := s"cil-chevron-bottom ${Styles.graphSelectorChevron}"),
      ),
      div(
        cls <-- menuOpenVar.signal.map(open => s"${Styles.graphSelectorMenu}${if (open) " open" else ""}"),
        top <-- menuTopVar.signal.map(t => s"${t}px"),
        left <-- menuLeftVar.signal.map(l => s"${l}px"),
        onMountCallback(ctx => menuEl = Some(ctx.thisNode.ref)),
        children <-- menuOpenVar.signal.combineWith(knownNamespaces, selectedNamespaceVar.signal).map {
          case (false, _, _) => Seq.empty[HtmlElement]
          case (true, known, selected) =>
            def selectItem(ns: String): HtmlElement =
              div(
                cls := s"${Styles.graphSelectorMenuItem}${if (selected.contains(ns)) " active" else ""}",
                title := s"Switch to $ns",
                onClick --> { _ =>
                  menuOpenVar.set(false)
                  selectedNamespaceVar.set(Some(ns))
                },
                span(cls := Styles.graphSelectorMenuName, ns),
              )

            val header = div(cls := Styles.graphSelectorMenuHeader, "Graphs")
            // The filter input is created once per menu opening (not per
            // keystroke) so it keeps focus while typing; only the list below
            // re-renders as the filter changes.
            val filter =
              if (known.length > FilterThreshold)
                Seq(
                  input(
                    cls := Styles.graphSelectorMenuFilter,
                    tpe := "text",
                    placeholder := "Filter graphs…",
                    onMountFocus,
                    value <-- filterVar,
                    onInput.mapToValue --> filterVar,
                  ),
                )
              else Seq.empty
            val list = div(
              cls := Styles.graphSelectorMenuList,
              children <-- filterVar.signal.map { rawQuery =>
                val query = rawQuery.trim.toLowerCase
                // Default graph pinned to the top, the rest alphabetical
                // (case-insensitive) so the list order stays stable as graphs
                // are added or removed.
                val visible = known
                  .filter(_.toLowerCase.contains(query))
                  .sortBy(n => (!defaultNamespace.contains(n), n.toLowerCase))
                if (known.isEmpty) Seq(div(cls := Styles.graphSelectorMenuEmpty, "No graphs"))
                else if (visible.isEmpty) Seq(div(cls := Styles.graphSelectorMenuEmpty, "No matching graph"))
                else visible.map(selectItem)
              },
            )
            (header +: filter) ++ Seq(list)
        },
      ),
    )
  }
}
