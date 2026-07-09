package com.thatdot.quine.webapp.components

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles

final case class ContextMenuItem(
  item: Modifier[HtmlElement],
  title: String,
  action: () => Unit,
)

final case class MenuAction(
  name: String,
  suffix: String = "",
  icon: String = "",
  action: () => Unit,
)

final case class MenuSection(
  label: String,
  actions: Vector[MenuAction],
)

final case class ContextMenuModel(
  header: Option[Modifier[HtmlElement]],
  sections: Vector[MenuSection],
  primarySectionIndex: Option[Int] = None,
) {
  def firstAction: Option[MenuAction] =
    sections.iterator.flatMap(_.actions).nextOption()

  def firstQueryAction: Option[MenuAction] =
    primarySectionIndex
      .flatMap(sections.lift)
      .flatMap(_.actions.headOption)
      .orElse(firstAction)
}

object ContextMenuModel {
  def actionsOnly(actions: Vector[MenuAction]): ContextMenuModel =
    ContextMenuModel(None, Vector(MenuSection("", actions)))
}

object ContextMenu {

  private val ScrollThreshold = 5
  private val SearchThreshold = 8

  def apply(x: Double, y: Double, items: Seq[ContextMenuItem]): HtmlElement =
    div(
      top := s"${y}px",
      left := s"${x}px",
      cls := Styles.contextMenu,
      ul(
        items.map { menuItem =>
          li(
            cls := Styles.grayClickable,
            title := menuItem.title,
            onClick --> (_ => menuItem.action()),
            menuItem.item,
          )
        },
      ),
    )

  def fromModel(x: Double, y: Double, model: ContextMenuModel): HtmlElement =
    div(
      top := s"${y}px",
      left := s"${x}px",
      cls := Styles.contextMenu,
      onMountCallback { ctx =>
        clampToViewport(ctx.thisNode.ref)
      },
      model.header
        .map(content =>
          div(
            cls := Styles.contextMenuHeader,
            cursor := "grab",
            onMouseDown --> { e =>
              e.preventDefault()
              val el = e.currentTarget.asInstanceOf[dom.HTMLElement].parentElement
              val startX = e.clientX
              val startY = e.clientY
              val startLeft = el.offsetLeft.toDouble
              val startTop = el.offsetTop.toDouble

              val onMove: js.Function1[dom.MouseEvent, Unit] = { (me: dom.MouseEvent) =>
                val pad = 8.0
                val vw = dom.window.innerWidth
                val vh = dom.window.innerHeight
                val sidebarRight = Option(dom.document.querySelector(".sidebar"))
                  .map(_.getBoundingClientRect().right)
                  .filter(_ > 0)
                  .getOrElse(pad)
                val rect = el.getBoundingClientRect()
                val parentRect = Option(el.offsetParent).map(_.getBoundingClientRect())
                val parentLeft = parentRect.fold(0.0)(_.left)
                val parentTop = parentRect.fold(0.0)(_.top)

                val rawLeft = startLeft + me.clientX - startX
                val rawTop = startTop + me.clientY - startY
                val viewLeft = rawLeft + parentLeft
                val viewTop = rawTop + parentTop

                val clampedLeft = math.max(sidebarRight, math.min(viewLeft, vw - rect.width - pad))
                val clampedTop = math.max(pad, math.min(viewTop, vh - rect.height - pad))

                el.style.left = s"${clampedLeft - parentLeft}px"
                el.style.top = s"${clampedTop - parentTop}px"
              }
              lazy val onUp: js.Function1[dom.MouseEvent, Unit] = { (_: dom.MouseEvent) =>
                dom.document.removeEventListener("mousemove", onMove)
                dom.document.removeEventListener("mouseup", onUp)
              }
              dom.document.addEventListener("mousemove", onMove)
              dom.document.addEventListener("mouseup", onUp)
            },
            content,
          ),
        )
        .getOrElse(emptyNode),
      div(
        flex := "1",
        minHeight := "0",
        overflowY := "auto",
        overflowX := "hidden",
        model.sections.zipWithIndex.map { case (section, idx) =>
          val needsDivider = model.header.isDefined || idx > 0
          div(
            if (needsDivider) div(cls := Styles.contextMenuDivider) else emptyNode,
            if (section.label.nonEmpty) div(cls := Styles.contextMenuSection, section.label) else emptyNode,
            renderSectionBody(section),
          )
        },
      ),
    )

  private def renderSectionBody(section: MenuSection): HtmlElement = {
    val count = section.actions.length
    if (count > SearchThreshold) renderSearchableSection(section)
    else if (count > ScrollThreshold) renderScrollableSection(section.actions)
    else renderPlainSection(section.actions)
  }

  private def renderPlainSection(actions: Vector[MenuAction]): HtmlElement =
    div(actions.map(renderAction))

  private def renderScrollableSection(actions: Vector[MenuAction]): HtmlElement =
    div(
      cls := s"${Styles.contextMenuSectionBody} scrollable",
      actions.map(renderAction),
    )

  private def renderSearchableSection(section: MenuSection): HtmlElement = {
    val searchVar = Var("")
    val filtered: Signal[Vector[MenuAction]] = searchVar.signal.map { needle =>
      val n = needle.trim.toLowerCase
      if (n.isEmpty) section.actions
      else
        section.actions.filter { ma =>
          ma.name.toLowerCase.contains(n) || ma.suffix.toLowerCase.contains(n)
        }
    }
    div(
      div(
        cls := Styles.contextMenuSectionSearch,
        htmlTag("i")(cls := "ion-ios-search"),
        input(
          typ := "text",
          placeholder := s"Search ${section.label.toLowerCase}…",
          controlled(value <-- searchVar, onInput.mapToValue --> searchVar),
          onMountFocus,
          onMouseDown --> (_.stopPropagation()),
        ),
      ),
      div(
        cls := s"${Styles.contextMenuSectionBody} scrollable",
        children <-- filtered.map(_.map(renderAction)),
      ),
    )
  }

  private def renderAction(action: MenuAction): HtmlElement =
    button(
      cls := Styles.contextMenuAction,
      role := "menuitem",
      onClick --> (_ => action.action()),
      if (action.icon.nonEmpty)
        span(cls := Styles.contextMenuActionIcon, action.icon)
      else emptyNode,
      div(
        cls := Styles.contextMenuActionBody,
        div(cls := Styles.contextMenuActionName, action.name),
        if (action.suffix.nonEmpty)
          div(cls := Styles.contextMenuActionSuffix, action.suffix)
        else emptyNode,
      ),
    )

  private def clampToViewport(el: dom.HTMLElement): Unit = {
    val pad = 8.0
    val vw = dom.window.innerWidth
    val vh = dom.window.innerHeight
    val sidebarRight = Option(dom.document.querySelector(".sidebar"))
      .map(_.getBoundingClientRect().right)
      .filter(_ > 0)
      .getOrElse(pad)

    val parentRect = Option(el.offsetParent).map(_.getBoundingClientRect())
    val parentLeft = parentRect.fold(0.0)(_.left)
    val parentTop = parentRect.fold(0.0)(_.top)

    val rect = el.getBoundingClientRect()
    if (rect.right > vw - pad) {
      val newLeft = math.max(sidebarRight, vw - rect.width - pad) - parentLeft
      el.style.left = s"${newLeft}px"
    }
    if (rect.left < sidebarRight) {
      el.style.left = s"${sidebarRight - parentLeft}px"
    }
    if (rect.bottom > vh - pad) {
      val newTop = math.max(pad, vh - rect.height - pad) - parentTop
      el.style.top = s"${newTop}px"
    }
  }
}
