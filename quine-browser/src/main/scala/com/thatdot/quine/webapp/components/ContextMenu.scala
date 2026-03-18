package com.thatdot.quine.webapp.components

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** Entry in a context menu
  *
  * @param item menu item content
  * @param title tooltip on item hover
  * @param action what to do if the item gets clicked
  * @note the action should pretty much always start by closing the whole menu
  */
final case class ContextMenuItem(
  item: Modifier[HtmlElement],
  title: String,
  action: () => Unit,
)

/** Context menu */
object ContextMenu {

  /** @param x x-coordinate of clicked page location
    * @param y y-coordinate of clicked page location
    * @param items what to put in the context menu
    */
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
}
