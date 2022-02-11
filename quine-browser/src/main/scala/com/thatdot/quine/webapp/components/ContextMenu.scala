package com.thatdot.quine.webapp.components

import scala.scalajs.js.Dynamic.{literal => jsObj}

import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

import com.thatdot.quine.webapp.Styles

/** Entry in a context menu
  *
  * @param item menu item
  * @param title tooltip on item hover
  * @param action what to do if the item gets clicked?
  *
  * @note the action should pretty much always start by closing the whole menu
  */
final case class ContextMenuItem(
  item: ReactElement,
  title: String,
  action: () => Unit
)

/** Context menu */
@react object ContextMenu {

  /** @param x x-coordinate of clicked page location
    * @param y y-coordinate of clicked page location
    * @param items what to put in the context menu
    */
  case class Props(
    x: Double,
    y: Double,
    items: Seq[ContextMenuItem]
  )

  val component: FunctionalComponent[ContextMenu.Props] = FunctionalComponent[Props] { props =>
    val menuStyle = jsObj(top = props.y + "px", left = props.x + "px")
    div(style := menuStyle, className := Styles.contextMenu)(
      ul(
        props.items.map { case ContextMenuItem(item, itemTitle, action) =>
          li(
            className := Styles.grayClickable,
            title := itemTitle,
            onClick := (_ => action())
          )(item)
        }: _*
      )
    )
  }

}
