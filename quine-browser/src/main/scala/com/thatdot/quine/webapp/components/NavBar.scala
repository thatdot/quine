package com.thatdot.quine.webapp.components

import scala.scalajs.js

import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

/** Item in a [[NavBar]] */
final case class NavItem(
  title: String,
  body: facade.ReactElement
)

/** Navigation bar that sits on the LHS of the window */
@react class NavBar extends Component {

  case class Props(
    initiallySelected: String,
    children: NavItem*
  )
  case class State(selected: String)

  def initialState: com.thatdot.quine.webapp.components.NavBar.State = State(props.initiallySelected)

  // TODO: pull CSS out into proper consistent classes
  private val menuStyle = js.Dynamic.literal(
    position = "fixed",
    bottom = "0",
    top = "0",
    left = "0",
    width = "160px",
    backgroundColor = "#ddd"
  )
  private val bodyStyle = js.Dynamic.literal(
    marginLeft = "170px"
  )
  private val listStyle = js.Dynamic.literal(
    listStyleType = "none",
    padding = "0",
    margin = "0",
    fontSize = "large"
  )
  private val itemStyle = js.Dynamic.literal(
    padding = "0.5em",
    cursor = "pointer",
    backgroundColor = "#ddd",
    transition = "background-color 0.2s"
  )
  private val selecteditemStyle = js.Dynamic.literal(
    padding = "0.5em",
    fontWeight = "bold",
    backgroundColor = "#bbb",
    transition = "background-color 0.2s"
  )

  def render(): ReactElement = {
    val elements = ul(style := listStyle)(
      props.children.map { nav: NavItem =>
        if (state.selected == nav.title) {
          li(style := selecteditemStyle)(nav.title)
        } else {
          val clickItem = () => setState(State(nav.title));
          li(style := itemStyle, onClick := clickItem)(nav.title)
        }
      }: _*
    )

    val body: facade.ReactElement = props.children.toSeq
      .find(_.title == state.selected)
      .map(_.body)
      .getOrElse("Not found")

    facade.Fragment(
      div(style := menuStyle)(elements),
      div(style := bodyStyle)(body)
    )
  }
}
