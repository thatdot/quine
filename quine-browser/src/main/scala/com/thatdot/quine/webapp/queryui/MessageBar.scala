package com.thatdot.quine.webapp.queryui

import scala.scalajs.js.Dynamic.{literal => jsObj}

import org.scalajs.dom
import org.scalajs.dom.{document, html, window}
import slinky.core.Component
import slinky.core.annotations.react
import slinky.core.facade.{React, ReactElement, ReactRef}
import slinky.web.html._

import com.thatdot.quine.webapp.Styles

/** Message to present to the user
  *
  * @param content body of the message
  * @param color background color (indicative of the sentiment of the message)
  */
final case class MessageBarContent(
  content: ReactElement,
  color: String
)

/** Message bar that pops up from the bottom of the screen
  *
  * Interactive component based on <https://stackoverflow.com/a/20927899/3072788>
  */
@react class MessageBar extends Component {

  case class Props(
    message: MessageBarContent,
    closeMessageBox: () => Unit
  )

  /** @param draggingYCoord is user is in the process of expanding the bar, this is the Y-coord of the bottom of the bar
    * @param draggedHeight height to which the user dragged the bar
    * @param autoScrollToBottom should the scroll position track new content being appended?
    */
  case class State(
    draggingYCoord: Option[Double],
    draggedHeight: Option[Double],
    autoScrollToBottom: Boolean
  )

  def initialState: State = State(draggingYCoord = None, draggedHeight = None, autoScrollToBottom = false)

  def onMouseDown_(e: dom.MouseEvent, elem: ReactRef[html.Div]): Unit =
    if (e.button == 0) { // only if it was a "left" mouse button down event
      setState(
        _.copy(draggingYCoord = Some(elem.current.getBoundingClientRect().bottom + window.pageYOffset))
      )
      e.stopPropagation()
      e.preventDefault()
    }

  def onMouseUp(e: dom.MouseEvent): Unit = {
    setState(_.copy(draggingYCoord = None))
    e.stopPropagation()
    e.preventDefault()
  }

  // Update the position of the top of the message bar if it changed
  def onMouseMove(e: dom.MouseEvent): Unit =
    for (yCoord <- state.draggingYCoord) {
      setState(_.copy(draggedHeight = Some(yCoord - e.pageY)))
      e.stopPropagation()
      e.preventDefault()
    }

  // Based on whether the user is at the bottom of the results, toggle `autoScrollToBottom`
  def onContentScroll(): Unit = {
    val contentDiv = contentRef.current
    val atBottom = contentDiv.offsetHeight + contentDiv.scrollTop + 5 >= contentDiv.scrollHeight
    setState(s => if (s.autoScrollToBottom == atBottom) s else s.copy(autoScrollToBottom = atBottom))
  }

  override def componentDidUpdate(prevProps: Props, prevState: State): Unit = {

    // If the user just clicked/released the top of the bar, register/unregister handlers
    if (prevState.draggingYCoord.nonEmpty && state.draggingYCoord.isEmpty) {
      document.removeEventListener("mousemove", onMouseMove(_))
      document.removeEventListener("mouseup", onMouseUp(_))
    } else if (prevState.draggingYCoord.isEmpty && state.draggingYCoord.nonEmpty) {
      document.addEventListener("mousemove", onMouseMove(_))
      document.addEventListener("mouseup", onMouseUp(_))
    }

    // If auto-scrolling, update the scroll position
    if (state.autoScrollToBottom) scrollToBottom()
  }

  // Scroll to the bottom of the the message bar content
  def scrollToBottom(): Unit = {
    val contentDiv = contentRef.current
    contentDiv.scrollTop = contentDiv.scrollHeight.toDouble
  }

  val fullBarRef: ReactRef[html.Div] = React.createRef[html.Div]
  val contentRef: ReactRef[html.Div] = React.createRef[html.Div]

  def render(): ReactElement = {

    val barStyle = jsObj(
      height = state.draggedHeight.fold("20%")(x => s"${x}px"),
      backgroundColor = props.message.color
    )

    div(style := barStyle, className := Styles.messageBar, ref := fullBarRef)(
      div(
        key := "message-bar-content",
        ref := contentRef,
        style := jsObj(
          overflowY = "scroll",
          height = "100%",
          width = "calc(100% - 0.8em)",
          padding = "0.4em",
          position = "absolute"
        ),
        onScroll := (_ => onContentScroll())
      )(
        props.message.content
      ),
      div(
        key := "message-bar-resize-handle",
        style := jsObj(
          position = "absolute",
          width = "100%",
          height = "3px",
          backgroundColor = "black",
          cursor = "ns-resize"
        ),
        onMouseDown := (e => onMouseDown_(e.nativeEvent, fullBarRef))
      ),
      div(
        key := "message-bar-close",
        className := Styles.messageBarButton,
        style := jsObj(display = "block")
      )(
        if (state.autoScrollToBottom) {
          i(
            key := "message-bar-scroll-to-top",
            className := "ion-ios-arrow-up",
            title := "Scroll to the top of results",
            onClick := { _ =>
              contentRef.current.scrollTop = 0
              setState(_.copy(autoScrollToBottom = false))
            }
          )
        } else {
          i(
            key := "message-bar-scroll-to-bottom",
            className := "ion-ios-arrow-down",
            title := "Scroll to the bottom of results",
            onClick := { _ =>
              scrollToBottom()
              setState(_.copy(autoScrollToBottom = true))
            }
          )
        },
        i(
          key := "message-bar-close",
          className := "ion-ios-close-outline",
          title := "Close message box",
          style := jsObj(marginLeft = "0.2em"),
          onClick := (_ => props.closeMessageBox())
        )
      )
    )
  }
}
