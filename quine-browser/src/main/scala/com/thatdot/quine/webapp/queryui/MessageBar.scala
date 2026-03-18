package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.{document, window}

import com.thatdot.quine.webapp.Styles

/** Message to present to the user
  *
  * @param content body of the message
  * @param colorClass CSS class for background color sentiment (e.g. "query-result-error")
  */
final case class MessageBarContent(
  content: HtmlElement,
  colorClass: String,
)

/** Message bar that pops up from the bottom of the screen
  *
  * Interactive component based on <https://stackoverflow.com/a/20927899/3072788>
  */
object MessageBar {

  def apply(
    message: MessageBarContent,
    closeMessageBox: () => Unit,
  ): HtmlElement = {
    val draggingYCoordVar = Var(Option.empty[Double])
    val draggedHeightVar = Var(Option.empty[Double])
    val autoScrollToBottomVar = Var(false)

    var fullBarEl: Option[dom.html.Div] = None
    var contentEl: Option[dom.html.Div] = None

    def scrollToBottom(): Unit =
      for (el <- contentEl)
        el.scrollTop = el.scrollHeight.toDouble

    val onMouseMove: js.Function1[dom.MouseEvent, Unit] = (e: dom.MouseEvent) => {
      if (draggingYCoordVar.now().isDefined) {
        for (yCoord <- draggingYCoordVar.now())
          draggedHeightVar.set(Some(yCoord - e.pageY))
        e.stopPropagation()
        e.preventDefault()
      }
    }

    val onMouseUp: js.Function1[dom.MouseEvent, Unit] = (e: dom.MouseEvent) => {
      draggingYCoordVar.set(None)
      e.stopPropagation()
      e.preventDefault()
    }

    div(
      cls := Styles.messageBar,
      cls := message.colorClass,
      styleAttr <-- draggedHeightVar.signal.map { dh =>
        val h = dh.fold("20%")(x => s"${x}px")
        s"height: $h;"
      },
      onMountCallback { ctx =>
        fullBarEl = Some(ctx.thisNode.ref)
      },
      onUnmountCallback { _ =>
        // Clean up document-level listeners if still dragging
        document.removeEventListener("mousemove", onMouseMove)
        document.removeEventListener("mouseup", onMouseUp)
        fullBarEl = None
        contentEl = None
      },
      // Observe dragging state changes to register/unregister document-level handlers
      draggingYCoordVar.signal.updates --> { opt =>
        if (opt.isDefined) {
          document.addEventListener("mousemove", onMouseMove)
          document.addEventListener("mouseup", onMouseUp)
        } else {
          document.removeEventListener("mousemove", onMouseMove)
          document.removeEventListener("mouseup", onMouseUp)
        }
      },
      // Auto-scroll when enabled
      autoScrollToBottomVar.signal --> { autoScroll =>
        if (autoScroll) scrollToBottom()
      },
      // Content div
      div(
        overflowY := "scroll",
        height := "100%",
        width := "calc(100% - 0.8em)",
        padding := "0.4em",
        position := "absolute",
        onMountCallback { ctx =>
          contentEl = Some(ctx.thisNode.ref)
        },
        onScroll --> { _ =>
          for (el <- contentEl) {
            val atBottom = el.offsetHeight + el.scrollTop + 5 >= el.scrollHeight
            if (autoScrollToBottomVar.now() != atBottom)
              autoScrollToBottomVar.set(atBottom)
          }
        },
        message.content,
      ),
      // Resize handle
      div(
        cls := Styles.messageBarResizeHandle,
        position := "absolute",
        width := "100%",
        height := "3px",
        cursor := "ns-resize",
        onMouseDown --> { e =>
          if (e.button == 0) {
            for (bar <- fullBarEl)
              draggingYCoordVar.set(Some(bar.getBoundingClientRect().bottom + window.pageYOffset))
            e.stopPropagation()
            e.preventDefault()
          }
        },
      ),
      // Close / scroll buttons
      div(
        cls := Styles.messageBarButton,
        display := "block",
        child <-- autoScrollToBottomVar.signal.map { autoScroll =>
          if (autoScroll)
            htmlTag("i")(
              cls := "ion-ios-arrow-up",
              title := "Scroll to the top of results",
              onClick --> { _ =>
                for (el <- contentEl) el.scrollTop = 0
                autoScrollToBottomVar.set(false)
              },
            )
          else
            htmlTag("i")(
              cls := "ion-ios-arrow-down",
              title := "Scroll to the bottom of results",
              onClick --> { _ =>
                scrollToBottom()
                autoScrollToBottomVar.set(true)
              },
            )
        },
        htmlTag("i")(
          cls := "ion-ios-close-outline",
          title := "Close message box",
          marginLeft := "0.2em",
          onClick --> { _ => closeMessageBox() },
        ),
      ),
    )
  }
}
