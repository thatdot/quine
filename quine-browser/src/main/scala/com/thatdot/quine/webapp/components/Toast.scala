package com.thatdot.quine.webapp.components

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

sealed abstract class ToastVariant(val cssClass: String)
object ToastVariant {
  case object Info extends ToastVariant("info")
  case object Success extends ToastVariant("success")
  case object Error extends ToastVariant("error")
}

final case class ToastMessage(text: String, variant: ToastVariant = ToastVariant.Info)

object Toast {

  def apply(messageVar: Var[Option[ToastMessage]], durationMs: Int = 3000): HtmlElement = {
    var timerHandle: Option[js.timers.SetTimeoutHandle] = None

    def cancelTimer(): Unit = {
      timerHandle.foreach(js.timers.clearTimeout)
      timerHandle = None
    }

    div(
      child <-- messageVar.signal.map {
        case Some(msg) =>
          cancelTimer()
          div(
            cls := s"${Styles.toast} ${Styles.toast}-${msg.variant.cssClass}",
            span(msg.text),
            button(
              cls := Styles.toastClose,
              "×",
              onClick --> { _ =>
                cancelTimer()
                messageVar.set(None)
              },
            ),
            onMountUnmountCallback(
              mount = { _ =>
                timerHandle = Some(js.timers.setTimeout(durationMs.toDouble) {
                  timerHandle = None
                  messageVar.set(None)
                })
              },
              unmount = { _ => cancelTimer() },
            ),
          )
        case None =>
          cancelTimer()
          emptyNode
      },
    )
  }
}
