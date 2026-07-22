package com.thatdot.quine.webapp.components

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

sealed abstract class ToastVariant(val cssClass: String, val iconClass: String)
object ToastVariant {
  case object Info extends ToastVariant("info", "cil-info-circle")
  case object Success extends ToastVariant("success", "cil-check-circle")
  case object Error extends ToastVariant("error", "cil-warning")
}

/** An optional one-click affordance on a toast — a labelled button next to the message,
  * e.g. "Run as text query" on the "valid query, non-node results" notice. Clicking it runs
  * `run` and dismisses the toast.
  */
final case class ToastAction(label: String, run: () => Unit)

final case class ToastMessage(
  text: String,
  variant: ToastVariant = ToastVariant.Info,
  action: Option[ToastAction] = None,
)

object Toast {

  /** Errors linger longer than the confirmation blips — they carry text the user
    * actually has to read (compile errors, server failures).
    */
  private val ErrorDurationMs = 30000

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
            i(cls := s"${msg.variant.iconClass} ${Styles.toastIcon}"),
            span(msg.text),
            msg.action
              .map { act =>
                button(
                  cls := Styles.toastAction,
                  act.label,
                  onClick --> { _ =>
                    cancelTimer()
                    messageVar.set(None)
                    act.run()
                  },
                )
              }
              .getOrElse(emptyNode),
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
                // A toast the user is meant to act on must outlast the confirmation blips —
                // give it (and errors) the long dwell so there's time to read and click.
                val d =
                  if (msg.variant == ToastVariant.Error || msg.action.isDefined) ErrorDurationMs
                  else durationMs
                timerHandle = Some(js.timers.setTimeout(d.toDouble) {
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
