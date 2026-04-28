package com.thatdot.quine.webapp.components.streams

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import com.raquo.laminar.api.L._

/** Submit-state tracker shared by the create forms and the inline output form.
  *
  * Encapsulates the `error` + `submitting` Vars that every form needs and the
  * three transitions: begin a submission, succeed, fail. `run(...)` wires the
  * three together for a typical async POST.
  */
final class SubmitState {
  val error: Var[Option[String]] = Var(Option.empty[String])
  val submitting: Var[Boolean] = Var(false)

  def reset(): Unit = {
    error.set(None)
    submitting.set(false)
  }

  /** Run an async submission; flip `submitting` while it's in flight, capture the
    * error message (or clear it) when it settles, and only invoke `onSuccess` for
    * a `Right`.
    */
  def run[A](
    attempt: => Future[Either[String, A]],
  )(onSuccess: A => Unit)(implicit ec: ExecutionContext): Unit = {
    error.set(None)
    submitting.set(true)
    attempt.onComplete {
      case Success(Right(value)) =>
        error.set(None)
        submitting.set(false)
        onSuccess(value)
      case Success(Left(err)) =>
        error.set(Some(err))
        submitting.set(false)
      case Failure(ex) =>
        error.set(Some(s"Unexpected error: ${ex.getMessage}"))
        submitting.set(false)
    }
  }
}

/** Standard idle/spinner submit button shared by the create forms. */
object FormSubmit {

  /** @param canSubmit  signal driving the enabled-when-true state (typically
    *                   "name field is non-empty"). Combined with `state.submitting`
    *                   so an in-flight submission also disables the button.
    * @param sizeSmall  when true, renders with `btn-sm` for inline placement.
    */
  def submitButton(
    idleLabel: String,
    busyLabel: String,
    state: SubmitState,
    canSubmit: Signal[Boolean],
    sizeSmall: Boolean = false,
  )(onSubmit: () => Unit): HtmlElement = {
    val sizeCls = if (sizeSmall) " btn-sm" else ""
    button(
      cls := s"btn btn-primary$sizeCls",
      disabled <-- canSubmit.combineWith(state.submitting.signal).map { case (can, busy) =>
        !can || busy
      },
      child <-- state.submitting.signal.map { busy =>
        if (busy) span(span(cls := "spinner-border spinner-border-sm me-1"), busyLabel)
        else span(idleLabel)
      },
      onClick --> { _ => onSubmit() },
    )
  }
}
