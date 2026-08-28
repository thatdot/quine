package com.thatdot.quine.app.model.jobs

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

import org.apache.pekko.actor.{Cancellable, Scheduler}

import io.circe.Json
import io.circe.syntax._

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.model.outputs2.query.standing.StandingQueryResultWorkflow.queryContextFoldableFrom
import com.thatdot.quine.app.model.outputs2.query.standing.TapBus
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.model.Milliseconds

/** Relays one background-query execution's result rows to wiretap subscribers on the [[TapBus]],
  * best-effort. While no subscriber is attached, the *first* [[BackgroundQueryTapRelay.MaxBufferedRows]]
  * rows are buffered (later ones counted as dropped) and retained until
  * [[BackgroundQueryTapRelay.GraceWindow]] past termination; on the first subscriber observed the
  * buffer flushes in order and subsequent rows publish live with standard drop-on-slow semantics.
  * Frames are the folded JSON rows, followed after termination by one completion frame
  * ([[BackgroundQueryTapRelay.CompletionFrameKey]]) carrying the terminal status and counts, so a
  * consumer can render "showing N of M". A run cancelled before any subscriber was observed
  * discards its buffer: cancel disclaims the results. Subscriber presence is checked per offered
  * row and by a periodic tick (which also resolves the grace window); publish failures are
  * swallowed and the relay never fails the query stream.
  */
final class BackgroundQueryTapRelay(
  tapBus: TapBus,
  topic: String,
  scheduler: Scheduler,
)(implicit ec: ExecutionContext) {

  import BackgroundQueryTapRelay._

  private[this] var live = false // a subscriber has been observed; rows publish directly
  private[this] var closed = false // sentinel sent, or buffer dropped: the relay is inert
  private[this] var sentinel: Option[Json] = None // set at terminal; published once flushed/live
  private[this] var graceDeadlineMillis = 0L
  private[this] val buffered = mutable.ArrayBuffer.empty[QueryContext]
  private[this] var droppedRows = 0L

  private[this] val tick: Cancellable =
    scheduler.scheduleWithFixedDelay(TickInterval, TickInterval)(() => check())

  /** Offer one result row (called from the query stream; never throws, never backpressures). */
  def offer(row: QueryContext): Unit = synchronized {
    if (closed) ()
    else if (live) publishRow(row)
    else if (tapBus.hasSubscribers(topic)) {
      goLive()
      publishRow(row)
    } else if (buffered.size < MaxBufferedRows) { buffered += row; () }
    else droppedRows += 1
  }

  /** The run reached `terminal`: a live tap gets the completion frame immediately; an unobserved
    * buffer waits out the grace window (except a cancelled run's, discarded on the spot).
    */
  def complete(terminal: ExecutionAction): Unit = synchronized {
    if (closed) ()
    else
      terminal match {
        case ExecutionAction.Cancelled() if !live => close()
        case _ =>
          sentinel = Some(completionFrame(terminal))
          if (live) {
            publishSentinel()
            close()
          } else graceDeadlineMillis = Milliseconds.currentTime().millis + GraceWindow.toMillis
      }
  }

  /** Periodic check: flush to a newly-observed subscriber, and resolve the grace window. */
  private def check(): Unit = synchronized {
    if (closed) ()
    else if (!live && tapBus.hasSubscribers(topic)) {
      goLive()
      if (sentinel.isDefined) {
        publishSentinel()
        close()
      }
    } else if (sentinel.isDefined && Milliseconds.currentTime().millis > graceDeadlineMillis)
      close() // grace lapsed with no subscriber: drop the buffer
  }

  private def goLive(): Unit = {
    live = true
    buffered.foreach(publishRow)
    buffered.clear()
  }

  private def publishRow(row: QueryContext): Unit = {
    val _ = Try(tapBus.publish(topic, row)(queryContextFoldableFrom)) // best-effort
  }

  private def publishSentinel(): Unit =
    sentinel.foreach { json =>
      val _ = Try(tapBus.publish(topic, json)(DataFoldableFrom.jsonDataFoldable)) // best-effort
    }

  private def completionFrame(terminal: ExecutionAction): Json = {
    val (status, totalRowCount, error) = terminal match {
      case ExecutionAction.Completed(count, _) => ("completed", Some(count), None)
      case ExecutionAction.Failed(err) => ("failed", None, Some(err))
      case ExecutionAction.Cancelled() => ("cancelled", None, None)
      case ExecutionAction.Interrupted() => ("interrupted", None, None) // reconciled state; never passed here
      case ExecutionAction.Started() => ("completed", None, None) // not a terminal; never passed
    }
    BackgroundQueryTapRelay.completionFrame(status, totalRowCount, droppedRows, error)
  }

  private def close(): Unit = {
    closed = true
    buffered.clear()
    val _ = tick.cancel()
    // Producer done: end the subscriber streams. Any final frame published just above is delivered
    // first (draining), then the socket closes. A tap that arrives after this synthesizes its own
    // final frame from the durable record.
    tapBus.complete(topic)
  }
}

object BackgroundQueryTapRelay {

  /** JSON key of the tap's final frame; row frames are plain JSON objects keyed by column name. */
  val CompletionFrameKey: String = "__backgroundQueryComplete"

  /** Build the tap's terminal completion frame. Shared by the relay's live path and the tap endpoint,
    * which synthesizes one from the status record for a tap that connects after the relay is gone —
    * so both emit an identically-shaped final frame.
    */
  def completionFrame(
    status: String,
    totalRowCount: Option[Long],
    droppedBufferedRows: Long,
    error: Option[String],
  ): Json =
    Json.obj(
      CompletionFrameKey := Json.obj(
        "status" := status,
        "totalRowCount" := totalRowCount,
        "droppedBufferedRows" := droppedBufferedRows,
        "error" := error,
      ),
    )

  /** Most rows held for a not-yet-connected subscriber (the first rows; overflow is counted in the
    * completion frame's `droppedBufferedRows`).
    */
  val MaxBufferedRows: Int = 1024

  /** How long past termination an unobserved buffer waits for a subscriber before being dropped. */
  val GraceWindow: FiniteDuration = 60.seconds

  /** Cadence of the subscriber check that flushes the buffer and resolves the grace window. */
  val TickInterval: FiniteDuration = 250.millis
}
