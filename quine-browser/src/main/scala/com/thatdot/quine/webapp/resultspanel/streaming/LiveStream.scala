package com.thatdot.quine.webapp.resultspanel.streaming

import scala.collection.mutable
import scala.scalajs.js.timers.{SetIntervalHandle, clearInterval, setInterval}

import com.raquo.airstream.ownership.Owner
import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.routes.CypherQueryResult

/** An `Owner` whose subscriptions can be torn down on demand (the stream session isn't a
  * Laminar element, so it owns its record subscription manually).
  */
final private class KillableOwner extends Owner {
  def killAll(): Unit = killSubscriptions()
}

/** Source-agnostic live record buffer for one streaming source.
  *
  * Fed by an injected `EventStream[Json]` of frames — the *producer* owns the transport
  * (socket, reconnect, tap config); this only buffers and shapes for display. Frames are
  * batched on a throttle tick so high rates never thrash Laminar. The column set is the
  * union of `data` keys (first-seen order, can grow mid-stream). [[freeze]] detaches from
  * the stream and stops consuming, leaving the buffer as a static snapshot.
  */
final class LiveStream {
  val rows: Var[Vector[StreamRow]] = Var(Vector.empty)
  val columns: Var[Vector[String]] = Var(Vector.empty)

  private var seq = 0L
  private val pending = mutable.ArrayBuffer.empty[StreamRow]
  private var interval: Option[SetIntervalHandle] = None
  private val streamOwner = new KillableOwner
  private var started = false

  /** Begin consuming a producer's frame stream. Call once per session. */
  def connect(records: EventStream[Json]): Unit = if (!started) {
    started = true
    val _ = records.foreach(onFrame)(streamOwner)
    interval = Some(setInterval(LiveStream.throttleMs)(tick()))
  }

  private def onFrame(json: Json): Unit =
    json.as[StandingTapFrame].toOption.foreach { frame =>
      seq += 1
      pending += StreamRow(seq, frame.isPositiveMatch, frame.data)
    }

  private def appendRows(newRows: Seq[StreamRow]): Unit = if (newRows.nonEmpty) {
    val existing = columns.now()
    val seen = existing.toSet
    val added = mutable.LinkedHashSet.empty[String]
    newRows.foreach(_.columnKeys.foreach(k => if (!seen.contains(k)) added += k))
    if (added.nonEmpty) columns.set(existing ++ added)
    val combined = rows.now() ++ newRows
    rows.set(if (combined.size > LiveStream.maxRows) combined.takeRight(LiveStream.maxRows) else combined)
  }

  private def tick(): Unit =
    if (pending.nonEmpty) {
      val batch = pending.toVector
      pending.clear()
      appendRows(batch)
    }

  /** Stop consuming and detach from the producer's stream, leaving the buffer frozen as a
    * snapshot. The producer's tap is stopped separately, via `LiveSource.stop`.
    */
  def freeze(): Unit = {
    interval.foreach(clearInterval)
    interval = None
    streamOwner.killAll()
  }

  /** Point-in-time snapshot of the buffer as a tabular result (for export / compare),
    * aligning each row's data to the current column set.
    */
  def toCypherResult: CypherQueryResult = {
    val cols = columns.now()
    val rs = rows.now().map(row => cols.map(col => row.fields.getOrElse(col, Json.Null)))
    CypherQueryResult(cols, rs)
  }
}

object LiveStream {
  private val throttleMs: Double = 100
  private val maxRows = 5000
}
