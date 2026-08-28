package com.thatdot.quine.webapp.dataservice

import scala.collection.mutable

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.v2api.V2Paths

/** Lifecycle of one background-query tap, as the client observes it.
  *
  * Distinct from [[WiretapStatus]] in one way that matters: a background query's stream is
  * finite, and [[BqTapStatus.Ended]] is reached by the server *telling us so* (the completion
  * frame) rather than by the socket dropping. It is terminal — see
  * [[BackgroundQueryTapStore]]'s class doc on why an ended tap is never reopened.
  */
sealed trait BqTapStatus
object BqTapStatus {
  case object Connecting extends BqTapStatus
  case object Live extends BqTapStatus

  /** The run finished. `completion` is the server's final frame when we saw it; `None` when the
    * socket closed without one (the run outlived its relay, or the connection dropped).
    */
  final case class Ended(completion: Option[BackgroundQueryCompletion]) extends BqTapStatus
  final case class Error(msg: String) extends BqTapStatus
}

/** The tap's final frame: `{"__backgroundQueryComplete": {status, totalRowCount,
  * droppedBufferedRows, error}}`.
  *
  * Mirrors `BackgroundQueryTapRelay.completionFrame` server-side. `droppedBufferedRows` counts
  * rows the relay discarded because nothing was listening fast enough — the tap is best-effort
  * and drop-on-slow, so a non-zero count means the table on screen is not the whole result.
  *
  * @see [[public/quine/src/main/scala/com/thatdot/quine/app/model/jobs/BackgroundQueryTapRelay.scala]]
  */
final case class BackgroundQueryCompletion(
  status: String,
  totalRowCount: Option[Long],
  droppedBufferedRows: Long,
  error: Option[String],
)

object BackgroundQueryCompletion {

  /** The key the server wraps its completion frame in. Deliberately `__`-prefixed so it cannot
    * collide with a Cypher column name.
    */
  val FrameKey: String = "__backgroundQueryComplete"

  /** `Some` iff `json` is the completion frame; every result row yields `None`.
    *
    * A frame carrying the key but a malformed body still matches (with defaulted fields): it is
    * unambiguously the terminator, and treating it as a data row instead would put
    * `__backgroundQueryComplete` in the results table.
    */
  def unapply(json: Json): Option[BackgroundQueryCompletion] =
    json.hcursor.downField(FrameKey).focus.map { body =>
      val c = body.hcursor
      BackgroundQueryCompletion(
        status = c.downField("status").as[String].getOrElse("completed"),
        totalRowCount = c.downField("totalRowCount").as[Option[Long]].toOption.flatten,
        droppedBufferedRows = c.downField("droppedBufferedRows").as[Long].getOrElse(0L),
        error = c.downField("error").as[Option[String]].toOption.flatten,
      )
    }
}

/** One consumer's view of a background-query tap: its lifecycle, how many rows it has seen, and
  * the row stream itself. Purely observable — the store owns the socket, so there is no `close`
  * here (same contract as [[WiretapHandler]]).
  */
final class BackgroundQueryTapHandler private[dataservice] (
  val executionId: String,
  val displayName: String,
  val status: Var[BqTapStatus],
  val rowCount: Var[Long],
  /** The most recent rows exactly as they arrived, capped at
    * [[BackgroundQueryTapHandler.MaxMessages]] — the inspection log's backing buffer. Mirrors
    * `WiretapHandler.messages`, so a background-query inspection and a standing-query one are
    * the same widget over the same shape of data.
    */
  val messages: Var[Seq[String]],
  private val rowsBus: EventBus[Json],
) {

  /** Result rows, one JSON object per row keyed by column name. The completion frame is never
    * emitted here — see [[BackgroundQueryTapStore]]. Unlike [[messages]] this is uncapped and
    * unbuffered: it feeds the Explorer's result cards, which do their own buffering.
    */
  val rows: EventStream[Json] = rowsBus.events

  private[dataservice] def push(raw: String, row: Json): Unit = {
    messages.update { msgs =>
      val next = msgs :+ raw
      if (next.length > BackgroundQueryTapHandler.MaxMessages)
        next.drop(next.length - BackgroundQueryTapHandler.MaxMessages)
      else next
    }
    rowCount.update(_ + 1)
    rowsBus.emit(row)
  }

  private[dataservice] def setStatus(s: BqTapStatus): Unit = status.set(s)
}

object BackgroundQueryTapHandler {

  /** Same cap as `WiretapHandler.MaxMessages`: the log is a tail, not a transcript. */
  val MaxMessages = 50
}

/** Manages background-query result taps for one graph namespace: one WebSocket per execution.
  *
  * A deliberate sibling of [[WiretapStore]] rather than an extension of it. That store is keyed
  * on `(sqName, WiretapTapPoint)` throughout, and its handlers' identity fields are read by the
  * standing-query table, the graph-feed chips, and the tap-query reconcile loop — none of which
  * has any meaning for a background query. This store also carries three things that one has no
  * room for: a terminal completion frame, non-reopenability, and no owner fan-out.
  *
  * Three behaviours worth knowing before changing anything here:
  *
  *   - '''The completion frame is never a row.''' `onmessage` tests for it first and returns
  *     without touching the row stream. Everything downstream — the results table, the row
  *     count, the CSV export — depends on that one branch.
  *   - '''A closed socket does not evict its handler.''' The opposite of [[WiretapStore]]: the
  *     handler stays in [[active]] carrying a terminal status, so the card watching it keeps
  *     rendering the rows it captured instead of vanishing when the run finishes. Only
  *     [[close]] (the card was closed) and [[closeAll]] (namespace teardown) remove one.
  *   - '''An ended tap is not reopenable.''' The server tears its relay down once the run
  *     terminates, plus a short grace window, so reconnecting yields nothing. The card layer
  *     enforces this through the `target.resumable` guards in `CardsStore.goLive`,
  *     `reopenContinuing`, and its `FetchMoreSamples` handler (a background-query tap is not
  *     resumable); this store simply never reconnects on its own.
  */
final class BackgroundQueryTapStore(graphName: String, routes: ClientRoutes) {

  private val activeVar: Var[Map[String, BackgroundQueryTapHandler]] = Var(Map.empty)

  /** The taps currently held, keyed by execution id. */
  val active: Signal[Map[String, BackgroundQueryTapHandler]] = activeVar.signal

  // Synchronous mirror of "which executions have a socket". `activeVar.update` defers to a
  // queued transaction when `open` runs inside one, so a second `open` for the same execution
  // in that window would pass an `activeVar`-based check and create a duplicate handler (the
  // socket then feeds one instance while the consumer subscribes the other). Mutating this map
  // synchronously closes that window — the same guard, and the same reasoning, as
  // `WiretapStore.isAttached`.
  private val sockets: mutable.Map[String, dom.WebSocket] = mutable.Map.empty

  // Which surfaces are watching each execution. Two can watch the same run at once — an
  // Explorer result card and the Streams page's viewer — and both stay mounted, so the socket
  // must outlive whichever closes first.
  private val subscribers = new SubscriberRegistry

  /** Open a tap on `executionId` for `subscriber`. Idempotent per subscriber; the second
    * subscriber on a run joins the existing socket rather than opening another.
    *
    * The server buffers the head of the result stream for a late subscriber, so connecting
    * immediately after the run is dispatched still catches its first rows.
    */
  def open(subscriber: String, executionId: String, displayName: String): Unit = {
    // The "was this the first subscriber" answer is deliberately ignored: see the socket guard
    // below, which is the stronger condition.
    val _ = subscribers.add(subscriber, executionId)
    // Guarded on the socket rather than on "was first": a handler can outlive its socket (a
    // finished run keeps its buffer), and re-opening over one would strand the existing view.
    if (!sockets.contains(executionId) && !activeVar.now().contains(executionId)) {
      val ws = new dom.WebSocket(s"${WebSocketBase.of(routes)}/${V2Paths.backgroundQueryTap(graphName, executionId)}")
      sockets.update(executionId, ws)
      val handler = new BackgroundQueryTapHandler(
        executionId = executionId,
        displayName = displayName,
        status = Var[BqTapStatus](BqTapStatus.Connecting),
        rowCount = Var(0L),
        messages = Var(Seq.empty),
        rowsBus = new EventBus[Json],
      )
      wire(ws, executionId, handler)
      activeVar.update(_ + (executionId -> handler))
    }
  }

  /** Release `subscriber`'s interest in `executionId`; idempotent. The socket and handler go
    * only once nobody is left watching — otherwise closing one surface's view would cut the
    * stream out from under another's.
    */
  def close(subscriber: String, executionId: String): Unit =
    if (subscribers.remove(subscriber, executionId)) dropTap(executionId)

  /** Close every tap regardless of who is watching. Called on namespace teardown, where the
    * whole store is being replaced.
    */
  def closeAll(): Unit = {
    val open = activeVar.now().keys.toList
    subscribers.clear()
    open.foreach(dropTap)
  }

  private def dropTap(executionId: String): Unit = {
    sockets.remove(executionId).foreach { ws =>
      if (ws.readyState != dom.WebSocket.CLOSED && ws.readyState != dom.WebSocket.CLOSING)
        ws.close(1000, "Tap closed")
    }
    // Surface a terminal status before dropping the handler, for anyone holding a direct
    // reference rather than tracking `active` membership. Same guard as `onclose`: a recorded
    // completion (`Ended(Some(_))`) or a prior error must not be overwritten with `Ended(None)`.
    activeVar.now().get(executionId).foreach { handler =>
      handler.status.now() match {
        case BqTapStatus.Connecting | BqTapStatus.Live => handler.setStatus(BqTapStatus.Ended(None))
        case _ => ()
      }
    }
    activeVar.update(_ - executionId)
  }

  // `handler` is captured by reference rather than looked up by id on each event. Looking it up
  // would mean a socket outliving its own handler could write to a *successor*: close(id)
  // immediately followed by open(id) leaves the old socket's `onclose` still queued, and by the
  // time it runs the id resolves to the new handler — which it would wrongly mark Ended. Holding
  // the reference makes every callback act on the handler it was opened for, or on nothing.
  private def wire(ws: dom.WebSocket, executionId: String, handler: BackgroundQueryTapHandler): Unit = {

    ws.onopen = (_: dom.Event) =>
      if (handler.status.now() == BqTapStatus.Connecting) handler.setStatus(BqTapStatus.Live)

    ws.onerror = (_: dom.Event) =>
      if (handler.status.now() == BqTapStatus.Connecting) handler.setStatus(BqTapStatus.Error("Connection failed"))

    ws.onclose = (_: dom.CloseEvent) => {
      // The socket is spent either way, so stop tracking it — but leave the handler in place
      // (see the class doc). Only remove the entry if it is still *this* socket, so a reopened
      // tap's socket isn't dropped by its predecessor's close event.
      if (sockets.get(executionId).contains(ws)) sockets.remove(executionId)
      // A close that follows the completion frame must not overwrite the completion already
      // recorded, and a prior error stays an error.
      handler.status.now() match {
        case BqTapStatus.Connecting | BqTapStatus.Live => handler.setStatus(BqTapStatus.Ended(None))
        case _ => ()
      }
    }

    ws.onmessage = (e: dom.MessageEvent) =>
      io.circe.parser.parse(e.data.toString).foreach {
        // Load-bearing: the completion frame terminates the tap and is NOT a result row.
        // Emitting it would put a `__backgroundQueryComplete` column in the results table.
        case BackgroundQueryCompletion(completion) =>
          handler.setStatus(BqTapStatus.Ended(Some(completion)))
          if (ws.readyState != dom.WebSocket.CLOSED && ws.readyState != dom.WebSocket.CLOSING)
            ws.close(1000, "Run complete")
        case row => handler.push(e.data.toString, row)
      }
  }
}
