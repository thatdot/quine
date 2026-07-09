package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L.{Signal, Var}

/** One auto-captured run in the session history. `pinned` (Keep) floats it into the
  * Kept group and protects it from pruning; `timeLabel` is the capture time.
  */
final case class HistoryEntry(content: ResultsContent, pinned: Boolean, timeLabel: String)

/** The session result history. Every run is auto-captured; the "current result" is
  * just the entry at `currentIdx`. Entries are chronological (newest last). History
  * knows nothing about compare mode — pruning returns the drop count so a holder of
  * entry indices (the compare pane) can adjust.
  */
final case class HistoryState(
  entries: Var[Vector[HistoryEntry]],
  currentIdx: Var[Int],
)
object HistoryState {
  def empty: HistoryState = HistoryState(Var(Vector.empty), Var(-1))

  /** The content currently shown (history's selected entry), if any. */
  def displayed(h: HistoryState): Signal[Option[ResultsContent]] =
    h.entries.signal.combineWith(h.currentIdx.signal).map { case (es, idx) => es.lift(idx).map(_.content) }

  /** Imperative read of the currently-shown content (for the dispatcher). */
  def displayedNow(h: HistoryState): Option[ResultsContent] =
    h.entries.now().lift(h.currentIdx.now()).map(_.content)

  /** Soft cap on retained runs; pinned (Kept) entries are never pruned. */
  private val cap = 100

  /** Append a new run and jump to it (it becomes the head/current). Once over the
    * cap, drop the oldest *unpinned* runs from the front (stopping at the first
    * pinned one), shifting the current index to match. Returns the number of entries
    * pruned from the front.
    */
  def capture(h: HistoryState, content: ResultsContent, timeLabel: String): Int = {
    val appended = h.entries.now() :+ HistoryEntry(content, pinned = false, timeLabel)
    var drop = 0
    while (appended.size - drop > cap && drop < appended.size && !appended(drop).pinned) drop += 1
    val pruned = appended.drop(drop)
    h.entries.set(pruned)
    h.currentIdx.set(pruned.size - 1)
    drop
  }

  /** Refine the head entry in place (same run, more/updated results). */
  def updateHead(h: HistoryState, content: ResultsContent): Unit = {
    val es = h.entries.now()
    if (es.nonEmpty) h.entries.set(es.updated(es.size - 1, es.last.copy(content = content)))
  }

  /** Auto-capture policy for the live query signal: a new run (None -> Some) appends
    * a fresh entry; a refine (Some -> Some) updates the head in place. Returns the
    * prune count from a capture (0 otherwise).
    */
  def recordLive(
    h: HistoryState,
    prev: Option[ResultsContent],
    live: Option[ResultsContent],
    timeLabel: => String,
  ): Int =
    (prev, live) match {
      case (None, Some(c)) => capture(h, c, timeLabel)
      case (Some(_), Some(c)) => updateHead(h, c); 0
      case _ => 0
    }

  def open(h: HistoryState, idx: Int): Unit = h.currentIdx.set(idx)
  def togglePinAt(h: HistoryState, idx: Int): Unit = {
    val es = h.entries.now()
    if (es.indices.contains(idx)) h.entries.set(es.updated(idx, es(idx).copy(pinned = !es(idx).pinned)))
  }
  def toggleCurrentPin(h: HistoryState): Unit = togglePinAt(h, h.currentIdx.now())
}

/** Read-only signal projection of the session history handed to the switcher (entries + which
  * one is on display).
  */
final case class SessionReads(entries: Signal[Vector[HistoryEntry]], currentIdx: Signal[Int])
object SessionReads {
  def of(h: HistoryState): SessionReads = SessionReads(h.entries.signal, h.currentIdx.signal)
}

/** What a visited-stack entry points at — a query run (by history index) or a tap (by id).
  * A query reference shifts when older entries are pruned; a stale reference (pruned run,
  * removed tap) is skipped at pop time.
  */
sealed abstract class VisitRef
object VisitRef {
  final case class Query(idx: Int) extends VisitRef
  final case class Tap(id: String) extends VisitRef
}

/** Browser-style navigation over visited sources: Back returns to the source you were just
  * viewing, Forward re-traces. A stack, not chronology — so it spans query runs *and* taps, and
  * garbage-collected runs simply drop out instead of renumbering anything.
  *
  * The store pushes the departed focus on every focus *change* (opening an entry, selecting a
  * tap, a new run arriving) and clears `forward` then, exactly like a browser.
  */
final class NavHistory {
  val back: Var[Vector[VisitRef]] = Var(Vector.empty) // most recent last
  val forward: Var[Vector[VisitRef]] = Var(Vector.empty)

  def push(ref: VisitRef): Unit = {
    back.update(_ :+ ref)
    forward.set(Vector.empty)
  }

  /** After `drop` oldest query entries were pruned, keep Query references pointed at the same
    * runs (dropping any that were pruned); Tap references are unaffected.
    */
  def shiftBy(drop: Int): Unit =
    if (drop > 0) {
      def shift(v: Vector[VisitRef]): Vector[VisitRef] = v.flatMap {
        case VisitRef.Query(i) => Option.when(i - drop >= 0)(VisitRef.Query(i - drop))
        case tap => Some(tap)
      }
      back.update(shift)
      forward.update(shift)
    }
}

/** Read-only slice for the header's back/forward arrows. */
final case class NavReads(canBack: Signal[Boolean], canForward: Signal[Boolean])
object NavReads {
  def of(nav: NavHistory): NavReads = NavReads(nav.back.signal.map(_.nonEmpty), nav.forward.signal.map(_.nonEmpty))
}
