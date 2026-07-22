package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.resultspanel.streaming.LiveStream

/** One tap available to the panel: the producer's [[LiveSource]] paired with the panel's
  * own [[LiveStream]] buffer. The entry outlives the source — when a source leaves the
  * host's set the buffer freezes and the entry is marked `ended` but stays selectable.
  */
final class TapEntry(val source: LiveSource, val stream: LiveStream) {
  val ended: Var[Boolean] = Var(false)
  def id: String = source.id
  def provenance: Provenance = source.provenance

  /** The `(sqName, tapPoint)` this entry taps, if the producer reports it — the panel keys
    * close / restart off this, never off the producer-owned [[id]].
    */
  def target: Option[TapTarget] = source.tapTarget

  /** Display status: `Ended` once the source has left the set, else the producer's status. */
  val status: Signal[SourceStatus] =
    ended.signal.combineWith(source.status).map { case (e, s) => if (e) SourceStatus.Ended else s }
}

/** The panel's set of tap sources (created elsewhere) and which one, if any, is the
  * currently-selected view. Selection is separate from query history: `selectedId = None`
  * means a query run is showing; `Some(id)` means that tap is showing.
  */
final class TapsState(val subscriptions: TapSubscriptions) {
  val entries: Var[Vector[TapEntry]] = Var(Vector.empty)
  val selectedId: Var[Option[String]] = Var(None)

  // A target key awaiting its source: set by `openAndSelect`, resolved by `sync` once the
  // source arrives (we can't select by `LiveSource.id` up front — that id is the producer's
  // and is assigned asynchronously when the tap opens).
  private[resultspanel] val pendingSelectKey: Var[Option[String]] = Var(None)

  def select(id: String): Unit = selectedId.set(Some(id))
  def clearSelection(): Unit = selectedId.set(None)
  def entryFor(id: String): Option[TapEntry] = entries.now().find(_.id == id)
  def selectedEntry: Option[TapEntry] = selectedId.now().flatMap(entryFor)

  /** Subscribe to a tap on `target` through the capability, keyed by the panel's own
    * [[TapTarget.key]] (idempotent per key — the producer shares the underlying socket).
    */
  def openTap(target: TapTarget): Unit = subscriptions.open(target.key, target)

  /** Open a tap (if not already) and select it as soon as its source is available — now if
    * an entry for that target already exists, otherwise deferred to [[TapsState.sync]].
    */
  def openAndSelect(target: TapTarget): Unit =
    entries.now().find(e => e.target.contains(target) && !e.ended.now()) match {
      case Some(e) => select(e.id)
      case None =>
        pendingSelectKey.set(Some(target.key))
        openTap(target)
    }

  /** Unsubscribe the panel's tap on the entry identified by `id` (a `LiveSource.id`). */
  def closeTap(id: String): Unit =
    entryFor(id).flatMap(_.target).foreach(t => subscriptions.close(t.key))

  /** Stop the tap `id`: free it server-side (close the panel's subscription, which lets the
    * producer drop the source) and freeze our buffer to a static snapshot that stays selectable.
    */
  def stop(id: String): Unit =
    entryFor(id).foreach { e =>
      e.target.foreach(t => subscriptions.close(t.key))
      e.stream.freeze()
      e.ended.set(true)
    }

  /** Re-open a stopped tap from its [[TapTarget]] and select it once its fresh source arrives
    * (sync replaces the stale ended entry with a fresh streaming one).
    */
  def restart(id: String): Unit =
    entryFor(id).flatMap(_.target).foreach(openAndSelect)

  /** Remove the tap `id` from the band: close it (frees the subscription so the source
    * departs and `sync` won't re-add it) and drop the entry; clear the selection if it was it.
    */
  def remove(id: String): Unit = {
    entryFor(id).foreach { e =>
      e.target.foreach(t => subscriptions.close(t.key))
      e.stream.freeze()
    }
    entries.update(_.filterNot(_.id == id))
    if (selectedId.now().contains(id)) clearSelection()
  }
}

object TapsState {
  def empty: TapsState = new TapsState(TapSubscriptions.empty)

  /** Reconcile the host's live set against held entries: open a [[LiveStream]] for each new
    * source, and freeze + mark `ended` any held source that has left (keeping it selectable).
    * `autoSelectIfIdle` selects the first arriving tap when nothing else is showing (an
    * otherwise-empty page), so something appears.
    */
  def sync(taps: TapsState, sources: Vector[LiveSource], autoSelectIfIdle: Boolean): Unit = {
    val current = taps.entries.now()
    val incoming = sources.map(_.id).toSet

    // Departed sources: freeze + mark ended (kept selectable).
    current.foreach { e =>
      if (!incoming.contains(e.id) && !e.ended.now()) {
        e.stream.freeze()
        e.ended.set(true)
      }
    }

    // A source is "still tracked" only if a *live* (non-ended) entry already holds it. A
    // brand-new source, or an ended one that has reappeared (a tap closed then reopened),
    // gets a fresh session; the stale ended entry it replaces is dropped.
    val liveHeldIds = current.collect { case e if !e.ended.now() => e.id }.toSet
    val staleEnded = current.filter(e => e.ended.now() && incoming.contains(e.id))
    val kept = current.filterNot(staleEnded.contains)
    val added = sources.filterNot(s => liveHeldIds.contains(s.id)).map { s =>
      val stream = new LiveStream
      stream.connect(s.records)
      new TapEntry(s, stream)
    }

    val nextEntries = kept ++ added
    if (added.nonEmpty || staleEnded.nonEmpty) taps.entries.set(nextEntries)

    // Resolve a deferred selection (from `openAndSelect`) once its target's source is present.
    taps.pendingSelectKey.now().foreach { key =>
      nextEntries.find(e => !e.ended.now() && e.target.exists(_.key == key)).foreach { e =>
        taps.select(e.id)
        taps.pendingSelectKey.set(None)
      }
    }

    if (added.nonEmpty && autoSelectIfIdle && taps.selectedId.now().isEmpty && current.isEmpty)
      taps.select(added.head.id)
  }
}

/** Read-only projection of [[TapsState]] for the Source picker. */
final case class TapsReads(
  entries: Signal[Vector[TapEntry]],
  selectedId: Signal[Option[String]],
)
object TapsReads {
  def of(t: TapsState): TapsReads =
    TapsReads(t.entries.signal, t.selectedId.signal)
}
