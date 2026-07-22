package com.thatdot.quine.webapp.dataservice

import scala.collection.mutable

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.ClientRoutes

sealed trait WiretapStatus
object WiretapStatus {
  case object Connecting extends WiretapStatus
  case object Live extends WiretapStatus
  final case class Error(msg: String) extends WiretapStatus
  case object Closed extends WiretapStatus
}

/** Which point in a standing query's output pipeline a tap observes - the dataservice's own
  * vocabulary for this, independent of any consumer's tap-point type (see the consumer-facing
  * equivalent, resultspanel.TapPoint).
  */
sealed abstract class WiretapTapPoint
object WiretapTapPoint {
  case object Raw extends WiretapTapPoint
  final case class PreEnrichment(output: String) extends WiretapTapPoint
  final case class PostEnrichment(output: String) extends WiretapTapPoint

  /** Tap point for a plain `(sqName, outputName)` source: no output name means the raw
    * match stream; an output name means its post-enrichment stream, or its pre-enrichment
    * (post-transformation) stream when `preEnrichment` is set.
    */
  def fromOutputName(outputName: Option[String], preEnrichment: Boolean = false): WiretapTapPoint =
    outputName.fold[WiretapTapPoint](Raw)(out => if (preEnrichment) PreEnrichment(out) else PostEnrichment(out))
}

/** The UI surface that asked for a wiretap. Two different surfaces (e.g. the Streams
  * page and the Explorer Settings tap-queries card) can independently open taps on the
  * same source without colliding, because each owns its own slot in
  * [[WiretapStore.active]].
  */
final case class WiretapOwner(name: String)

/** A handler observing one `(sqName, outputName)` source on behalf of one owner under a
  * caller-supplied `key` (unique within the owner).
  *
  * Handlers live in [[WiretapStore.active]] under their owner; they appear when someone
  * calls [[WiretapStore.open]] and vanish when someone calls [[WiretapStore.close]] (or
  * when the namespace tears down). The handler is purely observable — there is no
  * `close()` method on it, since commands flow through the store and the store is the
  * single source of truth for "what is currently open."
  *
  * The `(owner, key)` pair is the unit of sharing: two consumers using the same owner
  * and key share one handler. Different keys (whether under the same owner or a
  * different one) give independent handlers that nevertheless share the underlying
  * WebSocket if they target the same source.
  *
  * @param tapPoint which point in the standing query's output pipeline this handler observes -
  *   raw, or post-enrichment on an output
  */
final class WiretapHandler private[dataservice] (
  val key: String,
  val sqName: String,
  val tapPoint: WiretapTapPoint,
  val status: Var[WiretapStatus],
  val messages: Var[Seq[String]],
  // Total matches observed since the handler was opened. Lives alongside `messages`
  // (which is capped at MaxMessages) so the UI can display a true cumulative count.
  val matchCount: Var[Int],
  private val matchesBus: EventBus[io.circe.Json],
) {
  val matches: EventStream[io.circe.Json] = matchesBus.events

  def sourceKey: String = WiretapHandler.sourceKey(sqName, tapPoint)

  private[dataservice] def push(raw: String, parsed: io.circe.Json): Unit = {
    messages.update { msgs =>
      val next = msgs :+ raw
      if (next.length > WiretapHandler.MaxMessages)
        next.drop(next.length - WiretapHandler.MaxMessages)
      else next
    }
    matchCount.update(_ + 1)
    matchesBus.emit(parsed)
  }

  private[dataservice] def setStatus(s: WiretapStatus): Unit = status.set(s)
}

object WiretapHandler {
  val MaxMessages = 50

  def sourceKey(sqName: String, tapPoint: WiretapTapPoint): String = tapPoint match {
    case WiretapTapPoint.Raw => s"$sqName::raw"
    case WiretapTapPoint.PreEnrichment(out) => s"$sqName::pre::$out"
    case WiretapTapPoint.PostEnrichment(out) => s"$sqName::post::$out"
  }
}

/** Manages wiretap subscriptions for one graph namespace.
  *
  * Callers manipulate the store with fire-and-forget commands:
  *   - `open(owner, key, sqName, outputName)` — open if not already open. Idempotent.
  *   - `close(owner, key)` — close if open. Idempotent.
  *   - `closeAll()` — used by the lifecycle host on namespace teardown.
  *
  * The set of currently-open handlers is exposed as the reactive [[active]] signal,
  * grouped by owner, so any consumer (a manage-taps UI, a results log, a dispatch host
  * that turns matches into graph queries) can subscribe without holding handler
  * references.
  *
  * The store maintains one WebSocket per `(sqName, outputName)` source; handlers
  * with different `(owner, key)` pairs on the same source share that socket and the
  * socket closes automatically when its last handler is closed.
  */
final class WiretapStore(graphName: String, routes: ClientRoutes) {

  // The only externally-observable state. Grouped by owner; within an owner, each
  // handler has a unique `key`.
  private val activeVar: Var[Map[WiretapOwner, List[WiretapHandler]]] = Var(Map.empty)
  val active: Signal[Map[WiretapOwner, List[WiretapHandler]]] = activeVar.signal

  // Per-source state. The connection tracks the set of (owner, key) pairs that
  // currently belong to it so onmessage/onopen/etc. can fan out to exactly the right
  // handlers without scanning the whole `active` map. Handler instances themselves
  // live in `activeVar`; this set is just an index.
  final private class Connection(val sqName: String, val tapPoint: WiretapTapPoint, val ws: dom.WebSocket) {
    val attached: mutable.Set[(WiretapOwner, String)] = mutable.Set.empty
  }
  private val connections: mutable.Map[String, Connection] = mutable.Map.empty

  private def wsBase: String =
    routes.baseUrlOpt
      .filter(_.nonEmpty)
      .getOrElse(dom.window.location.origin)
      .replaceFirst("^http", "ws")

  private def rawTapUrl(sqName: String): String =
    s"$wsBase/api/v2/graph/$graphName/standingQueries/$sqName:tap"

  private def preEnrichmentTapUrl(sqName: String, outputName: String): String =
    s"$wsBase/api/v2/graph/$graphName/standingQueries/$sqName/outputs/$outputName:tapPreEnrichment"

  private def postEnrichmentTapUrl(sqName: String, outputName: String): String =
    s"$wsBase/api/v2/graph/$graphName/standingQueries/$sqName/outputs/$outputName:tap"

  private def tapUrl(sqName: String, tapPoint: WiretapTapPoint): String = tapPoint match {
    case WiretapTapPoint.Raw => rawTapUrl(sqName)
    case WiretapTapPoint.PreEnrichment(out) => preEnrichmentTapUrl(sqName, out)
    case WiretapTapPoint.PostEnrichment(out) => postEnrichmentTapUrl(sqName, out)
  }

  /** Open a handler for `(owner, key)` on `(sqName, tapPoint)`. No-op if a handler
    * already exists for that pair. If this is the first handler on the source, opens a
    * WebSocket for it.
    */
  def open(owner: WiretapOwner, key: String, sqName: String, tapPoint: WiretapTapPoint): Unit =
    if (!isOpen(owner, key) && !isAttached(owner, key)) {
      val srcKey = WiretapHandler.sourceKey(sqName, tapPoint)
      val conn = connections.getOrElseUpdate(srcKey, openConnection(sqName, tapPoint))
      val handler = createHandler(key, sqName, tapPoint, conn)
      conn.attached.add((owner, key))
      activeVar.update { m =>
        val list = m.getOrElse(owner, List.empty)
        m.updated(owner, handler :: list)
      }
    }

  /** Close the handler for `(owner, key)`. No-op if no such handler exists. If this was
    * the last handler on its source, closes the underlying WebSocket.
    */
  def close(owner: WiretapOwner, key: String): Unit =
    activeVar.now().get(owner).flatMap(_.find(_.key == key)).foreach { handler =>
      // Surface Closed to any subscriber still observing the handler's status before
      // we drop it from `active`. Subscribers tracking `active` membership wouldn't
      // need this; subscribers that latched onto a handler ref do.
      handler.setStatus(WiretapStatus.Closed)
      activeVar.update(removeHandler(_, owner, key))
      connections.get(handler.sourceKey).foreach { conn =>
        conn.attached.remove((owner, key))
        if (conn.attached.isEmpty) {
          if (conn.ws.readyState != dom.WebSocket.CLOSED && conn.ws.readyState != dom.WebSocket.CLOSING)
            conn.ws.close(1000, "All handlers closed")
          connections.remove(handler.sourceKey)
        }
      }
    }

  /** Close all open handlers and all underlying sockets. Called on namespace teardown. */
  def closeAll(): Unit =
    // Snapshot the (owner, key) set; `close` mutates `activeVar` and `connections`
    // underneath us, and closing the last handler on a source removes its Connection.
    activeVar
      .now()
      .iterator
      .flatMap { case (owner, hs) => hs.iterator.map(h => (owner, h.key)) }
      .toList
      .foreach { case (owner, key) => close(owner, key) }

  /** Synchronous read of the handler keys currently open under `owner` — for
    * reconciliation code running inside command handlers, where the reactive [[active]]
    * signal is not samplable.
    */
  def activeKeys(owner: WiretapOwner): Set[String] =
    activeVar.now().getOrElse(owner, List.empty).iterator.map(_.key).toSet

  private def isOpen(owner: WiretapOwner, key: String): Boolean =
    activeVar.now().get(owner).exists(_.exists(_.key == key))

  // `activeVar.update` defers to a queued transaction when `open` runs inside one, so a
  // second `open` for the same pair in that window passes the `isOpen` check and creates
  // a duplicate handler (the socket then feeds one instance while consumers subscribe
  // the other). `attached` is mutated synchronously, closing that window.
  private def isAttached(owner: WiretapOwner, key: String): Boolean =
    connections.valuesIterator.exists(_.attached.contains((owner, key)))

  private def removeHandler(
    m: Map[WiretapOwner, List[WiretapHandler]],
    owner: WiretapOwner,
    key: String,
  ): Map[WiretapOwner, List[WiretapHandler]] =
    m.get(owner) match {
      case None => m
      case Some(list) =>
        val updated = list.filterNot(_.key == key)
        if (updated.isEmpty) m - owner else m.updated(owner, updated)
    }

  // Iterate handlers attached to a connection by going through `activeVar` (the source
  // of truth); the connection's `attached` set is just a quick index.
  private def handlersFor(conn: Connection): Iterable[WiretapHandler] = {
    val current = activeVar.now()
    conn.attached.iterator.flatMap { case (owner, key) =>
      current.get(owner).flatMap(_.find(_.key == key))
    }.toVector
  }

  private def openConnection(sqName: String, tapPoint: WiretapTapPoint): Connection = {
    val url = tapUrl(sqName, tapPoint)
    val ws = new dom.WebSocket(url)
    val conn = new Connection(sqName, tapPoint, ws)

    ws.onopen = (_: dom.Event) =>
      handlersFor(conn).foreach { h =>
        if (h.status.now() == WiretapStatus.Connecting) h.setStatus(WiretapStatus.Live)
      }
    ws.onerror = (_: dom.Event) =>
      handlersFor(conn).foreach { h =>
        if (h.status.now() == WiretapStatus.Connecting) h.setStatus(WiretapStatus.Error("Connection failed"))
      }
    ws.onclose = (_: dom.CloseEvent) => {
      // Evict every handler still attached to this connection. The close-initiated-by-us
      // path has already cleared `conn.attached` and pulled the entry out of `connections`,
      // so this is a no-op in that case; on a server-initiated close it's where the
      // cleanup actually happens, so `active` reflects "the WS hung up on us" and a
      // subsequent `open(sameOwner, sameKey, …)` starts a fresh connection rather than
      // no-op-ing against a zombie entry.
      val affected = conn.attached.toList
      if (affected.nonEmpty) {
        val snapshot = activeVar.now()
        affected.foreach { case (owner, key) =>
          snapshot.get(owner).flatMap(_.find(_.key == key)).foreach { h =>
            h.status.now() match {
              case WiretapStatus.Error(_) => () // preserve a prior error status
              case _ => h.setStatus(WiretapStatus.Closed)
            }
          }
        }
        activeVar.update { current =>
          affected.foldLeft(current) { case (acc, (owner, key)) => removeHandler(acc, owner, key) }
        }
        conn.attached.clear()
        connections.remove(WiretapHandler.sourceKey(conn.sqName, conn.tapPoint))
      }
    }
    ws.onmessage = (e: dom.MessageEvent) => {
      val raw = e.data.toString
      val parsed = io.circe.parser.parse(raw).getOrElse(io.circe.Json.Null)
      handlersFor(conn).foreach(_.push(raw, parsed))
    }

    conn
  }

  private def createHandler(
    key: String,
    sqName: String,
    tapPoint: WiretapTapPoint,
    conn: Connection,
  ): WiretapHandler = {
    // Seed status from the WS's current readyState so a handler joining an already-Live
    // connection doesn't briefly display "Connecting."
    val initial: WiretapStatus = conn.ws.readyState match {
      case dom.WebSocket.OPEN => WiretapStatus.Live
      case dom.WebSocket.CONNECTING => WiretapStatus.Connecting
      case _ => WiretapStatus.Closed
    }
    new WiretapHandler(
      key = key,
      sqName = sqName,
      tapPoint = tapPoint,
      status = Var[WiretapStatus](initial),
      messages = Var[Seq[String]](Seq.empty),
      matchCount = Var[Int](0),
      matchesBus = new EventBus[io.circe.Json],
    )
  }
}
