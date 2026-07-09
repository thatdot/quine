package com.thatdot.quine.webapp.resultspanel

import scala.collection.mutable

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.v2api.V2Paths

sealed abstract class WiretapStatus
object WiretapStatus {
  case object Connecting extends WiretapStatus
  case object Live extends WiretapStatus
  final case class Error(msg: String) extends WiretapStatus
  case object Closed extends WiretapStatus
}

/** A handler observing one [[TapTarget]] source under a caller-supplied key.
  *
  * Handlers live in [[WiretapStore.active]]; they appear when someone calls
  * [[WiretapStore.open]] with their key and vanish when someone calls
  * [[WiretapStore.close]] (or when the namespace tears down). The handler is purely
  * observable — there is no `close()` method on it, since commands flow through the
  * store and the store is the single source of truth for "what is currently open."
  *
  * The `key` is the unit of sharing: two consumers using the same key for the same
  * source share one handler (and one row in `active`). Different keys on the same
  * source ([[TapTarget.key]]) give independent handlers that nevertheless share the
  * underlying WebSocket.
  *
  * @param target the standing query × pipeline point this handler observes
  */
final class WiretapHandler private[resultspanel] (
  val key: String,
  val target: TapTarget,
  val status: Var[WiretapStatus],
  private val matchesBus: EventBus[io.circe.Json],
) {
  val matches: EventStream[io.circe.Json] = matchesBus.events

  private[resultspanel] def push(parsed: io.circe.Json): Unit = matchesBus.emit(parsed)

  private[resultspanel] def setStatus(s: WiretapStatus): Unit = status.set(s)
}

/** Manages wiretap subscriptions for one graph namespace.
  *
  * Callers manipulate the store with fire-and-forget commands:
  *   - `open(key, target)` — open if not already open. Idempotent.
  *   - `close(key)` — close if open. Idempotent.
  *   - `closeAll()` — used by the lifecycle host on namespace teardown.
  *
  * The set of currently-open handlers is exposed as the reactive [[active]] signal,
  * so any consumer (a manage-taps UI, a results log, a dispatch host that turns
  * matches into graph queries) can subscribe without holding handler references.
  *
  * The store maintains one WebSocket per [[TapTarget]] source; handlers with
  * different keys on the same source share that socket and the socket closes
  * automatically when its last handler is closed.
  */
final class WiretapStore(graphName: String, routes: ClientRoutes) {

  // The only externally-observable state. Keyed by client-supplied key.
  private val activeVar: Var[Map[String, WiretapHandler]] = Var(Map.empty)
  val active: Signal[Map[String, WiretapHandler]] = activeVar.signal

  // Per-source state. The connection tracks the set of keys that currently belong
  // to it so onmessage/onopen/etc. can fan out to exactly the right handlers without
  // scanning the whole `active` map. Handler instances themselves live in `activeVar`;
  // this set is just an index.
  final private class Connection(val target: TapTarget, val ws: dom.WebSocket) {
    val keys: mutable.Set[String] = mutable.Set.empty
  }
  private val connections: mutable.Map[String, Connection] = mutable.Map.empty

  private def wsBase: String =
    routes.baseUrlOpt
      .filter(_.nonEmpty)
      .getOrElse(dom.window.location.origin)
      .replaceFirst("^http", "ws")

  private def tapUrl(target: TapTarget): String = {
    val base = s"$wsBase/${V2Paths.standingQueries(graphName)}/${target.sqName}"
    target.tapPoint match {
      case TapPoint.Raw => s"$base:tap"
      case TapPoint.PreEnrichment(out) => s"$base/outputs/$out:tap_pre_enrichment"
      case TapPoint.PostEnrichment(out) => s"$base/outputs/$out:tap"
    }
  }

  /** Open a handler for `key` on `target`. No-op if a handler already exists for
    * `key`. If this is the first handler on the source, opens a WebSocket for it.
    */
  def open(key: String, target: TapTarget): Unit =
    if (!activeVar.now().contains(key)) {
      val conn = connections.getOrElseUpdate(target.key, openConnection(target))
      val handler = createHandler(key, target, conn)
      conn.keys.add(key)
      activeVar.update(_ + (key -> handler))
    }

  /** Close the handler for `key`. No-op if no handler exists for `key`. If this was
    * the last handler on its source, closes the underlying WebSocket.
    */
  def close(key: String): Unit =
    activeVar.now().get(key).foreach { handler =>
      // Surface Closed to any subscriber still observing the handler's status before
      // we drop it from `active`. Subscribers tracking `active` membership wouldn't
      // need this; subscribers that latched onto a handler ref do.
      handler.setStatus(WiretapStatus.Closed)
      activeVar.update(_ - key)
      connections.get(handler.target.key).foreach { conn =>
        conn.keys.remove(key)
        if (conn.keys.isEmpty) {
          if (conn.ws.readyState != dom.WebSocket.CLOSED && conn.ws.readyState != dom.WebSocket.CLOSING)
            conn.ws.close(1000, "All handlers closed")
          connections.remove(handler.target.key)
        }
      }
    }

  /** Close all open handlers and all underlying sockets. Called on namespace teardown. */
  def closeAll(): Unit =
    // Snapshot the key set; `close(k)` mutates `activeVar` and `connections` underneath
    // us, and closing the last handler on a source removes its Connection.
    activeVar.now().keys.toList.foreach(close)

  // Iterate handlers attached to a connection by going through `activeVar` (the source
  // of truth); the connection's `keys` set is just a quick index.
  private def handlersFor(conn: Connection): Iterable[WiretapHandler] = {
    val current = activeVar.now()
    conn.keys.iterator.flatMap(current.get).toVector
  }

  private def openConnection(target: TapTarget): Connection = {
    val ws = new dom.WebSocket(tapUrl(target))
    val conn = new Connection(target, ws)

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
      // path has already cleared `conn.keys` and pulled the entry out of `connections`,
      // so this is a no-op in that case; on a server-initiated close it's where the
      // cleanup actually happens, so `active` reflects "the WS hung up on us" and a
      // subsequent `open(sameKey, …)` starts a fresh connection rather than no-op-ing
      // against a zombie entry.
      val affected = conn.keys.toList
      if (affected.nonEmpty) {
        val snapshot = activeVar.now()
        affected.foreach { k =>
          snapshot.get(k).foreach { h =>
            h.status.now() match {
              case WiretapStatus.Error(_) => () // preserve a prior error status
              case _ => h.setStatus(WiretapStatus.Closed)
            }
          }
        }
        activeVar.update(current => affected.foldLeft(current)(_ - _))
        conn.keys.clear()
        connections.remove(conn.target.key)
      }
    }
    ws.onmessage = (e: dom.MessageEvent) => {
      val parsed = io.circe.parser.parse(e.data.toString).getOrElse(io.circe.Json.Null)
      handlersFor(conn).foreach(_.push(parsed))
    }

    conn
  }

  private def createHandler(key: String, target: TapTarget, conn: Connection): WiretapHandler = {
    // Seed status from the WS's current readyState so a handler joining an already-Live
    // connection doesn't briefly display "Connecting."
    val initial: WiretapStatus = conn.ws.readyState match {
      case dom.WebSocket.OPEN => WiretapStatus.Live
      case dom.WebSocket.CONNECTING => WiretapStatus.Connecting
      case _ => WiretapStatus.Closed
    }
    new WiretapHandler(
      key = key,
      target = target,
      status = Var[WiretapStatus](initial),
      matchesBus = new EventBus[io.circe.Json],
    )
  }
}
