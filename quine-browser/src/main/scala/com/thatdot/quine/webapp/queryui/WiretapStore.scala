package com.thatdot.quine.webapp.queryui

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

/** The UI surface that asked for a wiretap. Two different surfaces (e.g. the Streams
  * page and the Explorer Settings tap-queries card) can independently open taps on the
  * same source without colliding, because each owns its own slot in
  * [[WiretapStore.active]].
  */
final case class WiretapOwner(name: String)

/** A handler observing one `(sqName, outputName)` source on behalf of one owner under a
  * caller-supplied `key` (unique within the owner + namespace).
  *
  * Handlers live in [[WiretapStore.active]] under their `(namespace, owner)` key; they
  * appear when someone calls [[WiretapStore.open]] and vanish when someone calls
  * [[WiretapStore.close]]. The handler is purely observable — there is no `close()`
  * method on it, since commands flow through the store and the store is the single
  * source of truth for "what is currently open."
  *
  * The `(namespace, owner, key)` triple is the unit of sharing: two consumers using the
  * same triple share one handler. Different keys (whether under the same owner or a
  * different one) give independent handlers that nevertheless share the underlying
  * WebSocket if they target the same `(namespace, sqName, outputName)`.
  *
  * @param outputName None = raw tap; Some(name) = post-enrichment tap on that output
  */
final class WiretapHandler private[queryui] (
  val key: String,
  val sqName: String,
  val outputName: Option[String],
  val status: Var[WiretapStatus],
  val messages: Var[Seq[String]],
  // Total matches observed since the handler was opened. Lives alongside `messages`
  // (which is capped at MaxMessages) so the UI can display a true cumulative count.
  val matchCount: Var[Int],
  private val matchesBus: EventBus[io.circe.Json],
) {
  val matches: EventStream[io.circe.Json] = matchesBus.events

  def sourceKey: String = WiretapHandler.sourceKey(sqName, outputName)

  private[queryui] def push(raw: String, parsed: io.circe.Json): Unit = {
    messages.update { msgs =>
      val next = msgs :+ raw
      if (next.length > WiretapHandler.MaxMessages)
        next.drop(next.length - WiretapHandler.MaxMessages)
      else next
    }
    matchCount.update(_ + 1)
    matchesBus.emit(parsed)
  }

  private[queryui] def setStatus(s: WiretapStatus): Unit = status.set(s)
}

object WiretapHandler {
  val MaxMessages = 50

  def sourceKey(sqName: String, outputName: Option[String]): String =
    outputName.fold(s"$sqName::raw")(out => s"$sqName::post::$out")
}

/** Manages wiretap subscriptions across all graph namespaces. One instance is
  * constructed at app-entry and shared by every page — the namespace is supplied at
  * command time, not baked into the store, so a single store serves every graph the
  * user visits.
  *
  * Callers manipulate the store with fire-and-forget commands:
  *   - `open(namespace, owner, key, sqName, outputName)` — open if not already open.
  *     Idempotent.
  *   - `close(namespace, owner, key)` — close if open. Idempotent.
  *   - `closeAll(namespace)` — close every handler under the given namespace. Used
  *     when a graph is torn down.
  *
  * The set of currently-open handlers is exposed as the reactive [[active]] signal,
  * grouped by `(namespace, owner)`, so any consumer (a manage-taps UI, a results log,
  * a dispatch host that turns matches into graph queries) can subscribe without
  * holding handler references.
  *
  * The store maintains one WebSocket per `(namespace, sqName, outputName)` source;
  * handlers with different `(owner, key)` pairs on the same source share that socket
  * and the socket closes automatically when its last handler is closed.
  */
final class WiretapStore(routes: ClientRoutes) {

  // The only externally-observable state. Grouped by (namespace, owner); within that
  // key, each handler has a unique `key`.
  private val activeVar: Var[Map[(String, WiretapOwner), List[WiretapHandler]]] = Var(Map.empty)
  val active: Signal[Map[(String, WiretapOwner), List[WiretapHandler]]] = activeVar.signal

  // Per-source state. The connection tracks the set of (owner, key) pairs that
  // currently belong to it so onmessage/onopen/etc. can fan out to exactly the right
  // handlers without scanning the whole `active` map. Handler instances themselves
  // live in `activeVar`; this set is just an index.
  final private class Connection(
    val namespace: String,
    val sqName: String,
    val outputName: Option[String],
    val ws: dom.WebSocket,
  ) {
    val attached: mutable.Set[(WiretapOwner, String)] = mutable.Set.empty
  }
  private val connections: mutable.Map[(String, String), Connection] = mutable.Map.empty

  private def wsBase: String =
    routes.baseUrlOpt
      .filter(_.nonEmpty)
      .getOrElse(dom.window.location.origin)
      .replaceFirst("^http", "ws")

  private def rawTapUrl(namespace: String, sqName: String): String =
    s"$wsBase/api/v2/graph/$namespace/standingQueries/$sqName:tap"

  private def postEnrichmentTapUrl(namespace: String, sqName: String, outputName: String): String =
    s"$wsBase/api/v2/graph/$namespace/standingQueries/$sqName/outputs/$outputName:tap"

  /** Open a handler for `(namespace, owner, key)` on `(sqName, outputName)`. No-op if
    * a handler already exists for that triple. If this is the first handler on the
    * source, opens a WebSocket for it.
    */
  def open(
    namespace: String,
    owner: WiretapOwner,
    key: String,
    sqName: String,
    outputName: Option[String],
  ): Unit =
    if (!isOpen(namespace, owner, key)) {
      val srcKey = WiretapHandler.sourceKey(sqName, outputName)
      val conn = connections.getOrElseUpdate((namespace, srcKey), openConnection(namespace, sqName, outputName))
      val handler = createHandler(key, sqName, outputName, conn)
      conn.attached.add((owner, key))
      activeVar.update { m =>
        val list = m.getOrElse((namespace, owner), List.empty)
        m.updated((namespace, owner), handler :: list)
      }
    }

  /** Close the handler for `(namespace, owner, key)`. No-op if no such handler exists.
    * If this was the last handler on its source, closes the underlying WebSocket.
    */
  def close(namespace: String, owner: WiretapOwner, key: String): Unit =
    activeVar.now().get((namespace, owner)).flatMap(_.find(_.key == key)).foreach { handler =>
      // Surface Closed to any subscriber still observing the handler's status before
      // we drop it from `active`. Subscribers tracking `active` membership wouldn't
      // need this; subscribers that latched onto a handler ref do.
      handler.setStatus(WiretapStatus.Closed)
      activeVar.update(removeHandler(_, namespace, owner, key))
      connections.get((namespace, handler.sourceKey)).foreach { conn =>
        conn.attached.remove((owner, key))
        if (conn.attached.isEmpty) {
          if (conn.ws.readyState != dom.WebSocket.CLOSED && conn.ws.readyState != dom.WebSocket.CLOSING)
            conn.ws.close(1000, "All handlers closed")
          connections.remove((namespace, handler.sourceKey))
        }
      }
    }

  /** Close every handler under `namespace`. Used when a graph is torn down (leaving
    * other namespaces' handlers untouched).
    */
  def closeAll(namespace: String): Unit =
    activeVar
      .now()
      .iterator
      .collect { case ((ns, owner), hs) if ns == namespace => hs.iterator.map(h => (owner, h.key)) }
      .flatten
      .toList
      .foreach { case (owner, key) => close(namespace, owner, key) }

  /** Synchronous read of the current handler keys under `(namespace, owner)`, for
    * callers that need to act on the state outside a Signal (e.g. reconciling against
    * a fresh server list). Reactive consumers should observe `active` instead.
    */
  def activeKeys(namespace: String, owner: WiretapOwner): Set[String] =
    activeVar.now().get((namespace, owner)).map(_.map(_.key).toSet).getOrElse(Set.empty)

  private def isOpen(namespace: String, owner: WiretapOwner, key: String): Boolean =
    activeVar.now().get((namespace, owner)).exists(_.exists(_.key == key))

  private def removeHandler(
    m: Map[(String, WiretapOwner), List[WiretapHandler]],
    namespace: String,
    owner: WiretapOwner,
    key: String,
  ): Map[(String, WiretapOwner), List[WiretapHandler]] =
    m.get((namespace, owner)) match {
      case None => m
      case Some(list) =>
        val updated = list.filterNot(_.key == key)
        if (updated.isEmpty) m - ((namespace, owner)) else m.updated((namespace, owner), updated)
    }

  // Iterate handlers attached to a connection by going through `activeVar` (the source
  // of truth); the connection's `attached` set is just a quick index.
  private def handlersFor(conn: Connection): Iterable[WiretapHandler] = {
    val current = activeVar.now()
    conn.attached.iterator.flatMap { case (owner, key) =>
      current.get((conn.namespace, owner)).flatMap(_.find(_.key == key))
    }.toVector
  }

  private def openConnection(namespace: String, sqName: String, outputName: Option[String]): Connection = {
    val url = outputName.fold(rawTapUrl(namespace, sqName))(postEnrichmentTapUrl(namespace, sqName, _))
    val ws = new dom.WebSocket(url)
    val conn = new Connection(namespace, sqName, outputName, ws)

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
          snapshot.get((conn.namespace, owner)).flatMap(_.find(_.key == key)).foreach { h =>
            h.status.now() match {
              case WiretapStatus.Error(_) => () // preserve a prior error status
              case _ => h.setStatus(WiretapStatus.Closed)
            }
          }
        }
        activeVar.update { current =>
          affected.foldLeft(current) { case (acc, (owner, key)) => removeHandler(acc, conn.namespace, owner, key) }
        }
        conn.attached.clear()
        connections.remove((conn.namespace, WiretapHandler.sourceKey(conn.sqName, conn.outputName)))
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
    outputName: Option[String],
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
      outputName = outputName,
      status = Var[WiretapStatus](initial),
      messages = Var[Seq[String]](Seq.empty),
      matchCount = Var[Int](0),
      matchesBus = new EventBus[io.circe.Json],
    )
  }
}
