package com.thatdot.quine.webapp.queryui

import scala.concurrent.{Future, Promise}
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.scalajs.js.Thenable.Implicits._
import scala.util.Random

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.scalajs.dom
import org.scalajs.dom.{
  IDBCursorWithValue,
  IDBDatabase,
  IDBObjectStore,
  IDBTransactionMode,
  Lock,
  LockManager,
  LockOptions,
  console,
  window,
}
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.UiEdge
import com.thatdot.quine.webapp.History

// Per-tab, per-namespace persistence for the graph explorer, backed by IndexedDB. Keys are
// "$tabId:$nsKey", where tabId lives in sessionStorage (stable across reloads) paired with an
// exclusive Web Lock held for the page's lifetime as the tab-liveness signal. Duplicating a
// tab clones sessionStorage, so the duplicate inherits the id — the still-held lock is how we
// detect that and fork the duplicate onto a fresh id with a copy of the state (see
// `tabIdReady`). Closed tabs never clean up their own entries, so purgeStale is the only GC
// for orphaned snapshots; a held lock protects a live tab's entries from it regardless of age.
final case class FullSerializableNode(
  id: String,
  hostIndex: Int,
  label: String,
  properties: Map[String, Json],
  x: Option[Double],
  y: Option[Double],
  fixed: Boolean,
)

final case class SerializableEdge(
  uiEdge: UiEdge[String],
  isSynthetic: Boolean,
)

final case class SerializableCluster(
  nodeIds: Seq[String],
  clusterId: String,
  name: String,
)

final case class SerializableHistoryEntry(
  query: String,
  pinned: Boolean,
  timeLabel: String,
  wasError: Boolean,
  errorMessage: Option[String],
)

final case class FullSnapshot(
  nodes: Seq[FullSerializableNode],
  edges: Seq[SerializableEdge],
  history: History[QueryUiEvent],
  query: String,
  foundNodesCount: Option[Int],
  foundEdgesCount: Option[Int],
  /** Historical-query timestamp from the time-travel control (`None` = live/latest). Not when
    * this snapshot was written — see `savedAt` below for that.
    */
  atTime: Option[Long],
  pinnedNodes: Set[String],
  viewPosition: (Double, Double),
  viewScale: Double,
  layout: String,
  collapsedClusters: Seq[SerializableCluster],
  clusterPositions: Map[String, (Double, Double)],
  resultsEntries: Seq[SerializableHistoryEntry],
  resultsCurrentIdx: Int,
  resultsCollapsed: Boolean,
  /** Wall-clock time (`js.Date.now()`) this snapshot was written to IndexedDB. Used only by
    * `ExplorerStore.purgeStale` to find and delete entries older than `MaxAgeMs`.
    */
  savedAt: Double,
)

object ExplorerStore {

  implicit private val jsonEncoder: Encoder[Json] = Encoder.instance(identity)
  implicit private val jsonDecoder: Decoder[Json] = Decoder.instance(c => Right(c.value))

  implicit private val tupleDoubleEncoder: Encoder[(Double, Double)] =
    Encoder.instance { case (a, b) => Json.arr(Json.fromDoubleOrNull(a), Json.fromDoubleOrNull(b)) }
  implicit private val tupleDoubleDecoder: Decoder[(Double, Double)] =
    Decoder.instance { c =>
      for {
        a <- c.downN(0).as[Double]
        b <- c.downN(1).as[Double]
      } yield (a, b)
    }

  implicit private val uiEdgeEncoder: Encoder[UiEdge[String]] = deriveEncoder
  implicit private val uiEdgeDecoder: Decoder[UiEdge[String]] = deriveDecoder

  implicit private val historyCirceEncoder: Encoder[History[QueryUiEvent]] =
    HistoryJsonSchema.historySchema.encoder
  implicit private val historyCirceDecoder: Decoder[History[QueryUiEvent]] =
    HistoryJsonSchema.historySchema.decoder

  implicit private val fullNodeEncoder: Encoder[FullSerializableNode] = deriveEncoder
  implicit private val fullNodeDecoder: Decoder[FullSerializableNode] = deriveDecoder
  implicit private val edgeEncoder: Encoder[SerializableEdge] = deriveEncoder
  implicit private val edgeDecoder: Decoder[SerializableEdge] = deriveDecoder
  implicit private val clusterEncoder: Encoder[SerializableCluster] = deriveEncoder
  implicit private val clusterDecoder: Decoder[SerializableCluster] = deriveDecoder
  implicit private val histEntryEncoder: Encoder[SerializableHistoryEntry] = deriveEncoder
  implicit private val histEntryDecoder: Decoder[SerializableHistoryEntry] = deriveDecoder
  implicit private val snapshotEncoder: Encoder[FullSnapshot] = deriveEncoder
  implicit private val snapshotDecoder: Decoder[FullSnapshot] = deriveDecoder

  private val DbName = "thatdot.explorer"
  private val DbVersion = 1d
  private val StoreName = "snapshots"
  private val TabIdKey = "thatdot.explorer.tabId"
  private val LockPrefix = "thatdot.explorer.tab."
  // Snapshots older than this are GC'd by purgeStale; the usual source is a tab that was
  // closed (or crashed) without ever calling clear/remove for its own entries.
  private val MaxAgeMs = 48d * 60 * 60 * 1000

  private var cachedDb: Option[IDBDatabase] = None

  private def freshTabId(): String = s"tab-${Random.nextLong().toHexString}-${Random.nextLong().toHexString}"

  // The Web Locks manager, when the browser has one. Checked dynamically because the
  // scalajs-dom facade exposes `navigator.locks` unconditionally.
  private def lockManager: Option[LockManager] = {
    val locks = window.navigator.asInstanceOf[js.Dynamic].locks
    if (js.isUndefined(locks) || locks == null) None else Some(locks.asInstanceOf[LockManager])
  }

  /** Try to acquire `name` exclusively and hold it for the rest of the page's lifetime: the
    * request callback returns a promise that never settles, and the browser releases the lock
    * when the page goes away — including on crash. Yields false when the lock is already held
    * by another page. Yields true when Web Locks is unavailable: with no liveness signal,
    * claiming sole ownership degrades to the pre-lock behavior (duplicated tabs share an id).
    */
  private def holdLockForPageLifetime(name: String): Future[Boolean] =
    lockManager match {
      case None => Future.successful(true)
      case Some(locks) =>
        val acquired = Promise[Boolean]()
        val opts = new LockOptions {}
        opts.ifAvailable = true
        val _ = locks.request(
          name,
          opts,
          (lock: Lock) =>
            if (lock == null) {
              acquired.success(false)
              js.Promise.resolve[Unit](())
            } else {
              acquired.success(true)
              Promise[Unit]().future.toJSPromise // never settles: the lock stays held
            },
        )
        acquired.future
    }

  private var settledTabId: Option[String] = None

  /** Resolves once this tab's identity is settled: an id whose page-lifetime lock this tab
    * now holds. Inheriting an id via sessionStorage covers reload, restore-closed-tab, and
    * crash recovery (the previous holder's lock is gone, so the lock is acquired and the
    * state is reused). A duplicated tab also inherits the id, but the original tab still
    * holds its lock — that collision is the duplication signal, and the duplicate forks:
    * fresh id, plus a copy of the original's snapshots so it keeps the state it was cloned
    * showing, then the two tabs diverge without clobbering each other's writes.
    */
  private val tabIdReady: Future[String] = {
    val settled = Option(window.sessionStorage.getItem(TabIdKey)).filter(_.nonEmpty) match {
      case None =>
        val id = freshTabId()
        window.sessionStorage.setItem(TabIdKey, id)
        holdLockForPageLifetime(LockPrefix + id).map(_ => id)
      case Some(id) =>
        holdLockForPageLifetime(LockPrefix + id).flatMap {
          case true => Future.successful(id)
          case false =>
            val fresh = freshTabId()
            window.sessionStorage.setItem(TabIdKey, fresh)
            holdLockForPageLifetime(LockPrefix + fresh)
              .flatMap(_ => copyTabEntries(from = id, to = fresh))
              .map(_ => fresh)
        }
    }
    settled.foreach(id => settledTabId = Some(id))
    settled
  }

  /** Run `op` with the settled tab id — synchronously when identity is already settled, else
    * once it settles. The synchronous path matters for the `beforeunload` save: an async hop
    * before the IndexedDB request would be deferred past the page's death (see
    * `awaitRequest`). Identity settles within milliseconds of startup, so by unload time the
    * synchronous path is the one taken.
    */
  private def withTabId(op: String => Unit): Unit = settledTabId match {
    case Some(id) => op(id)
    case None => tabIdReady.foreach(op)
  }

  private def storeKey(id: String, nsKey: String): String = s"$id:$nsKey"

  /** Copy every snapshot stored under tab `from` to the same namespace key under tab `to`.
    * Runs before a forked duplicate's identity settles, so its first load already sees the
    * copied state.
    */
  private def copyTabEntries(from: String, to: String): Future[Unit] = {
    val fromPrefix = s"$from:"
    withCursor("fork snapshots") { cursor =>
      val key = cursor.key.asInstanceOf[String]
      // Copies get the `to` prefix, so the cursor skips them if it reaches them later.
      if (key.startsWith(fromPrefix)) {
        val _ = cursor.source.put(cursor.value, storeKey(to, key.stripPrefix(fromPrefix)))
      }
    }
  }

  private def openDb(): Future[IDBDatabase] = cachedDb match {
    case Some(db) => Future.successful(db)
    case None =>
      val promise = Promise[IDBDatabase]()
      val factory = window.indexedDB
      if (factory.isEmpty) {
        promise.failure(new Exception("IndexedDB not available"))
        return promise.future
      }
      val request = factory.get.open(DbName, DbVersion)

      request.onupgradeneeded = (_: dom.IDBVersionChangeEvent) => {
        val db = request.result
        if (!db.objectStoreNames.contains(StoreName))
          db.createObjectStore(StoreName)
      }

      request.onsuccess = (_: dom.IDBEvent[IDBDatabase]) => {
        val db = request.result
        cachedDb = Some(db)
        val _ = promise.trySuccess(db)
      }

      request.onerror = (_: dom.ErrorEvent) => {
        console.warn("[Explorer] Failed to open IndexedDB")
        val _ = promise.tryFailure(new Exception("IndexedDB open failed"))
      }

      // Fired when a version upgrade is held up by another open connection. Fail this
      // caller rather than leave its Future pending forever; onsuccess can still fire
      // later (once the blocker closes) and will populate cachedDb via trySuccess.
      request.onblocked = (_: dom.IDBVersionChangeEvent) => {
        console.warn("[Explorer] IndexedDB open blocked by another connection")
        val _ = promise.tryFailure(new Exception("IndexedDB open blocked"))
      }

      promise.future
  }

  /** Run a single request against the snapshot store and resolve once it succeeds or fails,
    * logging on either a synchronous throw or an async request error.
    *
    * When the database handle is already cached, the transaction is issued synchronously
    * (no Future chaining before the request is created). This is load-bearing for the
    * `beforeunload` save path: a callback deferred through the execution context is a
    * macrotask that never runs once the page is tearing down, whereas an IndexedDB request
    * issued synchronously inside the unload handler is committed by the browser even after
    * the page closes.
    */
  private def awaitRequest[A](opName: String, mode: IDBTransactionMode)(
    issue: IDBObjectStore => dom.IDBRequest[_, A],
  ): Future[A] = {
    def run(db: IDBDatabase): Future[A] = {
      val promise = Promise[A]()
      try {
        val tx = db.transaction(StoreName, mode)
        val store = tx.objectStore(StoreName)
        val request = issue(store)

        request.onsuccess = (_: dom.IDBEvent[A]) => promise.success(request.result)
        request.onerror = (_: dom.ErrorEvent) => {
          console.warn(s"[Explorer] $opName failed")
          promise.failure(new Exception(s"$opName failed"))
        }
      } catch {
        case e: Exception =>
          console.warn(s"[Explorer] $opName failed: ${e.getMessage}")
          promise.failure(e)
      }
      promise.future
    }
    cachedDb match {
      case Some(db) => run(db)
      case None => openDb().flatMap(run)
    }
  }

  /** Walk every entry in the snapshot store, invoking `visit` for each cursor position (so
    * callers can inspect/delete records), logging on either a synchronous throw or an async
    * cursor error.
    */
  private def withCursor(opName: String)(visit: IDBCursorWithValue[IDBObjectStore] => Unit): Future[Unit] = {
    def run(db: IDBDatabase): Future[Unit] = {
      val promise = Promise[Unit]()
      try {
        val tx = db.transaction(StoreName, IDBTransactionMode.readwrite)
        val store = tx.objectStore(StoreName)
        val request = store.openCursor()

        request.onsuccess = (_: dom.IDBEvent[IDBCursorWithValue[IDBObjectStore]]) => {
          val cursor = request.result
          if (cursor != null) {
            visit(cursor)
            cursor.continue()
          } else promise.trySuccess(())
        }

        request.onerror = (_: dom.ErrorEvent) => {
          console.warn(s"[Explorer] $opName failed")
          promise.trySuccess(())
        }
      } catch {
        case e: Exception =>
          console.warn(s"[Explorer] $opName failed: ${e.getMessage}")
          promise.trySuccess(())
      }
      promise.future
    }
    cachedDb match {
      case Some(db) => run(db)
      case None => openDb().flatMap(run)
    }
  }

  def save(nsKey: String, snapshot: FullSnapshot): Unit = {
    val encoded = snapshot.asJson.noSpaces
    withTabId { id =>
      val _ = awaitRequest[dom.IDBKey]("save snapshot", IDBTransactionMode.readwrite)(
        _.put(encoded, storeKey(id, nsKey)),
      )
    }
  }

  def load(nsKey: String): Future[Option[FullSnapshot]] =
    tabIdReady
      .flatMap { id =>
        awaitRequest[dom.IDBValue]("load snapshot", IDBTransactionMode.readonly)(_.get(storeKey(id, nsKey)))
      }
      .map { raw =>
        if (raw == null || js.isUndefined(raw)) None
        else
          io.circe.parser.decode[FullSnapshot](raw.asInstanceOf[String]) match {
            case Right(snapshot) => Some(snapshot)
            case Left(err) =>
              // Delete rather than leave it: it would fail the same way on every load
              // until the age-based purge got to it.
              console.warn(s"[Explorer] Discarding undecodable snapshot for '$nsKey': ${err.getMessage}")
              remove(nsKey)
              None
          }
      }
      .recover { case _ => None }

  def remove(nsKey: String): Unit = withTabId { id =>
    val _ = awaitRequest[Unit]("remove snapshot", IDBTransactionMode.readwrite)(_.delete(storeKey(id, nsKey)))
  }

  def clear(): Unit = withTabId { id =>
    val prefix = s"$id:"
    val _ = withCursor("clear namespaces") { cursor =>
      if (cursor.key.asInstanceOf[String].startsWith(prefix)) { val _ = cursor.delete() }
    }
  }

  /** Tab ids whose entries must never be purged: every id whose page-lifetime lock is
    * currently held somewhere, i.e. every live explorer tab. Empty when Web Locks is
    * unavailable (callers must then protect at least their own id explicitly).
    */
  private def liveTabIds(): Future[Set[String]] =
    lockManager match {
      case None => Future.successful(Set.empty)
      case Some(locks) =>
        locks
          .query()
          .map(
            _.held.toSeq
              .map(_.name)
              .collect {
                case name if name.startsWith(LockPrefix) => name.stripPrefix(LockPrefix)
              }
              .toSet,
          )
          .recover { case _ => Set.empty[String] }
    }

  def purgeStale(maxAgeMs: Double = MaxAgeMs): Future[Int] =
    tabIdReady
      .flatMap { ownId =>
        liveTabIds().flatMap { live =>
          // Live tabs' entries are protected regardless of age — an idle-but-open tab's
          // savedAt can legitimately exceed maxAgeMs. Non-live entries still only age out,
          // so "reopen closed tab" can recover its state within the window.
          val protectedIds = live + ownId
          var deleted = 0
          val now = js.Date.now()
          withCursor("purge stale snapshots") { cursor =>
            try {
              val entryTabId = cursor.key.asInstanceOf[String].takeWhile(_ != ':')
              if (!protectedIds.contains(entryTabId)) {
                val raw = cursor.value.asInstanceOf[String]
                val isStale = io.circe.parser
                  .parse(raw)
                  .toOption
                  .flatMap(_.hcursor.get[Double]("savedAt").toOption)
                  .exists(ts => now - ts > maxAgeMs)
                if (isStale) {
                  cursor.delete()
                  deleted += 1
                }
              }
            } catch {
              case e: Exception =>
                console.warn(s"[Explorer] Failed to evaluate entry during purge: ${e.getMessage}")
            }
          }.map(_ => deleted)
        }
      }
      .recover { case _ => 0 }
}
