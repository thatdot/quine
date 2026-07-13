package com.thatdot.quine.webapp.queryui

import scala.util.{Failure, Success}

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom
import org.scalajs.dom.window
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.util.QuineApiClient
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2TapQuery

/** Always-mounted driver that keeps the shared wiretap store in sync with the user's
  * "Enable locally" tap-query intent, per namespace, on every page.
  *
  * The intent — which tap queries are enabled in each graph — is the single source of
  * truth, held in `enabledTapsVar` and shared with the Explorer Settings page (which
  * renders the toggles that mutate it). Because this runtime lives in the always-mounted
  * explorer wrapper rather than in the settings page, taps restored from `sessionStorage`
  * start firing on a plain explorer refresh without the user ever opening Settings.
  *
  * Responsibilities, all scoped to the currently-selected graph:
  *   - load a graph's saved intent from `sessionStorage` the first time it is seen,
  *   - persist intent back to `sessionStorage` whenever it changes,
  *   - reconcile the store's handlers (and `activeTapQueryMetadataVar`, read by QueryUi's
  *     dispatch host) against intent whenever intent, the graph, or the server list changes,
  *   - prune intent of taps the server no longer has.
  *
  * Handler teardown on a graph switch is left to QueryUi's per-namespace lifecycle
  * (`closeAll(oldNs)`); this runtime reopens the new graph's taps from intent.
  */
object TapQueryRuntime {

  /** Owner used for wiretaps opened from a tap query's "Enable locally" toggle. The
    * per-handler `key` within this owner is the tap query's `name`, so multiple tap
    * queries targeting the same `(sqName, outputName)` each get their own handler
    * (sharing the underlying WebSocket).
    */
  val TapQueryOwner: WiretapOwner = WiretapOwner("tapQuery")

  /** Per-tab persistence of which tap queries the user has flipped on with "Enable
    * locally" — scoped per-namespace (each entry is the set of tap-query names enabled
    * inside that graph). Stored in `sessionStorage` so a tab restores its own state on
    * reload but two tabs can have different taps enabled at once.
    */
  object EnabledTapsStorage {
    private val KeyPrefix = "thatdot.explorer.enabledTaps."

    def load(namespace: String): Set[String] =
      try Option(window.sessionStorage.getItem(KeyPrefix + namespace))
        .filter(_.nonEmpty)
        .flatMap(io.circe.parser.decode[Vector[String]](_).toOption)
        .map(_.toSet)
        .getOrElse(Set.empty)
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"[Explorer] Failed to load enabled taps from sessionStorage: ${e.getMessage}")
          Set.empty
      }

    def save(namespace: String, names: Set[String]): Unit =
      try if (names.isEmpty) window.sessionStorage.removeItem(KeyPrefix + namespace)
      else
        window.sessionStorage.setItem(
          KeyPrefix + namespace,
          Json.fromValues(names.toList.sorted.map(Json.fromString)).noSpaces,
        )
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"[Explorer] Failed to save enabled taps to sessionStorage: ${e.getMessage}")
      }
  }

  /** @param currentNamespaceSignal the graph the explorer is currently showing. `None`
    *                               (not-yet-resolved) is skipped rather than restoring the
    *                               default graph's taps for a boot flicker.
    * @param enabledTapsVar shared intent, keyed by namespace. Mutated by the settings
    *                       toggles; this runtime persists and applies it.
    * @param activeTapQueryMetadataVar shared with QueryUi's dispatch host — populated here
    *                                  alongside opening a handler so matches can dispatch.
    * @param canReadTapQueries whether the current user may GET the tap-query list. When
    *                          false, both the on-entry and periodic fetches are skipped
    *                          so roles lacking the permission don't spam the console with
    *                          401s. OSS callers (no auth) always pass true.
    */
  def apply(
    routes: ClientRoutes,
    currentNamespaceSignal: Signal[Option[String]],
    wiretapStore: WiretapStore,
    enabledTapsVar: Var[Map[String, Set[String]]],
    activeTapQueryMetadataVar: Var[Map[String, V2TapQuery]],
    canReadTapQueries: Boolean,
  ): HtmlElement = {
    // Local mirror of the current graph — `Signal.now()` is protected, so we read the
    // latest value through a Var we own (bound below).
    val currentNsVar: Var[Option[String]] = Var(None)

    // Latest server tap-query list and the namespace it belongs to. `reconcile` opens
    // handlers using this list's SQ/output metadata, so it only acts once the list for
    // the target namespace has arrived.
    val tapQueriesVar: Var[Vector[V2TapQuery]] = Var(Vector.empty)
    val loadedForNsVar: Var[Option[String]] = Var(None)

    def reconcile(ns: String): Unit =
      if (loadedForNsVar.now().contains(ns)) {
        val byName = tapQueriesVar.now().iterator.map(t => t.name -> t).toMap
        val want = enabledTapsVar.now().getOrElse(ns, Set.empty)
        val have = wiretapStore.activeKeys(ns, TapQueryOwner)
        have.diff(want).foreach { name =>
          wiretapStore.close(ns, TapQueryOwner, name)
          activeTapQueryMetadataVar.update(_ - name)
        }
        want.diff(have).foreach { name =>
          byName.get(name).foreach { t =>
            activeTapQueryMetadataVar.update(_ + (name -> t))
            wiretapStore.open(ns, TapQueryOwner, name, t.standingQueryName, t.outputName)
          }
        }
        // Already-open taps whose definition changed server-side (an edited query,
        // synthetic edges, or a moved tap point). Refresh the metadata the dispatch host
        // reads so the edit applies without a reload, and reopen the handler only when the
        // tap point (SQ/output) actually moved — an edit to the query alone reuses it.
        want.intersect(have).foreach { name =>
          byName.get(name).foreach { t =>
            val prev = activeTapQueryMetadataVar.now().get(name)
            if (!prev.contains(t)) activeTapQueryMetadataVar.update(_ + (name -> t))
            val tapPointMoved =
              prev.exists(p => p.standingQueryName != t.standingQueryName || p.outputName != t.outputName)
            if (tapPointMoved) {
              wiretapStore.close(ns, TapQueryOwner, name)
              wiretapStore.open(ns, TapQueryOwner, name, t.standingQueryName, t.outputName)
            }
          }
        }
      }

    def applyServerList(ns: String, tqs: Vector[V2TapQuery]): Unit = {
      tapQueriesVar.set(tqs)
      loadedForNsVar.set(Some(ns))
      val validNames = tqs.iterator.map(_.name).toSet
      enabledTapsVar.update { m =>
        val cur = m.getOrElse(ns, Set.empty)
        val pruned = cur.intersect(validNames)
        if (pruned == cur) m else m.updated(ns, pruned)
      }
      reconcile(ns)
    }

    def fetch(ns: String): Unit =
      if (canReadTapQueries)
        QuineApiClient
          .fetchV2[Vector[V2TapQuery]](s"api/v2/graph/$ns/queryUi/tapQueries", routes)
          .onComplete {
            case Success(tqs) => if (currentNsVar.now().contains(ns)) applyServerList(ns, tqs)
            case Failure(err) =>
              dom.console.warn(s"[TapQueryRuntime] Failed to load tap queries for '$ns': ${err.getMessage}")
          }

    div(
      display := "none",
      currentNamespaceSignal --> currentNsVar.writer,
      // On entering a graph, load its saved intent (once) then fetch its tap list so the
      // enabled taps' handlers open — even if the user never opens Explorer Settings.
      currentNamespaceSignal --> {
        case Some(ns) =>
          if (!enabledTapsVar.now().contains(ns)) enabledTapsVar.update(_ + (ns -> EnabledTapsStorage.load(ns)))
          fetch(ns)
        case None => ()
      },
      // Apply intent to the store whenever it changes (e.g. a settings toggle) for the
      // current graph. No-ops until this graph's tap list has loaded.
      enabledTapsVar.signal.combineWith(currentNamespaceSignal) --> {
        case (_, Some(ns)) => reconcile(ns)
        case _ => ()
      },
      // Persist intent to sessionStorage on every change. Intent only changes via a toggle
      // or a server-prune, never via the store being torn down, so this needs no gate.
      enabledTapsVar.signal --> Observer[Map[String, Set[String]]] { m =>
        m.foreach { case (ns, names) => EnabledTapsStorage.save(ns, names) }
      },
      // Poll so tap queries created/deleted server-side surface (and pruned intent
      // persists) without forcing a graph switch.
      EventStream.periodic(intervalMs = 5000).mapTo(()) --> { _ =>
        currentNsVar.now().foreach(ns => fetch(ns))
      },
    )
  }
}
