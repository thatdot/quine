package com.thatdot.quine.webapp.dataservice

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.raquo.airstream.core.{EventStream, Observer, Signal}
import com.raquo.airstream.eventbus.EventBus
import com.raquo.airstream.ownership.ManualOwner
import com.raquo.airstream.state.{Val, Var}
import io.circe.Json
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.routes.{ClientRoutes, MetricsReport, SampleQuery, ShardInMemoryLimit, UiNodeAppearance}
import com.thatdot.quine.v2api.routes.V2UiNodeQuickQuery
import com.thatdot.quine.webapp.util.{Pot, QuineApiClient}
import com.thatdot.quine.webapp.v2api.V2ApiTypes.{
  V2BackpressureSnapshot,
  V2IngestInfo,
  V2ServiceStatus,
  V2StandingQueryInfo,
  V2TapQuery,
}

/** @param useV2Api when false, resources with a V1 twin (the queryUi trio) are read via the
  *                  V1 routes, converted to the V2 shape at the wire boundary. V2-only
  *                  resources are unaffected — they have no V1 equivalent.
  */
class OssDataService(protected val clientRoutes: ClientRoutes, protected val useV2Api: Boolean = true)
    extends DataService {

  // Refresh commands (and successful saves) emit on these buses; the feeds key their poll
  // on the bus, so a tick restarts the poll, which fetches immediately. A refresh is an
  // event, not state — the buses carry no payload; feeds seed their own initial fetch.
  private val standingQueriesRefresh = new EventBus[Unit]
  private val ingestStreamsRefresh = new EventBus[Unit]
  private val sampleQueriesRefresh = new EventBus[Unit]
  private val quickQueriesRefresh = new EventBus[Unit]
  private val nodeAppearancesRefresh = new EventBus[Unit]
  private val tapQueriesRefresh = new EventBus[Unit]

  /** Mirror of the validated current namespace, for imperative reads (mutations act on the
    * graph the user is viewing). Lazily bootstrapped with an owned observer, same as the
    * wiretap runtime and for the same construction-order reason.
    */
  private lazy val currentNamespaceMirror: Var[NamespaceParameter] = {
    val mirror = Var(NamespaceParameter.defaultNamespaceParameter)
    currentNamespaceSignal.foreach(mirror.set)(new ManualOwner)
    mirror
  }

  /** Run a save, then invalidate the matching feed and answer the command's `replyTo`. */
  private def completeSave(
    save: Future[Unit],
    refresh: EventBus[Unit],
    replyTo: Observer[SaveResult],
  ): Unit =
    save.onComplete {
      case Success(_) =>
        refresh.emit(())
        replyTo.onNext(SaveSucceeded)
      case Failure(err) =>
        replyTo.onNext(
          SaveFailed(Option(err.getMessage).filter(_.nonEmpty).getOrElse("request failed")),
        )
    }

  /** Apply a namespace command. OSS has a single fixed graph, so both commands are no-ops;
    * [[EnterpriseDataService]] overrides this with real selection and refetch.
    */
  protected def handleNamespaceCommand(command: NamespaceService.Command): Unit = command match {
    case _: NamespaceService.SetNamespace => () // single fixed graph — selection is a no-op
    case NamespaceService.RefreshNamespaces => () // static single-graph list — nothing to refetch
  }

  lazy val namespaceDispatch: Observer[NamespaceService.Command] = Observer(handleNamespaceCommand)

  /** Whether the signed-in user may GET the tap-query list. When false, both the CRUD list
    * feed and the always-mounted reconcile poll stay empty so roles lacking the permission
    * never 401 against the tap-query endpoint. OSS has no auth, so it is always readable;
    * [[EnterpriseDataService]] overrides this from the user's permissions.
    */
  protected def canReadTapQueries: Boolean = true

  /** Whether the signed-in user may GET the full cluster status (`ClusterStatusRead`). Gates the
    * landing page's cluster-health visuals. OSS has no auth, so it is always readable;
    * [[EnterpriseDataService]] overrides this from the user's permissions.
    */
  protected def canReadClusterStatus: Boolean = true

  /** Whether the signed-in user may read cluster member positions — `ClusterStatusRead` (full
    * status) or `IngestRead` (a trimmed, positions-only status). When false the status feed stays
    * empty so roles lacking both never 403 against the status endpoint — this also empties the
    * derived [[memberIndicesSignal]], which the Streams page binds for its host selector and
    * per-ingest host column. OSS has no auth, so it is always readable; [[EnterpriseDataService]]
    * overrides this from the user's permissions.
    */
  protected def canReadClusterMembers: Boolean = true

  lazy val standingQueryDispatch: Observer[StandingQueryService.Command] = Observer {
    case StandingQueryService.RefreshStandingQueries => standingQueriesRefresh.emit(())
  }

  lazy val ingestStreamDispatch: Observer[IngestStreamService.Command] = Observer {
    case IngestStreamService.RefreshIngestStreams => ingestStreamsRefresh.emit(())
  }

  lazy val queryUiConfigDispatch: Observer[QueryUiConfigService.Command] = Observer {
    case QueryUiConfigService.SaveSampleQueries(sampleQueries, replyTo) =>
      completeSave(
        QuineApiClient.saveSampleQueries(sampleQueries, clientRoutes, useV2Api),
        sampleQueriesRefresh,
        replyTo,
      )
    case QueryUiConfigService.SaveQuickQueries(quickQueries, replyTo) =>
      completeSave(
        QuineApiClient.saveQuickQueries(quickQueries, clientRoutes, useV2Api),
        quickQueriesRefresh,
        replyTo,
      )
    case QueryUiConfigService.SaveNodeAppearances(appearances, replyTo) =>
      completeSave(
        QuineApiClient.saveNodeAppearances(appearances, clientRoutes, useV2Api),
        nodeAppearancesRefresh,
        replyTo,
      )
  }

  lazy val tapQueryDispatch: Observer[TapQueryService.Command] = Observer {
    case TapQueryService.SaveTapQueries(tapQueries, replyTo) =>
      completeSave(
        QuineApiClient.saveTapQueries(currentNamespaceMirror.now(), tapQueries, clientRoutes),
        tapQueriesRefresh,
        replyTo,
      )
  }

  // ── Backpressure ──

  /** The client-side history. The server reports only current state, so the window rates are
    * differenced over exists nowhere else; see [[BackpressureStore]].
    */
  private val backpressureStore: BackpressureStore = new BackpressureStore

  private val requestedScopeVar: Var[BackpressureService.Scope] = Var(BackpressureService.Scope.default)
  private val backpressurePausedVar: Var[Boolean] = Var(false)

  /** The one poll, recorded once.
    *
    * `QuineApiClient.backpressure` builds a *new* polling feed per call, so the several signals
    * below must derive from a single node rather than each calling it — two calls would be two
    * polls. Recording here rather than in each consumer also means the store is written once per
    * response, before anything downstream reads it.
    *
    * Safe to run on every emission, including the re-emissions Airstream produces when a signal is
    * restarted: `record` ignores a snapshot no newer than the one already held for that host. The
    * old RateComputer had no such guard, and a replayed value corrupted the deltas it kept.
    */
  private lazy val recordedBackpressurePoll: Signal[Pot[Seq[V2BackpressureSnapshot]]] =
    QuineApiClient
      .backpressure(clientRoutes)
      .potSignal
      .map { pot =>
        pot.toOption.foreach(backpressureStore.record)
        pot
      }

  /** The view held while paused. Polling and recording continue underneath, so the window keeps
    * filling and resuming is instant — pausing the poll instead would leave a hole in the history.
    */
  private var heldView: Option[BackpressureView] = None

  lazy val backpressureDispatch: Observer[BackpressureService.Command] = Observer {
    case BackpressureService.SetScope(scope, replyTo) =>
      requestedScopeVar.set(scope)
      // Echo the scope that actually took effect: a member that has left the cluster falls back to
      // the whole cluster (the same rule backpressureScopeSignal applies), so the issuer learns what
      // it is really showing. The recompute is instant — every scope resolves history already held —
      // so there is nothing to await before replying.
      replyTo.onNext(effectiveScope(scope, backpressureStore.currentMembers))
    case BackpressureService.PauseUpdates(replyTo) =>
      backpressurePausedVar.set(true)
      replyTo.onNext(())
    case BackpressureService.ResumeUpdates(replyTo) =>
      backpressurePausedVar.set(false)
      replyTo.onNext(())
  }

  /** The scope a request actually resolves to. A member that is not among the currently known cluster
    * members falls back to the whole cluster; an empty member list means OSS, where the selection
    * stands. The single source of the fallback rule, shared by the [[backpressureDispatch]] echo and
    * [[backpressureScopeSignal]].
    */
  private def effectiveScope(requested: BackpressureService.Scope, known: Seq[Int]): BackpressureService.Scope =
    requested match {
      case BackpressureService.Scope.Member(idx) if known.nonEmpty && !known.contains(idx) =>
        BackpressureService.Scope.Cluster
      case other => other
    }

  lazy val listClusterMembers: Signal[Seq[Int]] = backpressureStore.members

  /** The authoritative freeze flag, exposed so a diagram can reflect it on mount. It lives on the
    * service — which outlives the diagram — so pausing, leaving the page, and returning shows the
    * diagram still paused rather than silently live over a frozen view.
    */
  lazy val backpressurePausedSignal: Signal[Boolean] = backpressurePausedVar.signal

  /** Recomputed from the store on each poll, and deliberately *not* held while paused: the picker is
    * a navigation control, not part of the frozen picture. Pausing to read the diagram should not
    * also stop the page from telling you that another member has just fallen over.
    */
  lazy val memberStatusSignal: Signal[Map[Int, MemberStatus]] =
    recordedBackpressurePoll.map(_ => backpressureStore.statusByMember).distinct

  /** The requested scope, validated against the members that actually exist. A member that leaves the
    * cluster falls back to the whole cluster rather than rendering an empty diagram — the same
    * containment check [[NamespaceService.currentNamespaceSignal]] applies to a namespace that has
    * been deleted out from under the selection.
    *
    * An empty member list means OSS (no members at all), not "the chosen one is gone", so the
    * selection is left alone in that case.
    */
  lazy val backpressureScopeSignal: Signal[BackpressureService.Scope] =
    requestedScopeVar.signal
      .combineWith(listClusterMembers)
      .map { case (requested, known) => effectiveScope(requested, known) }
      .distinct

  lazy val backpressureSnapshotSignal: Signal[Pot[BackpressureView]] =
    recordedBackpressurePoll
      .combineWith(backpressureScopeSignal, backpressurePausedVar.signal)
      .map { case (pot, scope, paused) =>
        val resolved: Option[BackpressureView] =
          if (paused) heldView
          else {
            val next = backpressureStore.view(scope)
            next.foreach(v => heldView = Some(v))
            next
          }

        // History outlives a failed poll, so a fetch error degrades to the last good view rather
        // than blanking the diagram.
        (resolved, pot) match {
          case (Some(view), Pot.Ready(_) | Pot.Empty) => Pot.Ready(view)
          case (Some(view), Pot.PendingStale(_) | Pot.Pending) => Pot.PendingStale(view)
          case (Some(view), Pot.Failed(err)) => Pot.FailedStale(view, err)
          case (Some(view), Pot.FailedStale(_, err)) => Pot.FailedStale(view, err)
          case (None, Pot.Failed(err)) => Pot.Failed(err)
          case (None, Pot.FailedStale(_, err)) => Pot.Failed(err)
          case (None, Pot.Empty) => Pot.Empty
          case (None, _) => Pot.Pending
        }
      }

  lazy val namespacesSignal: Signal[Seq[NamespaceParameter]] = Val(
    Seq(NamespaceParameter.defaultNamespaceParameter),
  )

  lazy val currentNamespaceSignal: Signal[NamespaceParameter] = Val(NamespaceParameter.defaultNamespaceParameter)

  lazy val standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]] =
    currentNamespaceSignal.flatMapSwitch { ns =>
      standingQueriesRefresh.events
        .startWith(())
        .flatMapSwitch(_ => QuineApiClient.standingQueries(ns.namespaceId, clientRoutes).potSignal)
    }.distinct

  lazy val tapQueriesSignal: Signal[Vector[V2TapQuery]] =
    if (!canReadTapQueries) Val(Vector.empty)
    else
      currentNamespaceSignal.flatMapSwitch { ns =>
        tapQueriesRefresh.events
          .startWith(())
          .flatMapSwitch(_ => QuineApiClient.tapQueries(ns.namespaceId, clientRoutes).values.startWith(Vector.empty))
      }.distinct

  lazy val clusterStatusSignal: Signal[Pot[V2ServiceStatus]] =
    if (!canReadClusterStatus && !canReadClusterMembers) Val(Pot.Empty)
    else QuineApiClient.clusterStatus(clientRoutes).potSignal

  private val metricsFeeds = mutable.Map.empty[Option[Int], Signal[Pot[MetricsReport]]]

  def metricsSignal(member: Option[Int]): Signal[Pot[MetricsReport]] =
    metricsFeeds.getOrElseUpdate(
      member,
      QuineApiClient.metrics(member, clientRoutes, useV2Api).potSignal,
    )

  private val shardSizeLimitsFeeds = mutable.Map.empty[Option[Int], Signal[Pot[Map[Int, ShardInMemoryLimit]]]]

  def shardSizeLimitsSignal(member: Option[Int]): Signal[Pot[Map[Int, ShardInMemoryLimit]]] =
    shardSizeLimitsFeeds.getOrElseUpdate(
      member,
      QuineApiClient.shardSizeLimits(member, clientRoutes, useV2Api).potSignal,
    )

  // The trio feeds restart on their refresh buses via merge-of-initial-and-refresh, seeding
  // the initial fetch with `fromValue(())` (the bus itself has no initial event) and seeding
  // `startWith` only on the outermost signal — a save-triggered refetch keeps the last list
  // until fresh data arrives instead of flashing empty.
  lazy val sampleQueriesSignal: Signal[Vector[SampleQuery]] =
    EventStream
      .merge(EventStream.fromValue(()), sampleQueriesRefresh.events)
      .flatMapSwitch(_ => QuineApiClient.sampleQueries(clientRoutes, useV2Api).values)
      .startWith(Vector.empty)
      .distinct

  lazy val quickQueriesSignal: Signal[Vector[V2UiNodeQuickQuery]] =
    EventStream
      .merge(EventStream.fromValue(()), quickQueriesRefresh.events)
      .flatMapSwitch(_ => QuineApiClient.quickQueries(clientRoutes, useV2Api).values)
      .startWith(Vector.empty)
      .distinct

  lazy val nodeAppearancesSignal: Signal[Vector[UiNodeAppearance]] =
    EventStream
      .merge(EventStream.fromValue(()), nodeAppearancesRefresh.events)
      .flatMapSwitch(_ => QuineApiClient.nodeAppearances(clientRoutes, useV2Api).values)
      .startWith(Vector.empty)
      .distinct

  /** Per-tab persistence of which tap queries are enabled locally, one sessionStorage
    * entry per graph namespace — a tab restores its own enabled taps on reload while two
    * tabs can have different taps enabled at once.
    */
  private object EnabledTapsStorage {
    private val KeyPrefix = "thatdot.explorer.enabledTaps."

    def load(namespace: String): Set[String] =
      try Option(dom.window.sessionStorage.getItem(KeyPrefix + namespace))
        .filter(_.nonEmpty)
        .flatMap(io.circe.parser.decode[Vector[String]](_).toOption)
        .map(_.toSet)
        .getOrElse(Set.empty)
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"Failed to load enabled taps from sessionStorage: ${e.getMessage}")
          Set.empty
      }

    def save(namespace: String, names: Set[String]): Unit =
      try if (names.isEmpty) dom.window.sessionStorage.removeItem(KeyPrefix + namespace)
      else
        dom.window.sessionStorage.setItem(
          KeyPrefix + namespace,
          Json.fromValues(names.toList.sorted.map(Json.fromString)).noSpaces,
        )
      catch {
        case e: scala.scalajs.js.JavaScriptException =>
          dom.console.warn(s"Failed to save enabled taps to sessionStorage: ${e.getMessage}")
      }
  }

  /** Wiretap runtime: one [[WiretapStore]] per graph namespace, renewed when the current
    * namespace changes (the old namespace's sockets close). "Enable locally" tap-query
    * intent is persisted per graph in sessionStorage and reconciled against the server's
    * tap-query list, so enabled taps restore on reload or graph revisit and follow
    * server-side edits without a re-toggle. Bootstrapped lazily by the first use of any
    * wiretap member — construction must not force `currentNamespaceSignal`, which
    * subclasses override with members that do not exist until their own initialization
    * runs. Once bootstrapped, the service itself owns the namespace subscription: the one
    * deliberate departure from "no work until a consumer subscribes", safe because the
    * app root holds exactly one service for the app's lifetime.
    */
  final private class WiretapRuntime {
    private val owner = new ManualOwner

    val storeVar: Var[Option[WiretapStore]] = Var(None)
    val enabledTapQueriesVar: Var[Map[String, V2TapQuery]] = Var(Map.empty)

    // "Enable locally" intent for the current namespace: tap-query names only. The
    // metadata map above is filled from the toggle's payload or the server list, so the
    // dispatch host always acts on the freshest definition.
    private val enabledIntentVar: Var[Set[String]] = Var(Set.empty)
    private var currentNs: String = NamespaceParameter.defaultNamespaceParameter.namespaceId

    // Fires synchronously with the current namespace on subscription, so `storeVar` holds
    // a store from the moment the runtime exists. Restores the new namespace's persisted
    // intent; reconcile reopens its taps once the server list arrives.
    currentNamespaceSignal.foreach { ns =>
      storeVar.now().foreach(_.closeAll())
      enabledTapQueriesVar.set(Map.empty)
      currentNs = ns.namespaceId
      storeVar.set(Some(new WiretapStore(ns.namespaceId, clientRoutes)))
      enabledIntentVar.set(EnabledTapsStorage.load(ns.namespaceId))
    }(owner)

    // Persist intent per namespace on every change (toggles, restores, server prunes).
    enabledIntentVar.signal.foreach(names => EnabledTapsStorage.save(currentNs, names))(owner)

    // The server's tap-query list for the current namespace, tagged with the namespace it
    // belongs to (so one graph's intent is never reconciled against another's list) and
    // polled only while some tap is enabled — no intent, no fetch.
    private val tapListSignal: Signal[Option[(String, Vector[V2TapQuery])]] =
      enabledIntentVar.signal
        .map(_.nonEmpty)
        .distinct
        .combineWith(currentNamespaceSignal)
        .flatMapSwitch {
          case (true, ns) if canReadTapQueries =>
            QuineApiClient
              .tapQueries(ns.namespaceId, clientRoutes)
              .values
              .map(tapQueries => Option(ns.namespaceId -> tapQueries))
              .startWith(None)
          case _ => Val(None)
        }

    enabledIntentVar.signal
      .combineWith(tapListSignal)
      .foreach {
        case (want, Some((ns, tapQueries))) if ns == currentNs => reconcile(want, tapQueries)
        case _ => ()
      }(owner)

    /** Bring open handlers (and the metadata the dispatch host reads) in line with intent
      * and the server list: open newly-wanted taps, close no-longer-wanted ones, prune
      * intent of taps the server dropped, refresh metadata for edited queries, and reopen
      * a tap only when its tap point (SQ/output) actually moved — an edit to the query
      * alone reuses the open handler.
      */
    private def reconcile(want: Set[String], tapQueries: Vector[V2TapQuery]): Unit =
      storeVar.now().foreach { store =>
        val byName = tapQueries.iterator.map(t => t.name -> t).toMap
        val pruned = want.intersect(byName.keySet)
        if (pruned != want) enabledIntentVar.set(pruned) // re-enters with the pruned set
        else {
          val have = store.activeKeys(WiretapService.TapQueryOwner)
          have.diff(want).foreach { name =>
            store.close(WiretapService.TapQueryOwner, name)
            enabledTapQueriesVar.update(_ - name)
          }
          want.diff(have).foreach { name =>
            byName.get(name).foreach { t =>
              enabledTapQueriesVar.update(_ + (name -> t))
              store.open(
                WiretapService.TapQueryOwner,
                name,
                t.standingQueryName,
                WiretapTapPoint.fromOutputName(t.outputName),
              )
            }
          }
          want.intersect(have).foreach { name =>
            byName.get(name).foreach { t =>
              val prev = enabledTapQueriesVar.now().get(name)
              if (!prev.contains(t)) enabledTapQueriesVar.update(_ + (name -> t))
              val tapPointMoved =
                prev.exists(p => p.standingQueryName != t.standingQueryName || p.outputName != t.outputName)
              if (tapPointMoved) {
                store.close(WiretapService.TapQueryOwner, name)
                store.open(
                  WiretapService.TapQueryOwner,
                  name,
                  t.standingQueryName,
                  WiretapTapPoint.fromOutputName(t.outputName),
                )
              }
            }
          }
        }
      }

    /** Record intent and open immediately — the toggle carries the full tap query, so
      * there is nothing to wait for. Reconcile keeps the tap current afterwards.
      */
    def enable(tapQuery: V2TapQuery): Unit = {
      enabledTapQueriesVar.update(_ + (tapQuery.name -> tapQuery))
      enabledIntentVar.update(_ + tapQuery.name)
      storeVar
        .now()
        .foreach(
          _.open(
            WiretapService.TapQueryOwner,
            tapQuery.name,
            tapQuery.standingQueryName,
            WiretapTapPoint.fromOutputName(tapQuery.outputName),
          ),
        )
    }

    def disable(name: String): Unit = {
      enabledIntentVar.update(_ - name)
      enabledTapQueriesVar.update(_ - name)
      storeVar.now().foreach(_.close(WiretapService.TapQueryOwner, name))
    }
  }

  private lazy val wiretapRuntime: WiretapRuntime = new WiretapRuntime

  lazy val wiretapDispatch: Observer[WiretapService.Command] = Observer {
    case WiretapService.OpenTap(tapOwner, key, sqName, tapPoint) =>
      wiretapRuntime.storeVar.now().foreach(_.open(tapOwner, key, sqName, tapPoint))
    case WiretapService.CloseTap(tapOwner, key) =>
      wiretapRuntime.storeVar.now().foreach(_.close(tapOwner, key))
    case WiretapService.EnableTapQuery(tapQuery) =>
      wiretapRuntime.enable(tapQuery)
    case WiretapService.DisableTapQuery(name) =>
      wiretapRuntime.disable(name)
  }

  lazy val wiretapsSignal: Signal[Map[WiretapOwner, List[WiretapHandler]]] =
    wiretapRuntime.storeVar.signal.flatMapSwitch {
      case Some(store) => store.active
      case None => Val(Map.empty[WiretapOwner, List[WiretapHandler]])
    }

  lazy val enabledTapQueriesSignal: Signal[Map[String, V2TapQuery]] =
    wiretapRuntime.enabledTapQueriesVar.signal

  lazy val ingestStreamsSignal: Signal[Pot[Seq[V2IngestInfo]]] =
    currentNamespaceSignal.flatMapSwitch { ns =>
      ingestStreamsRefresh.events
        .startWith(())
        .flatMapSwitch(_ => QuineApiClient.ingestStreams(ns.namespaceId, clientRoutes).potSignal)
    }.distinct

}
