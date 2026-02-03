package com.thatdot.quine.graph.cypher.quinepattern

import scala.collection.mutable

import org.apache.pekko.actor.{Actor, ActorRef}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, StandingQueryResult}
import com.thatdot.quine.model.{HalfEdge, PropertyValue}

/** Logger for QuinePattern warnings and errors */
private[quinepattern] object QPLog extends LazySafeLogging {
  implicit val logConfig: LogConfig = LogConfig.permissive

  def warn(message: String): Unit = logger.warn(safe"${Safe(message)}")
}

/** Simple metrics for understanding QuinePattern performance.
  * Enable via system property: -Dqp.metrics=true
  * Configure interval: -Dqp.metrics.interval=10 (seconds, default 10)
  */
object QPMetrics {
  val enabled: Boolean = sys.props.getOrElse("qp.metrics", "false").toBoolean
  private val intervalSeconds: Int = sys.props.getOrElse("qp.metrics.interval", "10").toInt
  private val outputFile: Option[String] = sys.props.get("qp.metrics.file")
  // Filter by mode: "eager" or "lazy" (default: track all)
  private val filterMode: Option[String] = sys.props.get("qp.metrics.mode").map(_.toLowerCase)

  def shouldTrack(mode: RuntimeMode): Boolean = filterMode match {
    case Some("eager") => mode == RuntimeMode.Eager
    case Some("lazy") => mode == RuntimeMode.Lazy
    case _ => true
  }

  // Simple file writer
  private val writer: Option[java.io.PrintWriter] = outputFile.map { path =>
    new java.io.PrintWriter(new java.io.FileWriter(path, true), true) // append mode, autoflush
  }

  private def log(msg: String): Unit = writer match {
    case Some(w) => w.println(msg)
    case None => println(msg)
  }

  private val eventsHandled = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalStatesScanned = new java.util.concurrent.atomic.AtomicLong(0)
  private val propertyNotifications = new java.util.concurrent.atomic.AtomicLong(0)
  private val edgeNotifications = new java.util.concurrent.atomic.AtomicLong(0)
  private val labelNotifications = new java.util.concurrent.atomic.AtomicLong(0)
  private val stateInstalls = new java.util.concurrent.atomic.AtomicLong(0)
  private val stateUninstalls = new java.util.concurrent.atomic.AtomicLong(0)
  private val maxStatesPerNode = new java.util.concurrent.atomic.AtomicLong(0)
  private val loadQueryPlanCalls = new java.util.concurrent.atomic.AtomicLong(0)
  private val uniqueNodesWithStates = java.util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean](),
  )

  // For rate calculation
  private val lastPrintTime = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis())
  private val lastEventsHandled = new java.util.concurrent.atomic.AtomicLong(0)

  // Start periodic printing if enabled
  if (enabled) {
    val scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor { r =>
      val t = new Thread(r, "qp-metrics-printer")
      t.setDaemon(true)
      t
    }
    val _ = scheduler.scheduleAtFixedRate(
      () => printSummary(),
      intervalSeconds.toLong,
      intervalSeconds.toLong,
      java.util.concurrent.TimeUnit.SECONDS,
    )
  }

  def eventHandled(): Unit = if (enabled) { val _ = eventsHandled.incrementAndGet() }
  def statesScanned(count: Int): Unit = if (enabled) {
    val _ = totalStatesScanned.addAndGet(count.toLong)
    var current = maxStatesPerNode.get()
    val countLong = count.toLong
    while (countLong > current && !maxStatesPerNode.compareAndSet(current, countLong))
      current = maxStatesPerNode.get()
  }
  def propertyNotification(): Unit = if (enabled) { val _ = propertyNotifications.incrementAndGet() }
  def edgeNotification(): Unit = if (enabled) { val _ = edgeNotifications.incrementAndGet() }
  def labelNotification(): Unit = if (enabled) { val _ = labelNotifications.incrementAndGet() }
  def stateInstalled(mode: RuntimeMode): Unit =
    if (enabled && shouldTrack(mode)) { val _ = stateInstalls.incrementAndGet() }
  def stateUninstalled(mode: RuntimeMode): Unit =
    if (enabled && shouldTrack(mode)) { val _ = stateUninstalls.incrementAndGet() }
  private val nodeWakeMessages = new java.util.concurrent.atomic.AtomicLong(0)
  private val dispatchToTargetCalls = new java.util.concurrent.atomic.AtomicLong(0)

  def loadQueryPlanCalled(nodeId: Option[com.thatdot.common.quineid.QuineId]): Unit =
    if (enabled) {
      val _ = loadQueryPlanCalls.incrementAndGet()
      nodeId.foreach(id => uniqueNodesWithStates.add(id.toString))
    }

  def nodeWakeReceived(): Unit = if (enabled) { val _ = nodeWakeMessages.incrementAndGet() }
  def dispatchToTargetCalled(): Unit = if (enabled) { val _ = dispatchToTargetCalls.incrementAndGet() }

  def printSummary(): Unit = {
    val now = System.currentTimeMillis()
    val events = eventsHandled.get()
    val scanned = totalStatesScanned.get()
    val avgStatesPerEvent = if (events > 0) scanned.toDouble / events else 0.0

    // Calculate rate since last print
    val lastTime = lastPrintTime.getAndSet(now)
    val lastEvents = lastEventsHandled.getAndSet(events)
    val elapsedSec = (now - lastTime) / 1000.0
    val eventsPerSec = if (elapsedSec > 0) (events - lastEvents) / elapsedSec else 0.0

    val installs = stateInstalls.get()
    val uninstalls = stateUninstalls.get()
    val netStates = installs - uninstalls
    val timestamp = java.time.LocalDateTime.now().toString
    log(s"""
      |=== QuinePattern Metrics @ $timestamp ===
      |Events handled:        $events (${"%,.0f".format(eventsPerSec)}/sec)
      |Total states scanned:  $scanned
      |Avg states per event:  ${"%.2f".format(avgStatesPerEvent)}
      |Max states on a node:  ${maxStatesPerNode.get()}
      |Property notifications: ${propertyNotifications.get()}
      |Edge notifications:    ${edgeNotifications.get()}
      |Label notifications:   ${labelNotifications.get()}
      |State installs:        $installs
      |State uninstalls:      $uninstalls
      |Net states (leak?):    $netStates
      |====================
    """.stripMargin)
  }

  def reset(): Unit = {
    eventsHandled.set(0)
    totalStatesScanned.set(0)
    propertyNotifications.set(0)
    edgeNotifications.set(0)
    labelNotifications.set(0)
    stateInstalls.set(0)
    stateUninstalls.set(0)
    maxStatesPerNode.set(0)
    lastPrintTime.set(System.currentTimeMillis())
    lastEventsHandled.set(0)
  }
}

/** Tracing utility for debugging QuinePattern state notification flow.
  * Enable via system property: -Dqp.trace=true
  */
object QPTrace {
  val enabled: Boolean =
    sys.props.getOrElse("qp.trace", "false").toBoolean

  def log(msg: => String): Unit =
    if (enabled) System.err.println(s"[QP TRACE] $msg")

  def stateInstalled(nodeId: Option[QuineId], stateId: StandingQueryId, stateType: String): Unit =
    log(s"INSTALL node=${nodeId.getOrElse("none")} state=$stateId type=$stateType")

  def stateConnection(
    stateId: StandingQueryId,
    stateType: String,
    publishTo: StandingQueryId,
    extra: String = "",
  ): Unit =
    log(s"CONNECTION state=$stateId type=$stateType publishTo=$publishTo$extra")

  def stateKickstart(nodeId: Option[QuineId], stateId: StandingQueryId, stateType: String): Unit =
    log(s"KICKSTART node=${nodeId.getOrElse("none")} state=$stateId type=$stateType")

  def stateNotify(
    nodeId: Option[QuineId],
    stateId: StandingQueryId,
    stateType: String,
    from: StandingQueryId,
    deltaSize: Int,
  ): Unit =
    log(s"NOTIFY node=${nodeId.getOrElse("none")} state=$stateId type=$stateType from=$from deltaSize=$deltaSize")

  def stateEmit(
    nodeId: Option[QuineId],
    stateId: StandingQueryId,
    stateType: String,
    to: StandingQueryId,
    deltaSize: Int,
  ): Unit =
    log(s"EMIT node=${nodeId.getOrElse("none")} state=$stateId type=$stateType to=$to deltaSize=$deltaSize")

  def notificationDropped(
    targetId: StandingQueryId,
    fromId: StandingQueryId,
    deltaSize: Int,
    hostedStates: Iterable[StandingQueryId],
  ): Unit =
    log(s"DROPPED target=$targetId from=$fromId deltaSize=$deltaSize hosted=[${hostedStates.mkString(",")}]")

  def dispatchToNode(fromNode: Option[QuineId], toNode: QuineId, stateId: StandingQueryId, planType: String): Unit =
    log(s"DISPATCH from=${fromNode.getOrElse("none")} to=$toNode state=$stateId plan=$planType")
}

/** Hosts query state machines within an Actor.
  *
  * This trait provides the Actor integration layer for QueryStateBuilder.
  * It takes a pure StateGraph and instantiates actual runtime states,
  * managing their lifecycle and message routing.
  *
  * Separation of concerns:
  *   - QueryStateBuilder: Pure logic to build StateGraph from QueryPlan
  *   - QueryStateHost: Actor integration, lifecycle, message routing
  *   - QueryState: Actual state machine implementations
  *
  * ==========================================================================
  * STATE LIFECYCLE LIMITATIONS (Future Work Required)
  * ==========================================================================
  *
  * Currently, QuinePattern states are NOT persisted. This creates several issues:
  *
  * 1. NODE SLEEP/WAKE STATE LOSS:
  *    - When a node sleeps, its hostedStates are lost (in-memory only)
  *    - On wake, there's no mechanism to restore states
  *    - For AllNodes anchors: NodeWake fires but targetResults.contains(nodeId)
  *      prevents re-dispatch since Anchor thinks it already dispatched
  *    - For Computed anchors: No hook, state is lost permanently
  *    - Result: Updates silently stop flowing from slept/woken nodes
  *
  * 2. STANDING QUERY UNREGISTRATION:
  *    - Currently uses "soft unregister": just removes from local hostedStates
  *    - Remote child states become orphaned (no UnregisterState sent to them)
  *    - Orphaned states may continue sending updates to non-existent parents
  *    - Updates to non-existent states are silently dropped (see routeNotification)
  *
  * 3. NODE HOOK MEMORY LEAK:
  *    - AnchorState registers NodeWakeHooks but never unregisters them
  *    - See cleanup() in AnchorState for details
  *
  * FUTURE VISION (similar to MultipleValuesStandingQuery):
  *
  * 1. State Persistence:
  *    - Serialize states to persistor on install
  *    - Restore states on node wake
  *    - Enables proper state continuity across sleep/wake cycles
  *
  * 2. Standing Query Unregistration:
  *    - Partial: Remove specific states while keeping query active
  *    - Total: Remove entire standing query and all its states
  *    - Challenge: Avoid waking entire subgraphs just for cleanup
  *
  * 3. Lazy Cleanup Strategy:
  *    - Don't eagerly wake nodes to send UnregisterState
  *    - Instead, have nodes validate on wake: "is this state still relevant?"
  *    - Could use a global standing query registry for validation
  *    - Orphaned states self-cleanup when they detect parent is gone
  *
  * 4. Epoch/Generation Tracking:
  *    - Each standing query deployment has a generation number
  *    - States check generation before publishing results
  *    - Stale states (wrong generation) self-terminate
  */
trait QueryStateHost { this: Actor =>

  /** All hosted states indexed by their ID */
  protected val hostedStates: mutable.Map[StandingQueryId, QueryState] =
    mutable.Map.empty[StandingQueryId, QueryState]

  // ============================================================
  // EVENT INDEX - O(1) routing instead of O(n) scanning
  // ============================================================

  /** States watching specific property keys */
  protected val propertyWatchers: mutable.Map[Symbol, mutable.Set[StandingQueryId]] =
    mutable.Map.empty

  /** States watching ALL property changes */
  protected val allPropertyWatchers: mutable.Set[StandingQueryId] =
    mutable.Set.empty

  /** States watching specific edge types */
  protected val edgeWatchers: mutable.Map[Symbol, mutable.Set[StandingQueryId]] =
    mutable.Map.empty

  /** States watching ALL edge types */
  protected val allEdgeWatchers: mutable.Set[StandingQueryId] =
    mutable.Set.empty

  /** States watching label changes */
  protected val labelWatchers: mutable.Set[StandingQueryId] =
    mutable.Set.empty

  /** Register a state in the event index */
  private def registerInEventIndex(state: QueryState): Unit = {
    val id = state.id

    // Register for property events
    state match {
      case ps: PropertySensitiveState =>
        ps.watchedPropertyKeys match {
          case Some(keys) =>
            keys.foreach { key =>
              propertyWatchers.getOrElseUpdate(key, mutable.Set.empty) += id
            }
          case None =>
            allPropertyWatchers += id
        }
      case _ => ()
    }

    // Register for edge events
    state match {
      case es: EdgeSensitiveState =>
        es.watchedEdgeLabel match {
          case Some(label) =>
            edgeWatchers.getOrElseUpdate(label, mutable.Set.empty) += id
          case None =>
            allEdgeWatchers += id
        }
      case _ => ()
    }

    // Register for label events
    state match {
      case _: LabelSensitiveState =>
        labelWatchers += id
      case _ => ()
    }
  }

  /** Unregister a state from the event index */
  private def unregisterFromEventIndex(state: QueryState): Unit = {
    val id = state.id
    // Remove from property watchers
    state match {
      case ps: PropertySensitiveState =>
        ps.watchedPropertyKeys match {
          case Some(keys) =>
            keys.foreach { key =>
              propertyWatchers.get(key).foreach { set =>
                set -= id
                if (set.isEmpty) propertyWatchers -= key
              }
            }
          case None =>
            allPropertyWatchers -= id
        }
      case _ => ()
    }
    // Remove from edge watchers
    state match {
      case es: EdgeSensitiveState =>
        es.watchedEdgeLabel match {
          case Some(label) =>
            edgeWatchers.get(label).foreach { set =>
              set -= id
              if (set.isEmpty) edgeWatchers -= label
            }
          case None =>
            allEdgeWatchers -= id
        }
      case _ => ()
    }
    // Remove from label watchers
    state match {
      case _: LabelSensitiveState =>
        labelWatchers -= id
      case _ => ()
    }
  }

  /** Install a StateGraph, instantiating all states.
    *
    * @param graph The state graph to install
    * @param instantiator Creates actual states from descriptors
    * @param initialContext Initial node context for kickstarting
    * @return The root state ID
    */
  def installStateGraph(
    graph: StateGraph,
    instantiator: StateInstantiator,
    initialContext: NodeContext,
  ): StandingQueryId = {
    // Instantiate all states from descriptors, passing injectedContext for seeding
    graph.states.foreach { case (id, descriptor) =>
      val state = instantiator.instantiate(descriptor, graph, initialContext, graph.injectedContext)
      hostedStates += (id -> state)
      registerInEventIndex(state) // Register in event index for O(1) routing
      QPMetrics.stateInstalled(state.mode)
      QPTrace.stateInstalled(initialContext.quineId, id, state.getClass.getSimpleName)

      // Wire up host reference for direct local routing (bypasses mailbox)
      // and log state connections for debugging
      state match {
        case p: PublishingState =>
          p.setHost(this)
          QPTrace.stateConnection(id, state.getClass.getSimpleName, p.publishTo)
        case _ => ()
      }
    }

    QPTrace.log(s"GRAPH leaves=[${graph.leaves.mkString(",")}] root=${graph.rootId}")

    // Kickstart leaf states
    graph.leaves.foreach { leafId =>
      hostedStates.get(leafId).foreach { state =>
        QPTrace.stateKickstart(initialContext.quineId, leafId, state.getClass.getSimpleName)
        state.kickstart(initialContext, self)
      }
    }

    graph.rootId
  }

  /** Uninstall all states for a query */
  def uninstallStateGraph(rootId: StandingQueryId): Unit = {
    // Find all states belonging to this graph (traverse from root)
    val toRemove = collectDescendants(rootId)
    toRemove.foreach { id =>
      hostedStates.remove(id).foreach(unregisterFromEventIndex)
    }
  }

  /** Route a notification to the appropriate state */
  def routeNotification(
    targetId: StandingQueryId,
    fromId: StandingQueryId,
    delta: Delta.T,
  ): Unit =
    hostedStates.get(targetId) match {
      case Some(state) =>
        QPTrace.stateNotify(None, targetId, state.getClass.getSimpleName, fromId, delta.size)
        state.notify(delta, fromId, self)
      case None =>
        // State not found - expected in several cases:
        // 1. Eager mode: states unregister after emitting, late notifications are normal
        // 2. Soft unregister: parent state was removed but orphaned child states still send updates
        // 3. Node sleep/wake: states are not persisted, so updates from re-woken nodes may
        //    arrive for states that no longer exist
        // Silently dropping these is intentional - see QueryStateHost trait docs for details.
        QPTrace.notificationDropped(targetId, fromId, delta.size, hostedStates.keys)
    }

  /** Handle a graph event (property change, edge change, etc.)
    *
    * Uses indexed routing for O(1) lookup instead of O(n) scan of all states.
    */
  def handleGraphEvent(event: GraphEvent): Unit = {
    QPMetrics.eventHandled()

    event match {
      case GraphEvent.PropertyChanged(key, oldValue, newValue) =>
        // Get states watching this specific property + states watching all properties
        val watchersForKey = propertyWatchers.getOrElse(key, Set.empty)
        val allWatchers = allPropertyWatchers
        val statesNotified = watchersForKey.size + allWatchers.size
        QPMetrics.statesScanned(statesNotified)

        // Notify states watching this specific property
        watchersForKey.foreach { id =>
          hostedStates.get(id).foreach {
            case ps: PropertySensitiveState =>
              QPMetrics.propertyNotification()
              ps.onPropertyChange(key, oldValue, newValue, self)
            case _ => ()
          }
        }

        // Notify states watching all properties
        allWatchers.foreach { id =>
          hostedStates.get(id).foreach {
            case ps: PropertySensitiveState =>
              QPMetrics.propertyNotification()
              ps.onPropertyChange(key, oldValue, newValue, self)
            case _ => ()
          }
        }

      case GraphEvent.EdgeAdded(edge) =>
        // Get states watching this specific edge type + states watching all edges
        val watchersForType = edgeWatchers.getOrElse(edge.edgeType, Set.empty)
        val allWatchers = allEdgeWatchers
        val statesNotified = watchersForType.size + allWatchers.size
        QPMetrics.statesScanned(statesNotified)
        QPTrace.log(
          s"handleGraphEvent EdgeAdded(${edge.edgeType.name}, ${edge.direction}) indexedWatchers=$statesNotified",
        )

        // Notify states watching this specific edge type
        watchersForType.foreach { id =>
          hostedStates.get(id).foreach {
            case es: EdgeSensitiveState =>
              QPMetrics.edgeNotification()
              QPTrace.log(s"Calling onEdgeAdded on $id type=${es.getClass.getSimpleName}")
              es.onEdgeAdded(edge, self)
            case _ => ()
          }
        }

        // Notify states watching all edges
        allWatchers.foreach { id =>
          hostedStates.get(id).foreach {
            case es: EdgeSensitiveState =>
              QPMetrics.edgeNotification()
              QPTrace.log(s"Calling onEdgeAdded on $id type=${es.getClass.getSimpleName}")
              es.onEdgeAdded(edge, self)
            case _ => ()
          }
        }

      case GraphEvent.EdgeRemoved(edge) =>
        // Get states watching this specific edge type + states watching all edges
        val watchersForType = edgeWatchers.getOrElse(edge.edgeType, Set.empty)
        val allWatchers = allEdgeWatchers
        val statesNotified = watchersForType.size + allWatchers.size
        QPMetrics.statesScanned(statesNotified)
        QPTrace.log(
          s"handleGraphEvent EdgeRemoved(${edge.edgeType.name}, ${edge.direction}) indexedWatchers=$statesNotified",
        )

        // Notify states watching this specific edge type
        watchersForType.foreach { id =>
          hostedStates.get(id).foreach {
            case es: EdgeSensitiveState =>
              QPMetrics.edgeNotification()
              es.onEdgeRemoved(edge, self)
            case _ => ()
          }
        }

        // Notify states watching all edges
        allWatchers.foreach { id =>
          hostedStates.get(id).foreach {
            case es: EdgeSensitiveState =>
              QPMetrics.edgeNotification()
              es.onEdgeRemoved(edge, self)
            case _ => ()
          }
        }

      case GraphEvent.LabelsChanged(oldLabels, newLabels) =>
        val statesNotified = labelWatchers.size
        QPMetrics.statesScanned(statesNotified)

        // Notify all label watchers
        labelWatchers.foreach { id =>
          hostedStates.get(id).foreach {
            case ls: LabelSensitiveState =>
              QPMetrics.labelNotification()
              ls.onLabelsChanged(oldLabels, newLabels, self)
            case _ => ()
          }
        }
    }
  }

  private def collectDescendants(rootId: StandingQueryId): Set[StandingQueryId] = {
    val visited = mutable.Set.empty[StandingQueryId]
    val queue = mutable.Queue(rootId)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current)) {
        visited += current
        // Find children by looking at states that publish to current
        hostedStates.foreach { case (id, state) =>
          state match {
            case ps: PublishingState if ps.publishTo == current && !visited.contains(id) =>
              queue.enqueue(id)
            case _ => ()
          }
        }
      }
    }
    visited.toSet
  }
}

/** Delta operations for query states.
  *
  * Delta maps query context to multiplicity:
  *   - Positive multiplicity = assertion (result added)
  *   - Negative multiplicity = retraction (result removed)
  *   - Zero multiplicity entries are removed
  */
object Delta {

  /** The delta type: Map from QueryContext to multiplicity */
  type T = Map[QueryContext, Int]

  val empty: T = Map.empty
  val unit: T = Map(QueryContext.empty -> 1)

  /** Add two deltas together */
  def add(a: T, b: T): T =
    if (b.isEmpty) a
    else if (a.isEmpty) b
    else {
      val result = mutable.Map.from(a)
      b.foreach { case (ctx, mult) =>
        val newMult = result.getOrElse(ctx, 0) + mult
        if (newMult == 0) result -= ctx
        else result.update(ctx, newMult)
      }
      result.toMap
    }

  /** Subtract delta b from a */
  def subtract(a: T, b: T): T =
    if (b.isEmpty) a
    else add(a, b.view.mapValues(-_).toMap)

  /** Cross-product of two deltas.
    *
    * Optimized with fast paths:
    * - Empty × anything = empty
    * - Unit × anything = anything (unit is identity for cross-product)
    */
  def crossProduct(a: T, b: T): T =
    // Fast path: empty cross anything is empty
    if (a.isEmpty || b.isEmpty) empty
    // Fast path: unit is identity element for cross-product
    else if (a eq unit) b
    else if (b eq unit) a
    else
      for {
        (ctxA, multA) <- a
        (ctxB, multB) <- b
        product = multA * multB
        if product != 0
      } yield (ctxA ++ ctxB, product)
}

/** Context about the current node for state initialization */
case class NodeContext(
  quineId: Option[QuineId],
  properties: Map[Symbol, PropertyValue],
  edges: Set[HalfEdge],
  labels: Set[Symbol],
  graph: Option[
    com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph with com.thatdot.quine.graph.StandingQueryOpsGraph,
  ],
  namespace: Option[com.thatdot.quine.graph.NamespaceId],
)

object NodeContext {
  val empty: NodeContext = NodeContext(None, Map.empty, Set.empty, Set.empty, None, None)
}

/** Events from the graph that states may react to */
sealed trait GraphEvent

object GraphEvent {
  case class PropertyChanged(key: Symbol, oldValue: Option[PropertyValue], newValue: Option[PropertyValue])
      extends GraphEvent
  case class EdgeAdded(edge: HalfEdge) extends GraphEvent
  case class EdgeRemoved(edge: HalfEdge) extends GraphEvent
  case class LabelsChanged(oldLabels: Set[Symbol], newLabels: Set[Symbol]) extends GraphEvent
}

// ============================================================
// STATE INTERFACES
// ============================================================

/** Base trait for query states.
  *
  * States form a notification graph where children notify parents
  * of result changes (deltas). The root state delivers final results.
  */
sealed trait QueryState {
  def id: StandingQueryId
  def mode: RuntimeMode

  /** Receive a delta notification from a child state */
  def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit

  /** Initialize state with current node context */
  def kickstart(context: NodeContext, actor: ActorRef): Unit
}

/** State that publishes results to a parent */
trait PublishingState { this: QueryState =>
  def publishTo: StandingQueryId

  protected var hasEmitted: Boolean = false

  /** Host reference for direct local routing (bypasses actor mailbox) */
  protected var hostRef: Option[QueryStateHost] = None

  /** Set the host reference - called during state installation */
  def setHost(host: QueryStateHost): Unit = hostRef = Some(host)

  protected def emit(delta: Delta.T, actor: ActorRef): Unit = {
    val shouldEmit = mode match {
      case RuntimeMode.Eager => !hasEmitted
      case RuntimeMode.Lazy => true
    }
    if (shouldEmit) {
      QPTrace.stateEmit(None, id, this.getClass.getSimpleName, publishTo, delta.size)
      // Use direct routing when host is available (local state) - bypasses mailbox
      hostRef match {
        case Some(host) =>
          host.routeNotification(publishTo, id, delta)
        case None =>
          // Fallback to message passing (shouldn't happen for properly initialized states)
          actor ! QuinePatternCommand.QueryUpdate(publishTo, id, delta)
      }
      hasEmitted = true
      if (mode == RuntimeMode.Eager) {
        QPTrace.log(s"UNREGISTER state=$id (eager mode, after emit)")
        // Unregister still goes through actor to maintain ordering with any pending messages
        actor ! QuinePatternCommand.UnregisterState(id)
      }
    } else {
      QPTrace.log(s"EMIT-SKIPPED state=$id already emitted in eager mode")
    }
  }
}

/** State that reacts to external events */
trait EventDrivenState[A] { this: QueryState =>
  def handleEvent(event: A, actor: ActorRef): Unit
}

/** State that reacts to property changes */
trait PropertySensitiveState { this: QueryState =>
  def onPropertyChange(
    key: Symbol,
    oldValue: Option[PropertyValue],
    newValue: Option[PropertyValue],
    actor: ActorRef,
  ): Unit

  /** Which property keys this state watches.
    * - Some(set) = only watch these specific keys
    * - None = watch all property changes
    */
  def watchedPropertyKeys: Option[Set[Symbol]]
}

/** State that reacts to edge changes */
trait EdgeSensitiveState { this: QueryState =>
  def onEdgeAdded(edge: HalfEdge, actor: ActorRef): Unit
  def onEdgeRemoved(edge: HalfEdge, actor: ActorRef): Unit

  /** Which edge type this state watches.
    * - Some(label) = only watch edges with this label
    * - None = watch all edges
    */
  def watchedEdgeLabel: Option[Symbol]
}

/** State that reacts to label changes */
trait LabelSensitiveState { this: QueryState =>
  def onLabelsChanged(oldLabels: Set[Symbol], newLabels: Set[Symbol], actor: ActorRef): Unit
}

// ============================================================
// STATE INSTANTIATOR
// ============================================================

/** Creates actual QueryState instances from StateDescriptors.
  *
  * This is separate from the builder to allow different instantiation
  * strategies (e.g., for testing, for different execution contexts).
  */
trait StateInstantiator {

  /** Create a state from its descriptor */
  def instantiate(
    descriptor: StateDescriptor,
    graph: StateGraph,
    nodeContext: NodeContext,
    injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value],
  ): QueryState
}

/** Default instantiator that creates production state implementations */
object DefaultStateInstantiator extends StateInstantiator {

  override def instantiate(
    descriptor: StateDescriptor,
    stateGraph: StateGraph,
    nodeContext: NodeContext,
    injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value],
  ): QueryState =
    descriptor match {

      case d @ StateDescriptor.Output(id, _, target) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for OutputState but not provided in NodeContext"),
        )
        new OutputState(id, d.mode, target, graph, stateGraph.returnColumns, stateGraph.outputNameMapping)

      case d @ StateDescriptor.Unit(id, parentId, _, _) =>
        // UnitState uses injectedContext if provided - this seeds context from parent (e.g., Anchor dispatch)
        new UnitState(id, parentId, d.mode, injectedContext)

      case d @ StateDescriptor.WatchId(id, parentId, _, _, binding) =>
        // WatchId also uses injectedContext - merge it with the node binding when emitting
        new WatchIdState(id, parentId, d.mode, binding, nodeContext.quineId, injectedContext)

      case d @ StateDescriptor.WatchProperty(id, parentId, _, _, property, aliasAs, constraint) =>
        new WatchPropertyState(id, parentId, d.mode, property, aliasAs, constraint)

      case d @ StateDescriptor.WatchAllProperties(id, parentId, _, _, binding) =>
        new WatchAllPropertiesState(id, parentId, d.mode, binding, injectedContext)

      case d @ StateDescriptor.WatchLabels(id, parentId, _, _, aliasAs, constraint) =>
        new WatchLabelsState(id, parentId, d.mode, aliasAs, constraint)

      case d @ StateDescriptor.WatchNode(id, parentId, _, _, binding) =>
        new WatchNodeState(id, parentId, d.mode, binding, injectedContext)

      case d @ StateDescriptor.Product(id, parentId, _, plan, childIds, childEntryPoints) =>
        new ProductState(id, parentId, d.mode, childIds, plan.emitSubscriptionsLazily, childEntryPoints)

      case d @ StateDescriptor.Sequence(id, parentId, _, _, firstId, andThenId, contextBridgeId, contextFlow) =>
        new SequenceState(id, parentId, d.mode, firstId, andThenId, contextBridgeId, contextFlow)

      case d @ StateDescriptor.ContextBridge(id, parentId, _, sequenceId) =>
        new ContextBridgeState(id, parentId, d.mode, sequenceId)

      case d @ StateDescriptor.Filter(id, parentId, _, plan, inputId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for FilterState but not provided in NodeContext"),
        )
        new FilterState(id, parentId, d.mode, plan.predicate, inputId, graph, stateGraph.params)

      case d @ StateDescriptor.Project(id, parentId, _, plan, inputId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for ProjectState but not provided in NodeContext"),
        )
        new ProjectState(id, parentId, d.mode, plan.columns, plan.dropExisting, inputId, graph, stateGraph.params)

      case d @ StateDescriptor.Distinct(id, parentId, _, _, inputId) =>
        new DistinctState(id, parentId, d.mode, inputId)

      case d @ StateDescriptor.Expand(id, parentId, _, plan, onNeighborPlan) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for ExpandState but not provided in NodeContext"),
        )
        val namespace = nodeContext.namespace.getOrElse(
          throw new IllegalStateException("Namespace required for ExpandState but not provided in NodeContext"),
        )
        new ExpandState(
          id,
          parentId,
          d.mode,
          plan.edgeLabel,
          plan.direction,
          onNeighborPlan,
          graph,
          namespace,
          stateGraph.params,
        )

      case d @ StateDescriptor.Anchor(id, parentId, _, _, target, onTargetPlan, fallbackOutput) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for AnchorState but not provided in NodeContext"),
        )
        val namespace = nodeContext.namespace.getOrElse(
          throw new IllegalStateException("Namespace required for AnchorState but not provided in NodeContext"),
        )
        new AnchorState(
          id,
          parentId,
          d.mode,
          target,
          onTargetPlan,
          graph,
          namespace,
          fallbackOutput,
          stateGraph.params,
          stateGraph.injectedContext, // Pass injectedContext for evaluating target expressions
        )

      case d @ StateDescriptor.Unwind(id, parentId, _, plan, subqueryId, contextBridgeId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for UnwindState but not provided in NodeContext"),
        )
        new UnwindState(
          id,
          parentId,
          d.mode,
          plan.list,
          plan.binding,
          subqueryId,
          contextBridgeId,
          graph,
          stateGraph.params,
        )

      case d @ StateDescriptor.Procedure(id, parentId, _, plan, subqueryId, contextBridgeId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for ProcedureState but not provided in NodeContext"),
        )
        val namespace = nodeContext.namespace.getOrElse(
          throw new IllegalStateException("Namespace required for ProcedureState but not provided in NodeContext"),
        )
        new ProcedureState(
          id,
          parentId,
          d.mode,
          plan.procedureName,
          plan.arguments,
          plan.yields,
          subqueryId,
          contextBridgeId,
          graph,
          namespace,
          stateGraph.params,
        )

      case d @ StateDescriptor.Effect(id, parentId, _, plan, inputId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for EffectState but not provided in NodeContext"),
        )
        val namespace = nodeContext.namespace.getOrElse(
          throw new IllegalStateException("Namespace required for EffectState but not provided in NodeContext"),
        )
        new EffectState(
          id,
          parentId,
          d.mode,
          plan.effects,
          inputId,
          graph,
          nodeContext.quineId,
          namespace,
          stateGraph.params,
        )

      case d @ StateDescriptor.Aggregate(id, parentId, _, plan, inputId) =>
        new AggregateState(id, parentId, d.mode, plan.aggregations, plan.groupBy, inputId)

      case d @ StateDescriptor.Sort(id, parentId, _, plan, inputId) =>
        val graph = nodeContext.graph.getOrElse(
          throw new IllegalStateException("Graph required for SortState but not provided in NodeContext"),
        )
        new SortState(id, parentId, d.mode, plan.orderBy, inputId, graph, stateGraph.params)

      case d @ StateDescriptor.Limit(id, parentId, _, plan, inputId) =>
        new LimitState(id, parentId, d.mode, plan.count, inputId)

      case d @ StateDescriptor.SubscribeToQueryPart(id, parentId, _, plan, queryPartId) =>
        new SubscribeToQueryPartState(id, parentId, d.mode, queryPartId, plan.projection)
    }
}

// ============================================================
// STUB STATE IMPLEMENTATIONS
// These are minimal implementations to make the code compile.
// Real implementations will have full logic.
// ============================================================

class OutputState(
  val id: StandingQueryId,
  val mode: RuntimeMode,
  val target: OutputTarget,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
    with com.thatdot.quine.graph.StandingQueryOpsGraph,
  val returnColumns: Option[Set[Symbol]], // Columns to include in output (from RETURN clause)
  val outputNameMapping: Map[Symbol, Symbol] = Map.empty, // Maps internal binding IDs to human-readable names
) extends QueryState {

  import com.thatdot.common.logging.Log.LogConfig
  import com.thatdot.quine.model.QuineValue

  implicit private val logConfig: LogConfig = LogConfig.permissive

  // Log output target type on creation
  private val targetType = target match {
    case OutputTarget.StandingQuerySink(sqId, _) => s"StandingQuerySink($sqId)"
    case OutputTarget.EagerCollector(_) => "EagerCollector"
    case OutputTarget.LazyCollector(_) => "LazyCollector"
    case OutputTarget.RemoteState(node, stateId, _, dispatchId) =>
      s"RemoteState($node, $stateId, dispatchId=$dispatchId)"
    case OutputTarget.HostedState(_, stateId, dispatchId) => s"HostedState($stateId, $dispatchId)"
  }
  QPTrace.log(s"OUTPUT-CREATED id=$id target=$targetType mode=$mode")

  // For EagerCollector: accumulate results until complete
  private val collectedResults = scala.collection.mutable.ListBuffer.empty[QueryContext]
  private val isComplete = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** Filter context to only include return columns and apply output name mapping.
    * This converts internal binding IDs to human-readable output names.
    */
  private def filterContext(ctx: QueryContext): QueryContext = {
    // First filter to only return columns if specified
    val filtered = returnColumns match {
      case Some(columns) =>
        ctx.bindings.view.filterKeys(columns.contains).toMap
      case None =>
        ctx.bindings
    }

    // Then apply output name mapping to convert internal binding IDs to human-readable names
    val renamed = if (outputNameMapping.nonEmpty) {
      filtered.map { case (k, v) =>
        outputNameMapping.getOrElse(k, k) -> v
      }
    } else {
      filtered
    }

    QueryContext(renamed)
  }

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit =
    target match {
      case OutputTarget.StandingQuerySink(sqId, namespace) =>
        // Deliver results to standing query output
        QPTrace.log(s"OUTPUT-SINK id=$id sqId=$sqId deltaSize=${delta.size}")
        graph.requiredGraphIsReady()
        val sqns = graph.standingQueries(namespace)
        sqns match {
          case None =>
            if (delta.nonEmpty) {
              QPLog.warn(
                s"OUTPUT-SINK: No standing query namespace for $namespace. " +
                s"Delta with ${delta.size} entries dropped. sqId=$sqId",
              )
            }
          case Some(standingQueries) =>
            standingQueries.runningStandingQuery(sqId) match {
              case None =>
                if (delta.nonEmpty) {
                  QPLog.warn(
                    s"OUTPUT-SINK: Standing query $sqId not found in namespace $namespace. " +
                    s"Delta with ${delta.size} entries dropped. " +
                    s"Running queries: ${standingQueries.runningStandingQueries.keys.mkString(", ")}",
                  )
                }
              case Some(rsq) =>
                // Convert each delta entry to StandingQueryResult
                var resultCount = 0
                delta.foreach { case (ctx, mult) =>
                  val isPositive = mult > 0
                  val filteredCtx = filterContext(ctx)
                  val data = convertToQuineValueMap(filteredCtx)
                  val count = math.abs(mult)

                  // Emit the result `count` times (respecting multiplicity)
                  (1 to count).foreach { _ =>
                    val success = rsq.offerResult(StandingQueryResult(isPositive, data))
                    if (success) resultCount += 1
                  }
                }
                QPTrace.log(
                  s"OUTPUT-SINK-DELIVERED id=$id resultsOffered=$resultCount data=${delta.headOption.map(_._1.bindings.keys.mkString(",")).getOrElse("empty")}",
                )
            }
        }

      case OutputTarget.EagerCollector(promise) =>
        // Collect results for eager query
        QPTrace.log(s"OUTPUT-EAGER id=$id deltaSize=${delta.size}")
        delta.foreach { case (ctx, mult) =>
          if (mult > 0) {
            // Add `mult` copies (filtered to return columns)
            val filteredCtx = filterContext(ctx)
            (1 to mult).foreach(_ => collectedResults += filteredCtx)
          }
        // For eager mode, we don't handle retractions - results only accumulate
        }
        // For Eager mode outputs (like iterate), receiving ANY notification means the plan has completed.
        // This includes empty deltas, which signal "the plan evaluated but produced no results".
        // Auto-complete to fulfill the promise and unblock the output stream.
        // This is critical: without this, queries that produce no results would deadlock.
        complete()

      case OutputTarget.LazyCollector(collector) =>
        // Collect results incrementally for lazy/standing query testing
        // Filter contexts to return columns before adding to collector
        val filteredDelta = delta.map { case (ctx, mult) =>
          filterContext(ctx) -> mult
        }
        collector.addDelta(filteredDelta)

      case OutputTarget.RemoteState(originNode, stateId, namespace, dispatchId) =>
        // Send results back to the state on the origin node
        // Don't filter here - we need all bindings for proper delta tracking upstream
        import com.thatdot.quine.graph.behavior.QuinePatternCommand
        import com.thatdot.quine.graph.messaging.SpaceTimeQuineId

        // In Eager mode, always send a message (even empty delta) to signal completion.
        // This is critical: the dispatching Anchor is waiting for a response, and an empty
        // delta means "evaluated but produced no results". Without this, the Anchor waits forever.
        // Use dispatchId as the 'from' so the receiving state can identify this as expected results
        if (delta.nonEmpty || mode == RuntimeMode.Eager) {
          val stqid = SpaceTimeQuineId(originNode, namespace, None)
          graph.relayTell(stqid, QuinePatternCommand.QueryUpdate(stateId, dispatchId, delta))
        }

      case OutputTarget.HostedState(hostActorRef, stateId, dispatchId) =>
        // Send results back to a state on the hosting actor (NonNodeActor)
        // Don't filter here - we need all bindings for proper delta tracking upstream
        // Use dispatchId as the 'from' so the Anchor can identify this as target results
        import com.thatdot.quine.graph.behavior.QuinePatternCommand

        // In Eager mode, always send a message (even empty delta) to signal completion.
        if (delta.nonEmpty || mode == RuntimeMode.Eager) {
          hostActorRef ! QuinePatternCommand.QueryUpdate(stateId, dispatchId, delta)
        }
    }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()

  /** Called when the eager query is complete - delivers all collected results.
    * Uses trySuccess to handle the case where multiple OutputState instances
    * share the same Promise (e.g., when fallbackOutput propagates to multiple targets).
    */
  def complete(): Unit =
    target match {
      case OutputTarget.EagerCollector(promise) if isComplete.compareAndSet(false, true) =>
        // Use trySuccess - returns false if already completed, doesn't throw
        val results = collectedResults.toSeq
        QPTrace.log(
          s"OUTPUT-EAGER-COMPLETE id=$id resultsCount=${results.size} keys=${results.headOption.map(_.bindings.keys.mkString(",")).getOrElse("empty")}",
        )
        val success = promise.trySuccess(results)
        if (!success) {
          QPTrace.log(s"OUTPUT-EAGER-COMPLETE-FAILED id=$id (promise already completed)")
        }
      case _ => ()
    }

  private def convertToQuineValueMap(ctx: QueryContext): Map[String, QuineValue] =
    ctx.bindings.map { case (sym, patternValue) =>
      sym.name -> CypherAndQuineHelpers.patternValueToQuineValue(patternValue)
    }
}

class UnitState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value], // Context from parent (e.g., Anchor dispatch)
) extends QueryState
    with PublishingState {

  // Log Unit construction with connection info
  QPTrace.log(s"UNIT-CREATED id=$id publishTo=$publishTo hasInjectedContext=${injectedContext.nonEmpty}")

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(s"Unit $id: notify from=$from deltaSize=${delta.size}")
    emit(delta, actor)
  }
  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    // If we have injected context from parent, emit that instead of empty context
    val delta = if (injectedContext.nonEmpty) {
      val ctx = QueryContext(injectedContext)
      Map(ctx -> 1)
    } else {
      Delta.unit
    }
    QPTrace.log(
      s"Unit $id: kickstart with injectedContext=${injectedContext.nonEmpty} deltaSize=${delta.size} publishTo=$publishTo",
    )
    emit(delta, actor)
  }
}

/** WatchIdState emits the node's ID as a Value.NodeId.
  *
  * This is a pure identity operator - it only provides the node's QuineId.
  * Properties are handled separately by LocalProperty/LocalAllProperties.
  * Labels are handled by LocalLabels.
  *
  * The emitted value is stable (node IDs don't change), so this state
  * emits once on kickstart and never retracts.
  */
class WatchIdState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val binding: Symbol,
  val quineId: Option[QuineId],
  val injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {
  import com.thatdot.quine.language.ast.Value

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = ()

  override def kickstart(context: NodeContext, actor: ActorRef): Unit =
    context.quineId.foreach { qid =>
      // Emit just the node ID - properties and labels are handled by other operators
      val nodeIdValue = Value.NodeId(qid)
      val ctx = QueryContext(injectedContext + (binding -> nodeIdValue))
      emit(Map(ctx -> 1), actor)
    }
}

class WatchPropertyState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val property: Symbol,
  val aliasAs: Option[Symbol],
  val constraint: PropertyConstraint,
) extends QueryState
    with PublishingState
    with PropertySensitiveState {

  import com.thatdot.quine.language.ast.Value

  // Track whether we've emitted a match (for proper retraction)
  private var currentlyMatched: Option[QueryContext] = None

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = ()

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    var emitted = false
    val propValue = context.properties.get(property)
    propValue match {
      case Some(pv) =>
        val patternValue = CypherAndQuineHelpers.propertyValueToPatternValue(pv)
        if (constraint(patternValue)) {
          val ctx = makeContext(patternValue)
          currentlyMatched = Some(ctx)
          emit(Map(ctx -> 1), actor)
          emitted = true
        }
      case None =>
        if (constraint.satisfiedByNone) {
          val ctx = makeContext(Value.Null)
          currentlyMatched = Some(ctx)
          emit(Map(ctx -> 1), actor)
          emitted = true
        }
    }
    // In Eager mode, if we didn't emit a match, emit empty to signal "evaluated, no match"
    if (mode == RuntimeMode.Eager && !emitted) {
      emit(Delta.empty, actor)
    }
  }

  override def onPropertyChange(
    key: Symbol,
    oldValue: Option[PropertyValue],
    newValue: Option[PropertyValue],
    actor: ActorRef,
  ): Unit =
    if (key == property) {
      val deltaBuilder = mutable.Map.empty[QueryContext, Int]

      // Retract old match if any
      currentlyMatched.foreach { oldCtx =>
        deltaBuilder(oldCtx) = deltaBuilder.getOrElse(oldCtx, 0) - 1
      }
      currentlyMatched = None

      // Check new value
      newValue match {
        case Some(pv) =>
          val patternValue = CypherAndQuineHelpers.propertyValueToPatternValue(pv)
          if (constraint(patternValue)) {
            val ctx = makeContext(patternValue)
            currentlyMatched = Some(ctx)
            deltaBuilder(ctx) = deltaBuilder.getOrElse(ctx, 0) + 1
          }
        case None =>
          if (constraint.satisfiedByNone) {
            val ctx = makeContext(Value.Null)
            currentlyMatched = Some(ctx)
            deltaBuilder(ctx) = deltaBuilder.getOrElse(ctx, 0) + 1
          }
      }

      // Emit non-zero deltas
      val nonZero = deltaBuilder.filter(_._2 != 0).toMap
      if (nonZero.nonEmpty) {
        emit(nonZero, actor)
      }
    }

  private def makeContext(value: Value): QueryContext =
    aliasAs match {
      case Some(alias) => QueryContext(Map(alias -> value))
      case None => QueryContext.empty
    }

  // Implement PropertySensitiveState.watchedPropertyKeys
  override def watchedPropertyKeys: Option[Set[Symbol]] = Some(Set(property))
}

class WatchAllPropertiesState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val binding: Symbol,
  val injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState
    with PropertySensitiveState {

  import scala.collection.immutable.SortedMap
  import com.thatdot.quine.language.ast.Value

  // Track current properties for proper retraction
  private var currentProperties: Map[Symbol, Value] = Map.empty
  private var currentContext: Option[QueryContext] = None
  // Store the labelsProperty key to filter it out (labels are internal, not user-visible properties)
  private var labelsPropertyKey: Option[Symbol] = None

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = ()

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    // Store the labelsProperty key for filtering
    labelsPropertyKey = context.graph.map(_.labelsProperty)
    // Filter out labelsProperty from properties
    currentProperties = context.properties
      .filter { case (k, _) => !labelsPropertyKey.contains(k) }
      .map { case (k, v) => k -> CypherAndQuineHelpers.propertyValueToPatternValue(v) }
    val mapValue = Value.Map(SortedMap.from(currentProperties))
    // Include injectedContext so outer bindings (like parameter aliases) are available
    val ctx = QueryContext(injectedContext + (binding -> mapValue))
    currentContext = Some(ctx)
    emit(Map(ctx -> 1), actor)
  }

  override def onPropertyChange(
    key: Symbol,
    oldValue: Option[PropertyValue],
    newValue: Option[PropertyValue],
    actor: ActorRef,
  ): Unit = {
    // Skip labelsProperty changes - it's an internal property not exposed to users
    if (labelsPropertyKey.contains(key)) return

    val deltaBuilder = mutable.Map.empty[QueryContext, Int]

    // Retract old context if any
    currentContext.foreach { oldCtx =>
      deltaBuilder(oldCtx) = deltaBuilder.getOrElse(oldCtx, 0) - 1
    }

    // Update properties map
    newValue match {
      case Some(pv) =>
        currentProperties = currentProperties + (key -> CypherAndQuineHelpers.propertyValueToPatternValue(pv))
      case None =>
        currentProperties = currentProperties - key
    }

    // Emit new context (include injectedContext for outer bindings)
    val mapValue = Value.Map(SortedMap.from(currentProperties))
    val newCtx = QueryContext(injectedContext + (binding -> mapValue))
    currentContext = Some(newCtx)
    deltaBuilder(newCtx) = deltaBuilder.getOrElse(newCtx, 0) + 1

    // Emit non-zero deltas
    val nonZero = deltaBuilder.filter(_._2 != 0).toMap
    if (nonZero.nonEmpty) {
      emit(nonZero, actor)
    }
  }

  // Implement PropertySensitiveState.watchedPropertyKeys
  // WatchAllProperties watches ALL properties (except labelsProperty), so return None
  override def watchedPropertyKeys: Option[Set[Symbol]] = None
}

class WatchLabelsState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val aliasAs: Option[Symbol],
  val constraint: LabelConstraint,
) extends QueryState
    with PublishingState
    with LabelSensitiveState {

  import com.thatdot.quine.language.ast.Value

  // Track whether we've emitted a match (for proper retraction)
  private var currentlyMatched: Option[QueryContext] = None

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = ()

  override def kickstart(context: NodeContext, actor: ActorRef): Unit =
    if (constraint(context.labels)) {
      val ctx = makeContext(context.labels)
      currentlyMatched = Some(ctx)
      emit(Map(ctx -> 1), actor)
    } else if (mode == RuntimeMode.Eager) {
      // In Eager mode, emit empty to signal "evaluated, no match"
      emit(Delta.empty, actor)
    }

  override def onLabelsChanged(oldLabels: Set[Symbol], newLabels: Set[Symbol], actor: ActorRef): Unit = {
    val wasMatching = constraint(oldLabels)
    val isMatching = constraint(newLabels)

    (wasMatching, isMatching) match {
      case (true, true) =>
        // Still matching but labels changed - retract old, assert new (if aliasing labels)
        aliasAs match {
          case Some(_) =>
            val deltaBuilder = mutable.Map.empty[QueryContext, Int]
            currentlyMatched.foreach { oldCtx =>
              deltaBuilder(oldCtx) = deltaBuilder.getOrElse(oldCtx, 0) - 1
            }
            val newCtx = makeContext(newLabels)
            currentlyMatched = Some(newCtx)
            deltaBuilder(newCtx) = deltaBuilder.getOrElse(newCtx, 0) + 1
            val nonZero = deltaBuilder.filter(_._2 != 0).toMap
            if (nonZero.nonEmpty) emit(nonZero, actor)
          case None =>
            // No aliasing, so context doesn't change
            ()
        }
      case (true, false) =>
        // No longer matching - retract
        currentlyMatched.foreach { oldCtx =>
          emit(Map(oldCtx -> -1), actor)
        }
        currentlyMatched = None
      case (false, true) =>
        // Now matching - assert
        val ctx = makeContext(newLabels)
        currentlyMatched = Some(ctx)
        emit(Map(ctx -> 1), actor)
      case (false, false) =>
        // Still not matching - no-op
        ()
    }
  }

  private def makeContext(labels: Set[Symbol]): QueryContext =
    aliasAs match {
      case Some(alias) =>
        val labelList = Value.List(labels.toList.map(s => Value.Text(s.name)))
        QueryContext(Map(alias -> labelList))
      case None =>
        QueryContext.empty
    }
}

/** WatchNodeState emits a complete Value.Node with id, labels, and properties.
  *
  * This watches both properties and labels, combining them into a single node value.
  * The labelsProperty (configurable, e.g. __LABEL) is filtered from properties since
  * labels are provided separately.
  */
class WatchNodeState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val binding: Symbol,
  val injectedContext: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState
    with PropertySensitiveState
    with LabelSensitiveState {

  import scala.collection.immutable.SortedMap
  import com.thatdot.quine.language.ast.Value

  // Track current state for proper retraction
  private var currentQuineId: Option[QuineId] = None
  private var currentLabels: Set[Symbol] = Set.empty
  private var currentProperties: Map[Symbol, Value] = Map.empty
  private var currentContext: Option[QueryContext] = None
  private var labelsPropertyKey: Option[Symbol] = None

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = ()

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    currentQuineId = context.quineId
    currentLabels = context.labels
    // Store the labelsProperty key for filtering
    labelsPropertyKey = context.graph.map(_.labelsProperty)
    // Filter out labelsProperty from properties
    currentProperties = context.properties
      .filter { case (k, _) => !labelsPropertyKey.contains(k) }
      .map { case (k, v) => k -> CypherAndQuineHelpers.propertyValueToPatternValue(v) }

    emitCurrentState(actor)
  }

  override def onPropertyChange(
    key: Symbol,
    oldValue: Option[PropertyValue],
    newValue: Option[PropertyValue],
    actor: ActorRef,
  ): Unit = {
    // Skip labelsProperty changes - labels are handled via onLabelsChanged
    if (labelsPropertyKey.contains(key)) return

    val deltaBuilder = mutable.Map.empty[QueryContext, Int]

    // Retract old context if any
    currentContext.foreach { oldCtx =>
      deltaBuilder(oldCtx) = deltaBuilder.getOrElse(oldCtx, 0) - 1
    }

    // Update properties map
    newValue match {
      case Some(pv) =>
        currentProperties = currentProperties + (key -> CypherAndQuineHelpers.propertyValueToPatternValue(pv))
      case None =>
        currentProperties = currentProperties - key
    }

    // Emit new context
    emitWithDelta(deltaBuilder, actor)
  }

  override def onLabelsChanged(oldLabels: Set[Symbol], newLabels: Set[Symbol], actor: ActorRef): Unit = {
    val deltaBuilder = mutable.Map.empty[QueryContext, Int]

    // Retract old context if any
    currentContext.foreach { oldCtx =>
      deltaBuilder(oldCtx) = deltaBuilder.getOrElse(oldCtx, 0) - 1
    }

    currentLabels = newLabels
    emitWithDelta(deltaBuilder, actor)
  }

  private def emitCurrentState(actor: ActorRef): Unit =
    currentQuineId.foreach { qid =>
      val nodeValue = Value.Node(qid, currentLabels, Value.Map(SortedMap.from(currentProperties)))
      val ctx = QueryContext(injectedContext + (binding -> nodeValue))
      currentContext = Some(ctx)
      emit(Map(ctx -> 1), actor)
    }

  private def emitWithDelta(deltaBuilder: mutable.Map[QueryContext, Int], actor: ActorRef): Unit =
    currentQuineId.foreach { qid =>
      val nodeValue = Value.Node(qid, currentLabels, Value.Map(SortedMap.from(currentProperties)))
      val newCtx = QueryContext(injectedContext + (binding -> nodeValue))
      currentContext = Some(newCtx)
      deltaBuilder(newCtx) = deltaBuilder.getOrElse(newCtx, 0) + 1

      val nonZero = deltaBuilder.filter(_._2 != 0).toMap
      if (nonZero.nonEmpty) {
        emit(nonZero, actor)
      }
    }

  // WatchNode watches ALL properties (except labelsProperty), so return None
  override def watchedPropertyKeys: Option[Set[Symbol]] = None
}

class ProductState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val childIds: List[StandingQueryId],
  val emitSubscriptionsLazily: Boolean,
  val childEntryPoints: Set[StandingQueryId] = Set.empty, // Children that need context injection
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.graph.behavior.QuinePatternCommand

  // Accumulated state from each child: Map[QueryContext, multiplicity]
  private val childStates: mutable.Map[StandingQueryId, Delta.T] = mutable.Map.empty

  // Track how many children have been "activated" for lazy subscription emission.
  // When emitSubscriptionsLazily is true AND we're in Lazy mode, we only consider
  // children one at a time, activating the next child when the current one produces
  // non-empty results. This mirrors MVSQ's lazy subscription behavior.
  //
  // IMPORTANT: In Eager mode, we always activate all children immediately because:
  // 1. Children notify exactly once in Eager mode
  // 2. We need all notifications to compute the final cross-product
  // 3. Lazy subscription is designed for streaming (Lazy mode), not one-shot queries
  private var activeChildCount: Int =
    if (emitSubscriptionsLazily && mode == RuntimeMode.Lazy && childIds.nonEmpty) 1
    else childIds.size

  // Monotonic flag: once true, stays true. Tracks when all active children have
  // produced at least one non-empty result. This avoids repeated checks.
  private var _isReadyToReport: Boolean = false

  /** Check if we're ready to report results (all active children have non-empty results).
    * Uses monotonic caching - once ready, we stay ready.
    */
  private def isReadyToReport: Boolean = _isReadyToReport || {
    val activeChildren = childIds.take(activeChildCount)
    val ready = activeChildren.forall { cid =>
      childStates.get(cid).exists(_.nonEmpty)
    }
    // Only cache as ready if ALL children are active and ready
    if (ready && activeChildCount == childIds.size) _isReadyToReport = true
    ready
  }

  /** Check if all siblings (children other than `from`) have non-empty accumulated state.
    * Used for early exit optimization in Lazy mode.
    */
  private def allSiblingsHaveResults(from: StandingQueryId): Boolean = {
    val siblings = childIds.take(activeChildCount).filter(_ != from)
    siblings.forall(cid => childStates.get(cid).exists(_.nonEmpty))
  }

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    // Check if this is context injection (from a ContextBridge, not from a child)
    if (!childIds.contains(from) && childEntryPoints.nonEmpty) {
      // This is context injection - forward to all child entry points
      QPTrace.log(
        s"Product $id: context injection from=$from, forwarding to ${childEntryPoints.size} child entry points",
      )
      childEntryPoints.foreach { childId =>
        actor ! QuinePatternCommand.QueryUpdate(childId, id, delta)
      }
      return
    }

    // Lazy subscription: check if this child is active
    val childIndex = childIds.indexOf(from)
    if (childIndex >= activeChildCount) {
      // This child isn't active yet - store its state but don't process further
      // This can happen when emitSubscriptionsLazily is true and earlier children
      // haven't produced results yet. We still accumulate the state so we don't
      // lose it, but we don't propagate results upstream.
      childStates(from) = Delta.add(childStates.getOrElse(from, Delta.empty), delta)
      QPTrace.log(
        s"Product $id: storing notification from inactive child $from (index $childIndex >= activeChildCount $activeChildCount)",
      )
      return
    }

    // Normal child result handling
    // Update the accumulated state for the child that sent the delta
    // CHANGE DETECTION (key optimization from MVSQ): Only proceed with cross-product
    // computation if the delta actually changes the accumulated state. This prevents
    // expensive recomputation when children send redundant updates.
    val previousState = childStates.getOrElse(from, Delta.empty)
    val newState = Delta.add(previousState, delta)
    val stateChanged = newState != previousState
    childStates(from) = newState

    // Lazy subscription: if this child just produced non-empty results and is the last active child,
    // activate the next child. This mimics MVSQ's behavior of only subscribing to the next
    // subquery when the previous one has results.
    // Note: This only applies in Lazy mode - in Eager mode, all children are always active.
    if (emitSubscriptionsLazily && mode == RuntimeMode.Lazy && delta.nonEmpty && childIndex == activeChildCount - 1) {
      val currentChildState = childStates(from)
      if (currentChildState.nonEmpty && activeChildCount < childIds.size) {
        activeChildCount += 1
        val newChildId = childIds(activeChildCount - 1)
        QPTrace.log(s"Product $id: activating child #${activeChildCount - 1} (id $newChildId)")
        // If the newly activated child already has accumulated state (from earlier notifications
        // that we stored), we should now process it. Check if it has results and potentially
        // activate the next child too.
        @scala.annotation.tailrec
        def activateChain(): Unit =
          if (activeChildCount < childIds.size) {
            val lastActiveId = childIds(activeChildCount - 1)
            childStates.get(lastActiveId) match {
              case Some(state) if state.nonEmpty =>
                activeChildCount += 1
                QPTrace.log(s"Product $id: chain-activating child #${activeChildCount - 1}")
                activateChain()
              case _ => ()
            }
          }
        activateChain()
      }
    }

    val allChildrenNotified = childStates.size == childIds.size

    QPTrace.log(
      s"Product $id: notify from=$from deltaSize=${delta.size} stateChanged=$stateChanged " +
      s"children=${childIds.size} active=$activeChildCount notified=${childStates.size} " +
      s"allNotified=$allChildrenNotified isReady=$isReadyToReport",
    )

    mode match {
      case RuntimeMode.Eager =>
        // In Eager mode, wait for ALL children to notify before emitting
        // Each child notifies exactly once (even with empty delta)
        if (allChildrenNotified) {
          // Compute full cross-product of all children's accumulated states
          val fullProduct = childIds
            .map(cid => childStates.getOrElse(cid, Delta.empty))
            .foldLeft(Delta.unit)(Delta.crossProduct)
          QPTrace.log(s"Product $id: all children notified, emitting fullProduct size=${fullProduct.size}")
          emit(fullProduct, actor)
        }

      case RuntimeMode.Lazy =>
        // CHANGE DETECTION (key optimization from MVSQ): If the delta didn't actually
        // change this child's accumulated state, skip the expensive cross-product
        // computation. This prevents redundant work when children send duplicate updates.
        if (!stateChanged) {
          QPTrace.log(s"Product $id: skipping cross-product - state unchanged")
          return
        }

        // Early exit optimization: if any sibling has empty state (or hasn't reported),
        // the cross-product will be empty, so skip the computation entirely.
        // This is a significant optimization for queries with many branches where
        // some branches may not match.
        if (!allSiblingsHaveResults(from)) {
          QPTrace.log(s"Product $id: skipping cross-product - not all siblings have results")
          return
        }

        // In Lazy mode, emit incrementally on each child update
        // When child i sends delta_i, output = delta_i × (∏_{j≠i} state[j])
        val otherChildIds = childIds.take(activeChildCount).filter(_ != from)
        val otherStates = otherChildIds.map(cid => childStates(cid)) // Safe - checked above
        val otherProduct = otherStates.foldLeft(Delta.unit)(Delta.crossProduct)
        val outputDelta = Delta.crossProduct(delta, otherProduct)
        if (outputDelta.nonEmpty) {
          emit(outputDelta, actor)
        }
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class SequenceState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val firstId: StandingQueryId,
  val andThenId: StandingQueryId,
  val contextBridgeId: Option[StandingQueryId], // Bridge that feeds first's context into andThen
  val contextFlow: ContextFlow,
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.graph.behavior.QuinePatternCommand

  // Log Sequence construction with connection info
  QPTrace.log(
    s"SEQUENCE-CREATED id=$id firstId=$firstId andThenId=$andThenId bridgeId=${contextBridgeId.getOrElse("none")} publishTo=$publishTo",
  )

  // Track accumulated results from first
  // We need this to combine with andThen's results according to contextFlow
  private var firstState: Delta.T = Delta.empty

  // Track whether we've received from first and andThen (for Eager mode)
  private var firstNotified: Boolean = false
  private var andThenNotified: Boolean = false

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(
      s"Sequence $id: notify from=$from deltaSize=${delta.size} " +
      s"isFirst=${from == firstId} isAndThen=${from == andThenId} " +
      s"firstNotified=$firstNotified andThenNotified=$andThenNotified",
    )

    if (from == firstId) {
      firstNotified = true
      // Update first's accumulated state
      firstState = Delta.add(firstState, delta)

      // Forward first's context to andThen via the context bridge
      contextBridgeId match {
        case Some(bridgeId) =>
          QPTrace.log(s"Sequence $id: forwarding to bridge $bridgeId")
          actor ! QuinePatternCommand.QueryUpdate(bridgeId, id, delta)
        case None =>
          QPTrace.log(s"Sequence $id: no bridge, cannot forward context")
      }

    } else if (from == andThenId) {
      andThenNotified = true

      // andThen has produced results
      val outputDelta = contextFlow match {
        case ContextFlow.Replace => delta
        case ContextFlow.Extend => Delta.crossProduct(firstState, delta)
      }

      QPTrace.log(s"Sequence $id: andThen notified, outputDelta size=${outputDelta.size}")

      // In Eager mode, emit even if empty - this signals "andThen completed with no results"
      if (outputDelta.nonEmpty || mode == RuntimeMode.Eager) {
        emit(outputDelta, actor)
      }
    } else {
      QPTrace.log(s"Sequence $id: unknown sender $from (expected first=$firstId or andThen=$andThenId)")
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

/** Bridge state that receives context from Sequence and forwards to andThen's entry point.
  *
  * This enables proper context flow in Sequence: first produces context, Sequence forwards
  * to this bridge, bridge forwards to andThen's entry point, andThen evaluates with that context.
  */
class ContextBridgeState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId, // The entry point state in andThen
  val mode: RuntimeMode,
  val sequenceId: StandingQueryId, // The Sequence state that sends context
) extends QueryState
    with PublishingState {

  // Log ContextBridge construction with connection info
  QPTrace.log(s"BRIDGE-CREATED id=$id publishTo=$publishTo sequenceId=$sequenceId")

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(
      s"ContextBridge $id: notify from=$from deltaSize=${delta.size} " +
      s"expectedFrom=$sequenceId forwardTo=$publishTo",
    )
    // Only accept context from the Sequence state
    if (from == sequenceId) {
      // Forward context to andThen's entry point
      emit(delta, actor)
    } else {
      QPTrace.log(s"ContextBridge $id: ignoring notification from unexpected sender $from")
    }
  }

  // Don't kickstart - wait for context from Sequence
  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class FilterState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val predicate: com.thatdot.quine.language.ast.Expression,
  val inputId: StandingQueryId,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.language.ast.Value
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    val outputDelta = mutable.Map.empty[QueryContext, Int]

    delta.foreach { case (ctx, mult) =>
      // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
      val env = EvalEnvironment(ctx, params)

      // Evaluate predicate
      val result = eval(predicate).run(env)
      result match {
        case Right(Value.True) =>
          outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + mult
        case Right(Value.False) | Right(Value.Null) =>
          // Filter out - don't add to output
          ()
        case Right(_) =>
          // Non-boolean result - treat as filter failure
          ()
        case Left(_) =>
          // Evaluation error - filter out
          ()
      }
    }

    // Emit non-zero deltas
    val nonZero = outputDelta.filter(_._2 != 0).toMap
    // In Eager mode, emit even if empty - this signals "processed input, no output" and
    // allows downstream states to know this branch completed (critical for completion signaling).
    if (nonZero.nonEmpty || mode == RuntimeMode.Eager) {
      emit(nonZero, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class ProjectState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val columns: List[Projection],
  val dropExisting: Boolean,
  val inputId: StandingQueryId,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    val outputDelta = mutable.Map.empty[QueryContext, Int]

    delta.foreach { case (ctx, mult) =>
      // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
      val env = EvalEnvironment(ctx, params)

      // Evaluate each projection column
      val projectedBindings = columns.flatMap { proj =>
        val result = eval(proj.expression).run(env)
        result match {
          case Right(value) => Some(proj.as -> value)
          case Left(_) => None // Skip on evaluation error
        }
      }.toMap

      if (projectedBindings.size == columns.size) {
        // All columns evaluated successfully
        val newBindings = if (dropExisting) projectedBindings else ctx.bindings ++ projectedBindings
        val newCtx = QueryContext(newBindings)
        outputDelta(newCtx) = outputDelta.getOrElse(newCtx, 0) + mult
      }
    // If some columns failed, we drop the row
    }

    // Emit non-zero deltas
    val nonZero = outputDelta.filter(_._2 != 0).toMap
    // In Eager mode, emit even if empty - this signals "processed input, no output"
    if (nonZero.nonEmpty || mode == RuntimeMode.Eager) {
      emit(nonZero, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class DistinctState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val inputId: StandingQueryId,
) extends QueryState
    with PublishingState {

  // Track count of how many times each context has been seen
  private val counts: mutable.Map[QueryContext, Int] = mutable.Map.empty

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    val outputDelta = mutable.Map.empty[QueryContext, Int]

    delta.foreach { case (ctx, mult) =>
      val oldCount = counts.getOrElse(ctx, 0)
      val newCount = oldCount + mult

      // Update count (remove if zero)
      if (newCount <= 0) counts -= ctx
      else counts(ctx) = newCount

      // Emit transitions at boundaries
      if (oldCount == 0 && newCount > 0) {
        // First occurrence: emit assertion
        outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + 1
      } else if (oldCount > 0 && newCount <= 0) {
        // Last occurrence removed: emit retraction
        outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) - 1
      }
    // Otherwise (still has multiple copies): no output change
    }

    // Emit non-zero deltas
    val nonZero = outputDelta.filter(_._2 != 0).toMap
    // In Eager mode, emit even if empty - this signals "processed input, no output"
    if (nonZero.nonEmpty || mode == RuntimeMode.Eager) {
      emit(nonZero, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class ExpandState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val edgeLabel: Option[Symbol],
  val direction: com.thatdot.quine.model.EdgeDirection,
  val onNeighborPlan: QueryPlan,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val namespace: com.thatdot.quine.graph.NamespaceId,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState
    with EdgeSensitiveState {

  import com.thatdot.quine.graph.behavior.QuinePatternCommand
  import com.thatdot.quine.graph.messaging.SpaceTimeQuineId

  // The QuineId of the node hosting this state (set in kickstart)
  private var originNodeId: Option[QuineId] = None

  // Track results from each neighbor, keyed by the edge
  private val neighborResults: mutable.Map[HalfEdge, (StandingQueryId, Delta.T)] = mutable.Map.empty

  // Track whether we've dispatched to any neighbors (for Eager mode empty result handling)
  private var dispatchedNeighborCount: Int = 0

  // Track how many neighbors have responded (for Eager mode buffering)
  private val respondedNeighbors: mutable.Set[StandingQueryId] = mutable.Set.empty

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit =
    // Find which neighbor sent this result
    neighborResults.find(_._2._1 == from) match {
      case Some((edge, (sqid, oldDelta))) =>
        // Update the neighbor's accumulated results
        val newDelta = Delta.add(oldDelta, delta)
        neighborResults(edge) = (sqid, newDelta)
        respondedNeighbors.add(from)

        val allNeighborsResponded = respondedNeighbors.size == dispatchedNeighborCount

        QPTrace.log(
          s"Expand $id: notify from=$from deltaSize=${delta.size} " +
          s"dispatched=$dispatchedNeighborCount responded=${respondedNeighbors.size} allResponded=$allNeighborsResponded",
        )

        mode match {
          case RuntimeMode.Eager =>
            // In Eager mode, wait for ALL dispatched neighbors to respond
            if (allNeighborsResponded) {
              val combined = neighborResults.values.map(_._2).foldLeft(Delta.empty)(Delta.add)
              QPTrace.log(s"Expand $id: all neighbors responded, emitting combined size=${combined.size}")
              emit(combined, actor)
            }

          case RuntimeMode.Lazy =>
            // In Lazy mode, emit the incoming delta incrementally
            // Each neighbor sends incremental deltas, so we pass them through directly
            // BUG FIX: Previously emitted ALL accumulated results on each update,
            // causing O(n^2) emissions. Now correctly emits only the new delta.
            if (delta.nonEmpty) {
              emit(delta, actor)
            }
        }
      case None =>
        // Unknown sender - ignore
        QPTrace.log(s"Expand $id: notify from unknown sender $from (not in neighborResults)")
        ()
    }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    // Store our node's QuineId for use in dispatchToNeighbor
    originNodeId = context.quineId

    // Dispatch plan to all matching existing edges
    context.edges.foreach { edge =>
      if (matchesEdge(edge)) {
        dispatchToNeighbor(edge, actor)
        dispatchedNeighborCount += 1
      }
    }

    // In Eager mode, if no neighbors matched, emit empty delta to signal "no results"
    if (mode == RuntimeMode.Eager && dispatchedNeighborCount == 0) {
      emit(Delta.empty, actor)
    }
  }

  override def onEdgeAdded(edge: HalfEdge, actor: ActorRef): Unit = {
    val matches = matchesEdge(edge)
    val alreadyTracked = neighborResults.contains(edge)
    QPTrace.log(
      s"Expand $id: onEdgeAdded edge=${edge.edgeType.name}(${edge.direction}) matches=$matches alreadyTracked=$alreadyTracked edgeLabel=${edgeLabel
        .map(_.name)} direction=$direction",
    )
    // Only dispatch if edge matches AND we haven't already dispatched for it
    // (kickstart may have already processed this edge before edge event fired)
    if (matches && !alreadyTracked) {
      dispatchToNeighbor(edge, actor)
    }
  }

  override def onEdgeRemoved(edge: HalfEdge, actor: ActorRef): Unit =
    neighborResults.remove(edge).foreach { case (sqid, oldDelta) =>
      // Retract the removed neighbor's contribution
      val retraction = oldDelta.view.mapValues(-_).toMap
      if (retraction.nonEmpty) {
        emit(retraction, actor)
      }
      // Send unregister message to the neighbor node
      val stqid = SpaceTimeQuineId(edge.other, namespace, None)
      graph.relayTell(stqid, QuinePatternCommand.UnregisterState(sqid))
    }

  private def matchesEdge(edge: HalfEdge): Boolean =
    edgeLabel.forall(_ == edge.edgeType) && edge.direction == direction

  private def dispatchToNeighbor(edge: HalfEdge, actor: ActorRef): Unit = {
    val sqid = StandingQueryId.fresh()
    neighborResults(edge) = (sqid, Delta.empty)

    originNodeId match {
      case Some(originNode) =>
        QPTrace.dispatchToNode(Some(originNode), edge.other, sqid, s"Expand(${edgeLabel.map(_.name).getOrElse("*")})")

        // Create the output target that will send results back to this state
        // Include sqid as dispatchId so results can be matched to neighborResults
        val output = OutputTarget.RemoteState(originNode, id, namespace, sqid)

        // Send LoadQueryPlan to load the neighbor plan on the neighbor node
        val neighborTarget = SpaceTimeQuineId(edge.other, namespace, None)
        graph.relayTell(
          neighborTarget,
          QuinePatternCommand.LoadQueryPlan(
            sqid = sqid,
            plan = onNeighborPlan,
            mode = mode,
            params = params, // Pass params to neighbor nodes for expression evaluation
            namespace = namespace,
            output = output,
          ),
        )

      case None =>
        // This shouldn't happen if kickstart was called first
        throw new IllegalStateException("ExpandState.dispatchToNeighbor called before kickstart")
    }
  }

  // Implement EdgeSensitiveState.watchedEdgeLabel
  override def watchedEdgeLabel: Option[Symbol] = edgeLabel
}

class AnchorState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val target: AnchorTarget,
  val onTargetPlan: QueryPlan,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val namespace: com.thatdot.quine.graph.NamespaceId,
  val fallbackOutput: Option[OutputTarget], // Used when hosted on NonNodeActor (no QuineId for RemoteState)
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
  val injectedContext: Map[
    Symbol,
    com.thatdot.quine.language.ast.Value,
  ], // Context from parent (e.g., Anchor dispatch) for evaluating target expressions
) extends QueryState
    with PublishingState
    with com.thatdot.quine.graph.quinepattern.NodeWakeHook {

  import com.thatdot.quine.language.ast.Value
  import com.thatdot.quine.graph.behavior.QuinePatternCommand
  import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  // The QuineId of the node hosting this state (set in kickstart, None for NonNodeActor)
  private var originNodeId: Option[QuineId] = None

  // The ActorRef of the actor hosting this state (set in kickstart, for routing back from dispatched plans)
  private var hostActorRef: Option[ActorRef] = None

  // Track results from target nodes, keyed by their QuineId
  private val targetResults: mutable.Map[QuineId, (StandingQueryId, Delta.T)] = mutable.Map.empty

  // Track which senders are known target results (for distinguishing from context injection)
  private val knownTargetSenders: mutable.Set[StandingQueryId] = mutable.Set.empty

  // Track how many targets have responded (for Eager mode buffering)
  private val respondedTargets: mutable.Set[StandingQueryId] = mutable.Set.empty

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit =
    // Check if this is from a known target (result flowing back)
    if (knownTargetSenders.contains(from)) {
      // Find which target sent this result
      targetResults.find(_._2._1 == from) match {
        case Some((qid, (sqid, oldDelta))) =>
          // Update the target's accumulated results
          val newDelta = Delta.add(oldDelta, delta)
          targetResults(qid) = (sqid, newDelta)
          respondedTargets.add(from)

          val allTargetsResponded = respondedTargets.size == dispatchedTargetCount

          QPTrace.log(
            s"Anchor $id: notify from=$from deltaSize=${delta.size} " +
            s"dispatched=$dispatchedTargetCount responded=${respondedTargets.size} allResponded=$allTargetsResponded",
          )

          mode match {
            case RuntimeMode.Eager =>
              // In Eager mode, wait for ALL dispatched targets to respond
              if (allTargetsResponded) {
                val combined = targetResults.values.map(_._2).foldLeft(Delta.empty)(Delta.add)
                QPTrace.log(s"Anchor $id: all targets responded, emitting combined size=${combined.size}")
                emit(combined, actor)
              }

            case RuntimeMode.Lazy =>
              // In Lazy mode, emit incrementally
              if (delta.nonEmpty) {
                emit(delta, actor)
              }
          }
        case None =>
          // Unknown sender - ignore
          QPTrace.log(s"Anchor $id: notify from unknown sender $from (not in targetResults)")
          ()
      }
    } else {
      // This is context injection (from ContextBridge in a Sequence)
      // The Anchor may not have been kickstarted (if it's an entry point for andThen),
      // so we need to set hostActorRef here for dispatchToTarget to work
      if (hostActorRef.isEmpty) {
        hostActorRef = Some(actor)
        QPTrace.log(s"Anchor $id: setting hostActorRef from context injection notify")
      }

      // Track how many dispatches happen during this notify call
      val dispatchCountBefore = dispatchedTargetCount

      // Use the context to evaluate target and dispatch
      delta.foreach { case (ctx, mult) =>
        if (mult > 0) {
          // Dispatch using this context
          dispatchWithContext(ctx, actor)
        }
      }

      // In Eager mode, if no contexts were dispatched (empty delta or all retractions),
      // we need to emit an empty delta to signal completion. Without this, the parent
      // Sequence waits forever for a response that will never come.
      val dispatchCountAfter = dispatchedTargetCount
      if (mode == RuntimeMode.Eager && dispatchCountAfter == dispatchCountBefore) {
        QPTrace.log(s"Anchor $id: empty context injection, emitting empty delta")
        emit(Delta.empty, actor)
      }
    }

  // Track whether we've dispatched any targets (for Eager mode empty result handling)
  private var dispatchedTargetCount: Int = 0

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    // Store our node's QuineId and host actor ref for use in dispatchToTarget
    originNodeId = context.quineId
    hostActorRef = Some(actor)

    target match {
      case AnchorTarget.Computed(expr) =>
        // Evaluate expression to get target node ID(s)
        // Use injectedContext (from Anchor dispatch) for evaluating expressions that reference bindings
        val env = EvalEnvironment(QueryContext(injectedContext), params)

        QPTrace.log(s"ANCHOR-KICKSTART id=$id expr=$expr params=[${params.keys
          .map(_.name)
          .mkString(",")}] injectedContext=[${injectedContext.keys.map(_.name).mkString(",")}]")
        params.get(Symbol("that")).foreach { thatVal =>
          QPTrace.log(s"ANCHOR-KICKSTART-THAT id=$id value=$thatVal")
        }

        eval(expr).run(env) match {
          case Right(Value.NodeId(qid)) =>
            QPTrace.log(s"ANCHOR-KICKSTART-EVAL id=$id result=NodeId($qid)")
            // Pass injectedContext to dispatched plan so nested Anchors can evaluate their targets
            dispatchToTarget(qid, injectedContext)
          case Right(Value.Node(qid, _, _)) =>
            QPTrace.log(s"ANCHOR-KICKSTART-EVAL id=$id result=Node(id=$qid)")
            // Pass injectedContext to dispatched plan so nested Anchors can evaluate their targets
            dispatchToTarget(qid, injectedContext)
          case Right(Value.List(values)) =>
            QPTrace.log(s"ANCHOR-KICKSTART-EVAL id=$id result=List(size=${values.size})")
            // Multiple targets - pass injectedContext to each
            values.foreach {
              case Value.NodeId(qid) => dispatchToTarget(qid, injectedContext)
              case Value.Node(qid, _, _) => dispatchToTarget(qid, injectedContext)
              case _ => () // Skip non-node values
            }
          case Right(other) =>
            QPTrace.log(s"ANCHOR-KICKSTART-EVAL id=$id result=Other($other) - no dispatch")
            // Couldn't evaluate to node ID(s) - no targets
            ()
          case Left(err) =>
            QPTrace.log(s"ANCHOR-KICKSTART-EVAL id=$id FAILED: $err")
            // Couldn't evaluate to node ID(s) - no targets
            ()
        }

        // In Eager mode, if we didn't dispatch any targets, emit empty delta to signal "no results"
        if (mode == RuntimeMode.Eager && dispatchedTargetCount == 0) {
          QPTrace.log(s"ANCHOR-KICKSTART-EMPTY id=$id emitting empty delta (no targets dispatched)")
          emit(Delta.empty, actor)
        }

      case AnchorTarget.AllNodes =>
        import org.apache.pekko.stream.Materializer
        implicit val mat: Materializer = Materializer(graph.system)
        mode match {
          case RuntimeMode.Eager =>
            // Enumerate all existing nodes and dispatch to each
            val _ = graph.enumerateAllNodeIds(namespace).runForeach { nodeId =>
              dispatchToTarget(nodeId)
            }
          case RuntimeMode.Lazy =>
            // For standing queries: enumerate all existing nodes AND register hook for new nodes
            // This ensures the standing query finds existing matches immediately, and also
            // maintains incrementally as new nodes appear.
            // Pass hostActorRef so new node notifications are sent via message (thread-safe)
            // TODO: Hook unregistration is not yet implemented - see cleanup() method for details
            hostActorRef match {
              case Some(ref) =>
                graph.registerNodeHook(this, ref)
                QPTrace.log(s"ANCHOR-HOOK-REGISTERED id=$id namespace=$namespace")
              case None =>
                QPLog.warn(
                  s"Anchor $id: Cannot register node wake hook - hostActorRef is None. " +
                  "New nodes will not trigger this standing query.",
                )
            }
            val _ = graph.enumerateAllNodeIds(namespace).runForeach { nodeId =>
              dispatchToTarget(nodeId)
            }
        }
    }
  }

  // NodeWakeHook implementation - provides info for sending NodeWake messages
  override def getNodeWakeInfo: (StandingQueryId, com.thatdot.quine.graph.NamespaceId, Map[Symbol, Value]) =
    (id, namespace, storedContext.getOrElse(Map.empty))

  /** Handle NodeWake message (called on the correct actor thread).
    * This is the thread-safe entry point for node wake dispatches.
    *
    * KNOWN LIMITATION - NODE SLEEP/WAKE STATE LOSS:
    * The targetResults check prevents re-dispatch to nodes we've already dispatched to.
    * This is correct for the initial enumeration race (node wakes during enumeration),
    * but causes issues when a target node sleeps and wakes later:
    *
    * 1. We dispatch to Node B, targetResults(B) = (sqid, delta)
    * 2. Node B installs state, publishes results
    * 3. Node B sleeps (state lost - not persisted)
    * 4. Node B wakes, NodeWake fires
    * 5. targetResults.contains(B) is TRUE, so no re-dispatch
    * 6. Node B's state is gone, updates stop flowing
    *
    * Future fix: When we implement state persistence (like MVSQ), nodes will restore
    * their states on wake. Until then, standing queries may lose coverage of nodes
    * that sleep and wake. See QueryStateHost trait docs for the full vision.
    */
  def handleNodeWake(nodeId: QuineId, context: Map[Symbol, Value], actor: ActorRef): Unit =
    // Only dispatch if we haven't already dispatched to this node
    // The targetResults check prevents double-dispatch when a node wakes up during
    // initial enumeration (where we dispatch during enumeration, then the wake triggers this hook).
    // Note: This also prevents re-dispatch after node sleep/wake - see method docs above.
    if (!targetResults.contains(nodeId)) {
      dispatchToTarget(nodeId, context)
    }

  /** Dispatch using injected context (from ContextBridge in a Sequence).
    * This evaluates the target expression using the injected context bindings.
    */
  private def dispatchWithContext(ctx: QueryContext, actor: ActorRef): Unit = {
    val dispatchedCount = dispatchedTargetCount // Capture before dispatch

    target match {
      case AnchorTarget.Computed(expr) =>
        // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
        val env = EvalEnvironment(ctx, params)

        QPTrace.log(s"Anchor $id: dispatchWithContext expr=$expr ctxBindings=[${ctx.bindings.keys
          .map(_.name)
          .mkString(",")}] params=[${params.keys.map(_.name).mkString(",")}]")

        eval(expr).run(env) match {
          case Right(Value.NodeId(qid)) =>
            QPTrace.log(s"Anchor $id: expr evaluated to NodeId=$qid")
            // Pass context bindings to target so effects can evaluate expressions
            dispatchToTarget(qid, ctx.bindings)
          case Right(Value.Node(qid, _, _)) =>
            QPTrace.log(s"Anchor $id: expr evaluated to Node with id=$qid")
            // Node value - extract its ID and dispatch
            dispatchToTarget(qid, ctx.bindings)
          case Right(Value.List(values)) =>
            QPTrace.log(s"Anchor $id: expr evaluated to List size=${values.size}")
            // Multiple targets
            values.foreach {
              case Value.NodeId(qid) =>
                dispatchToTarget(qid, ctx.bindings)
              case Value.Node(qid, _, _) =>
                dispatchToTarget(qid, ctx.bindings)
              case _ => () // Skip non-node values
            }
          case Right(other) =>
            QPTrace.log(s"Anchor $id: expr evaluated to non-NodeId: $other")
            // Couldn't evaluate to node ID(s) - no targets
            ()
          case Left(err) =>
            QPTrace.log(s"Anchor $id: expr evaluation FAILED: $err")
            // Couldn't evaluate to node ID(s) - no targets
            ()
        }

        // In Eager mode, if no new targets were dispatched for this context, emit empty delta
        if (mode == RuntimeMode.Eager && dispatchedTargetCount == dispatchedCount) {
          emit(Delta.empty, actor)
        }

      case AnchorTarget.AllNodes =>
        // For AllNodes with context injection, dispatch to all nodes with context
        import org.apache.pekko.stream.Materializer
        implicit val mat: Materializer = Materializer(graph.system)
        mode match {
          case RuntimeMode.Eager =>
            val _ = graph.enumerateAllNodeIds(namespace).runForeach { nodeId =>
              dispatchToTarget(nodeId, ctx.bindings)
            }
          case RuntimeMode.Lazy =>
            // Store context for later node wake dispatches and enumerate existing nodes
            storedContext = Some(ctx.bindings)
            // Pass hostActorRef so new node notifications are sent via message (thread-safe)
            // TODO: Hook unregistration is not yet implemented - see cleanup() method for details
            hostActorRef match {
              case Some(ref) =>
                graph.registerNodeHook(this, ref)
                QPTrace.log(
                  s"ANCHOR-HOOK-REGISTERED-WITH-CONTEXT id=$id namespace=$namespace contextKeys=${ctx.bindings.keys.map(_.name).mkString(",")}",
                )
              case None =>
                QPLog.warn(
                  s"Anchor $id: Cannot register node wake hook (with context) - hostActorRef is None. " +
                  "New nodes will not trigger this standing query.",
                )
            }
            val _ = graph.enumerateAllNodeIds(namespace).runForeach { nodeId =>
              dispatchToTarget(nodeId, ctx.bindings)
            }
        }
    }
  }

  // Context stored for AllNodes + Lazy mode (to inject into newly waking nodes)
  private var storedContext: Option[Map[Symbol, Value]] = None

  /** Dispatch to target node, optionally with injected context for the onTarget plan.
    *
    * @param qid The target node to dispatch to
    * @param injectedContext Context bindings to seed into the dispatched plan (for LocalEffect to evaluate expressions)
    */
  private def dispatchToTarget(qid: QuineId, injectedContext: Map[Symbol, Value] = Map.empty): Unit = {
    val sqid = StandingQueryId.fresh()
    targetResults(qid) = (sqid, Delta.empty)
    knownTargetSenders.add(sqid) // Track this sender for notify()
    dispatchedTargetCount += 1 // Track for empty result handling in Eager mode

    val targetType = target match {
      case AnchorTarget.Computed(_) => "Computed"
      case AnchorTarget.AllNodes => "AllNodes"
    }
    QPTrace.dispatchToNode(originNodeId, qid, sqid, s"Anchor($targetType)")
    QPTrace.log(s"Anchor dispatch: injectedContext keys=[${injectedContext.keys.map(_.name).mkString(",")}]")

    // Determine the output target for the dispatched plan
    val output = originNodeId match {
      case Some(originNode) =>
        // Results flow back to this state on the origin node
        // Include sqid as dispatchId so results can be matched to targetResults
        OutputTarget.RemoteState(originNode, id, namespace, sqid)
      case None =>
        // No origin node (e.g., NonNodeActor)
        // Use HostedState to route results back to this Anchor state on the host actor
        // This allows multi-Anchor Sequences to work: first Anchor's results flow back
        // to the Sequence, which then forwards context to the second Anchor
        hostActorRef match {
          case Some(actorRef) =>
            // Include sqid so OutputState uses it as 'from' - this lets us identify
            // the results as coming from a known target (not context injection)
            OutputTarget.HostedState(actorRef, id, sqid)
          case None =>
            // No host actor ref - use fallback output (final output)
            fallbackOutput.getOrElse {
              QPLog.warn(
                "AnchorState has no originNodeId, no hostActorRef, and no fallbackOutput. " +
                "Results may not be delivered correctly.",
              )
              OutputTarget.StandingQuerySink(sqid, namespace)
            }
        }
    }

    // Send LoadQueryPlan to load the target plan on the target node
    // Include injectedContext so LocalEffect states can evaluate expressions
    val targetStqid = SpaceTimeQuineId(qid, namespace, None)
    graph.relayTell(
      targetStqid,
      QuinePatternCommand.LoadQueryPlan(
        sqid = sqid,
        plan = onTargetPlan,
        mode = mode,
        params = params, // Pass params to target nodes for expression evaluation
        namespace = namespace,
        output = output,
        injectedContext = injectedContext, // Pass context bindings for LocalEffect expression evaluation
      ),
    )
  }

  /** Clean up when this state is unregistered.
    *
    * CURRENT STATUS: This method is intentionally NOT called from UnregisterState handler.
    * We use "soft unregister" to avoid waking large subgraphs for cleanup.
    * See UnregisterState handler in QuinePatternQueryBehavior for rationale.
    *
    * KNOWN ISSUES:
    *
    * 1. NODE HOOK MEMORY LEAK:
    *    This method does not call graph.unregisterNodeHook(this) to remove the hook
    *    registered in kickstart() for Lazy mode AllNodes anchors. This causes:
    *    - Hook references accumulate in QuinePatternOpsGraph.nodeHooks
    *    - Every new node creation dispatches NodeWake messages to orphaned hooks
    *    - Memory grows with standing query deploy/undeploy cycles
    *
    * 2. EAGER CLEANUP WOULD WAKE SUBGRAPHS:
    *    The current implementation sends UnregisterState to all target nodes via
    *    graph.relayTell, which would wake sleeping nodes. For large standing queries
    *    spanning many nodes, this could cause significant unnecessary node activation.
    *
    * FUTURE IMPLEMENTATION (see QueryStateHost trait docs):
    *
    * When we implement proper standing query lifecycle management:
    *
    * 1. Fix hook leak: Add graph.unregisterNodeHook(this) for AnchorState instances
    *
    * 2. Lazy cleanup for child states:
    *    - Don't eagerly send UnregisterState to target nodes
    *    - Instead, use a global standing query registry
    *    - Nodes validate state relevance on wake via registry lookup
    *    - Orphaned states self-cleanup when they detect parent query is gone
    *
    * 3. State persistence (like MVSQ):
    *    - Persist states so they survive node sleep/wake
    *    - On wake, restore states and validate against registry
    *    - Enables proper incremental cleanup without waking entire subgraphs
    *
    * 4. Epoch tracking:
    *    - Standing queries have generation numbers
    *    - States check generation before publishing
    *    - Wrong generation = stale state = self-terminate
    */
  def cleanup(): Unit =
    // Note: This sends UnregisterState to all target nodes, which would wake them.
    // Currently not called to avoid waking subgraphs. Orphaned states are harmless
    // since their updates are dropped when the parent state no longer exists.
    targetResults.foreach { case (qid, (sqid, _)) =>
      val targetStqid = SpaceTimeQuineId(qid, namespace, None)
      graph.relayTell(targetStqid, QuinePatternCommand.UnregisterState(sqid))
    }
}

class UnwindState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val listExpr: com.thatdot.quine.language.ast.Expression,
  val binding: Symbol,
  val subqueryId: StandingQueryId,
  val contextBridgeId: Option[StandingQueryId], // Bridge that forwards bindings to subquery's entry point
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.language.ast.Value
  import com.thatdot.quine.graph.behavior.QuinePatternCommand
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  // Log Unwind construction with connection info
  QPTrace.log(
    s"UNWIND-CREATED id=$id subqueryId=$subqueryId bridgeId=${contextBridgeId.getOrElse("none")} publishTo=$publishTo",
  )

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(s"Unwind $id: notify from=$from deltaSize=${delta.size} isSubquery=${from == subqueryId}")

    if (from == subqueryId) {
      // Results from subquery - pass through to parent
      QPTrace.log(s"Unwind $id: passing through subquery results to parent")
      emit(delta, actor)
    } else {
      // Incoming context from parent (when Unwind is an entry point)
      // Evaluate list with this context and forward to subquery via bridge
      QPTrace.log(s"Unwind $id: received parent context, evaluating list")
      processWithContext(delta, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    QPTrace.log(s"Unwind $id: kickstart bridgeId=${contextBridgeId.getOrElse("none")}")
    // Evaluate the list expression with empty context (no parent bindings)
    processWithContext(Map(QueryContext.empty -> 1), actor)
  }

  /** Evaluate list expression for each incoming context and forward to subquery */
  private def processWithContext(incomingDelta: Delta.T, actor: ActorRef): Unit = {
    val outputDelta = mutable.Map.empty[QueryContext, Int]

    incomingDelta.foreach { case (incomingCtx, mult) =>
      if (mult > 0) {
        // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
        val env = EvalEnvironment(incomingCtx, params)

        eval(listExpr).run(env) match {
          case Right(Value.List(values)) =>
            // For each element, create a combined context
            values.foreach { value =>
              val ctx = incomingCtx ++ Map(binding -> value)
              outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + mult
            }
          case Right(Value.Null) =>
            // Null list - no output for this context
            ()
          case Right(other) =>
            // Non-list value - treat as single-element list
            val ctx = incomingCtx ++ Map(binding -> other)
            outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + mult
          case Left(err) =>
            // Evaluation error - log and skip
            QPTrace.log(s"Unwind $id: list evaluation error: $err")
        }
      }
    }

    QPTrace.log(s"Unwind $id: produced ${outputDelta.size} bindings")

    // Forward to subquery via context bridge
    contextBridgeId match {
      case Some(bridgeId) =>
        if (outputDelta.nonEmpty || mode == RuntimeMode.Eager) {
          QPTrace.log(s"Unwind $id: forwarding to bridge $bridgeId")
          actor ! QuinePatternCommand.QueryUpdate(bridgeId, id, outputDelta.toMap)
        }
      case None =>
        // No bridge - emit directly to parent (fallback for simple cases without subquery)
        if (outputDelta.nonEmpty || mode == RuntimeMode.Eager) {
          emit(outputDelta.toMap, actor)
        }
    }
  }
}

/** State for executing procedure calls.
  *
  * Similar to UnwindState, this executes a subquery for each result row produced
  * by the procedure. The procedure is executed when context is injected (for
  * standing queries) or at kickstart (for eager queries).
  */
class ProcedureState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val procedureName: Symbol,
  val arguments: List[com.thatdot.quine.language.ast.Expression],
  val yields: List[(Symbol, Symbol)],
  val subqueryId: StandingQueryId,
  val contextBridgeId: Option[StandingQueryId],
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val namespace: NamespaceId,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import scala.collection.mutable
  import scala.concurrent.duration._

  import org.apache.pekko.util.Timeout

  import com.thatdot.quine.language.ast.Value
  import com.thatdot.quine.graph.behavior.QuinePatternCommand
  import com.thatdot.quine.graph.cypher.quinepattern.procedures.{
    GetFilteredEdgesProcedure,
    ProcedureContext,
    QuinePatternProcedureRegistry,
  }
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val ec: scala.concurrent.ExecutionContext = graph.nodeDispatcherEC
  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider
  implicit private val timeout: Timeout = Timeout(30.seconds)

  // Ensure getFilteredEdges is registered
  QuinePatternProcedureRegistry.register(GetFilteredEdgesProcedure)

  QPTrace.log(
    s"PROCEDURE-CREATED id=$id procedure=$procedureName subqueryId=$subqueryId bridgeId=${contextBridgeId
      .getOrElse("none")} publishTo=$publishTo",
  )

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(s"Procedure $id: notify from=$from deltaSize=${delta.size} isSubquery=${from == subqueryId}")

    if (from == subqueryId) {
      // Results from subquery - pass through to parent
      QPTrace.log(s"Procedure $id: passing through subquery results to parent")
      emit(delta, actor)
    } else {
      // Incoming context from parent (when Procedure is an entry point)
      // Execute procedure with this context and forward results to subquery via bridge
      QPTrace.log(s"Procedure $id: received parent context, executing procedure")
      processWithContext(delta, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    QPTrace.log(s"Procedure $id: kickstart bridgeId=${contextBridgeId.getOrElse("none")}")
    // Execute the procedure with empty context (no parent bindings)
    processWithContext(Map(QueryContext.empty -> 1), actor)
  }

  /** Execute procedure for each incoming context and forward results to subquery */
  private def processWithContext(incomingDelta: Delta.T, actor: ActorRef): Unit = {
    import scala.concurrent.Future

    // Look up the procedure
    val procedureOpt = QuinePatternProcedureRegistry.get(procedureName.name)
    procedureOpt match {
      case None =>
        QPTrace.log(s"Procedure $id: unknown procedure '${procedureName.name}'")
        // Unknown procedure - emit error or empty in eager mode
        if (mode == RuntimeMode.Eager) {
          contextBridgeId match {
            case Some(bridgeId) =>
              actor ! QuinePatternCommand.QueryUpdate(bridgeId, id, Map.empty)
            case None =>
              emit(Map.empty, actor)
          }
        }

      case Some(procedure) =>
        // Collect all futures for procedure executions
        // Each future returns a delta map for its input context
        val futures: Seq[Future[Map[QueryContext, Int]]] = incomingDelta.toSeq.flatMap { case (incomingCtx, mult) =>
          if (mult > 0) {
            // Evaluate arguments in context
            val env = EvalEnvironment(incomingCtx, params)
            val evaluatedArgs: Seq[Value] = arguments.map { argExpr =>
              eval(argExpr).run(env) match {
                case Right(value) => value
                case Left(err) =>
                  QPTrace.log(s"Procedure $id: argument evaluation error: $err")
                  Value.Null
              }
            }

            // Create procedure context
            // Note: At runtime, the graph is a GraphService that extends both QuinePatternOpsGraph
            // and LiteralOpsGraph, so this cast is safe
            val literalGraph =
              graph.asInstanceOf[com.thatdot.quine.graph.BaseGraph with com.thatdot.quine.graph.LiteralOpsGraph]
            val procContext = ProcedureContext(
              graph = literalGraph,
              namespace = namespace,
              atTime = None, // TODO: Support historical queries
              timeout = timeout,
            )

            // Execute the procedure and map results to delta
            val resultFuture: Future[Map[QueryContext, Int]] = procedure
              .execute(evaluatedArgs, procContext)
              .map { results =>
                val outputDelta = mutable.Map.empty[QueryContext, Int]
                results.foreach { resultRow =>
                  // Map procedure outputs to yield symbols
                  // yields is List[(resultField, boundAs)] - look up resultField, bind to boundAs
                  val bindings: Map[Symbol, Value] = yields.flatMap { case (resultField, boundAs) =>
                    resultRow.get(resultField.name).map(boundAs -> _)
                  }.toMap

                  // Combine with incoming context
                  val ctx = incomingCtx ++ bindings
                  outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + mult
                }
                QPTrace.log(s"Procedure $id: context produced ${outputDelta.size} bindings")
                outputDelta.toMap
              }
              .recover { case err =>
                QPTrace.log(s"Procedure $id: execution error: ${err.getMessage}")
                Map.empty[QueryContext, Int]
              }

            Some(resultFuture)
          } else {
            None
          }
        }

        // Wait for ALL procedure calls to complete, then emit combined results
        Future.sequence(futures).foreach { allDeltas =>
          // Combine all deltas into one
          val combinedDelta = mutable.Map.empty[QueryContext, Int]
          allDeltas.foreach { delta =>
            delta.foreach { case (ctx, mult) =>
              combinedDelta(ctx) = combinedDelta.getOrElse(ctx, 0) + mult
            }
          }

          QPTrace.log(s"Procedure $id: all calls complete, combined ${combinedDelta.size} bindings")

          // Forward combined results to subquery via context bridge
          contextBridgeId match {
            case Some(bridgeId) =>
              if (combinedDelta.nonEmpty || mode == RuntimeMode.Eager) {
                QPTrace.log(s"Procedure $id: forwarding combined results to bridge $bridgeId")
                actor ! QuinePatternCommand.QueryUpdate(bridgeId, id, combinedDelta.toMap)
              }
            case None =>
              if (combinedDelta.nonEmpty || mode == RuntimeMode.Eager) {
                emit(combinedDelta.toMap, actor)
              }
          }
        }
    }
  }
}

class EffectState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val effects: List[LocalQueryEffect],
  val inputId: StandingQueryId,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val currentNodeId: Option[com.thatdot.common.quineid.QuineId], // The node this effect runs on (None if NonNodeActor)
  val namespace: NamespaceId,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.language.ast.Value
  import com.thatdot.quine.graph.behavior.QuinePatternCommand
  import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  val hasNodeContext: Boolean = currentNodeId.isDefined

  // Log Effect construction with connection info
  QPTrace.log(
    s"EFFECT-CREATED id=$id inputId=$inputId publishTo=$publishTo effectCount=${effects.size} currentNode=${currentNodeId.map(idProvider.qidToPrettyString).getOrElse("none")}",
  )

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    QPTrace.log(
      s"Effect $id: notify from=$from deltaSize=${delta.size} effectCount=${effects.size} hasNodeContext=$hasNodeContext",
    )

    // Build modified delta with updated contexts after applying effects
    val modifiedDelta = delta.map { case (ctx, mult) =>
      if (mult > 0) {
        // Apply effects and collect context updates
        val updatedCtx = effects.foldLeft(ctx) { (currentCtx, effect) =>
          applyEffectAndUpdateContext(effect, currentCtx, actor)
        }
        (updatedCtx, mult)
      } else {
        // Retractions pass through unchanged
        // Note: We don't "un-apply" effects for retractions
        // Effects are typically not reversible (SET property, etc.)
        (ctx, mult)
      }
    }

    // Pass modified delta to parent
    emit(modifiedDelta, actor)
  }

  /** Apply an effect and return updated context.
    * This both fires the async persistence message AND updates the context
    * so subsequent operations (like RETURN) see the new values.
    * Similar to how the ad-hoc interpreter handles SET.
    */
  private def applyEffectAndUpdateContext(
    effect: LocalQueryEffect,
    ctx: QueryContext,
    actor: ActorRef,
  ): QueryContext = {
    import scala.collection.immutable.SortedMap

    // First apply the effect (fire-and-forget to persist)
    applyEffect(effect, ctx, actor)

    // Then update the context if this is a SET effect
    val env = EvalEnvironment(ctx, params)

    /** Update a property in the context for a target binding.
      * Handles both Value.Node (from LocalNode) and Value.Map (from LocalAllProperties).
      */
    def updatePropertyInContext(targetBindingOpt: Option[Symbol], property: Symbol, newValue: Value): QueryContext = {
      // Determine which binding to update
      val targetKey: Option[Symbol] = targetBindingOpt.orElse {
        // Effect is inside an anchor - find a node binding in context
        ctx.bindings
          .collectFirst { case (k, _: Value.Node) => k }
          .orElse(ctx.bindings.collectFirst { case (k, _: Value.Map) => k })
      }

      targetKey match {
        case Some(key) =>
          ctx.bindings.get(key) match {
            case Some(Value.Node(id, labels, Value.Map(existingProps))) =>
              // Update the properties within the Value.Node
              val updatedNode = Value.Node(id, labels, Value.Map(existingProps + (property -> newValue)))
              QueryContext(ctx.bindings + (key -> updatedNode))
            case Some(Value.Map(existingProps)) =>
              // Update the properties map directly (for LocalAllProperties case)
              val updatedMap = Value.Map(existingProps + (property -> newValue))
              QueryContext(ctx.bindings + (key -> updatedMap))
            case _ =>
              ctx
          }
        case None =>
          ctx
      }
    }

    /** Update multiple properties in the context for a target binding. */
    def updatePropertiesInContext(
      targetBindingOpt: Option[Symbol],
      newProps: SortedMap[Symbol, Value],
    ): QueryContext = {
      val targetKey: Option[Symbol] = targetBindingOpt.orElse {
        ctx.bindings
          .collectFirst { case (k, _: Value.Node) => k }
          .orElse(ctx.bindings.collectFirst { case (k, _: Value.Map) => k })
      }

      targetKey match {
        case Some(key) =>
          ctx.bindings.get(key) match {
            case Some(Value.Node(id, labels, Value.Map(existingProps))) =>
              val updatedNode = Value.Node(id, labels, Value.Map(existingProps ++ newProps))
              QueryContext(ctx.bindings + (key -> updatedNode))
            case Some(Value.Map(existingProps)) =>
              val updatedMap = Value.Map(existingProps ++ newProps)
              QueryContext(ctx.bindings + (key -> updatedMap))
            case _ =>
              ctx
          }
        case None =>
          ctx
      }
    }

    effect match {
      case LocalQueryEffect.SetProperty(targetBindingOpt, property, valueExpr) =>
        eval(valueExpr).run(env) match {
          case Right(newValue) =>
            updatePropertyInContext(targetBindingOpt, property, newValue)
          case Left(_) =>
            ctx
        }

      case LocalQueryEffect.SetProperties(targetBindingOpt, propsExpr) =>
        eval(propsExpr).run(env) match {
          case Right(Value.Map(newProps)) =>
            updatePropertiesInContext(targetBindingOpt, newProps)
          case _ =>
            ctx
        }

      case LocalQueryEffect.SetLabels(targetBindingOpt, newLabels) =>
        // Update labels in the context's Value.Node
        val targetKey: Option[Symbol] = targetBindingOpt.orElse {
          // Effect is inside an anchor - find a node binding in context
          ctx.bindings.collectFirst { case (k, _: Value.Node) => k }
        }

        targetKey match {
          case Some(key) =>
            ctx.bindings.get(key) match {
              case Some(Value.Node(id, existingLabels, props)) =>
                // Merge new labels with existing (SET adds labels, doesn't replace)
                val updatedNode = Value.Node(id, existingLabels ++ newLabels, props)
                QueryContext(ctx.bindings + (key -> updatedNode))
              case _ =>
                ctx
            }
          case None =>
            ctx
        }

      case _ =>
        // Other effects (CreateHalfEdge, CreateNode, Foreach) don't modify the context
        ctx
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()

  private def applyEffect(effect: LocalQueryEffect, ctx: QueryContext, actor: ActorRef): Unit = {
    // Warn if we're trying to apply effects without a node context
    if (!hasNodeContext) {
      QPLog.warn(
        s"EffectState is applying effect $effect without a node context. " +
        "This indicates a planner bug - effects should be inside an Anchor's onTarget so they run on actual nodes.",
      )
    }

    // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
    val env = EvalEnvironment(ctx, params)

    effect match {
      case LocalQueryEffect.CreateNode(_, _, _) =>
        // CreateNode not yet implemented
        ()

      case LocalQueryEffect.SetProperty(targetBindingOpt, property, valueExpr) =>
        eval(valueExpr).run(env) match {
          case Right(_) =>
            // Determine which node should receive this property
            val targetNodeOpt: Option[com.thatdot.common.quineid.QuineId] = targetBindingOpt.flatMap { targetBinding =>
              ctx.bindings.get(targetBinding) match {
                case Some(Value.NodeId(qid)) => Some(qid)
                case Some(Value.Node(qid, _, _)) => Some(qid)
                case _ => None
              }
            }

            targetNodeOpt match {
              case Some(targetQid) if currentNodeId.contains(targetQid) =>
                // Target matches current node - set property locally
                actor ! QuinePatternCommand.SetProperty(property, valueExpr, ctx, params)
              case Some(targetQid) =>
                // Target is a different node - dispatch via relayTell
                QPTrace.log(
                  s"SetProperty: remote dispatch to ${idProvider.qidToPrettyString(targetQid)} for property $property",
                )
                val stqid = SpaceTimeQuineId(targetQid, namespace, None)
                graph.relayTell(stqid, QuinePatternCommand.SetProperty(property, valueExpr, ctx, params))
              case None =>
                // No target binding - use current actor (legacy behavior)
                actor ! QuinePatternCommand.SetProperty(property, valueExpr, ctx, params)
            }
          case Left(_) =>
            ()
        }

      case LocalQueryEffect.SetProperties(targetBindingOpt, propsExpr) =>
        eval(propsExpr).run(env) match {
          case Right(Value.Map(props)) =>
            // Determine which node should receive these properties
            val targetNodeOpt: Option[com.thatdot.common.quineid.QuineId] = targetBindingOpt.flatMap { targetBinding =>
              ctx.bindings.get(targetBinding) match {
                case Some(Value.NodeId(qid)) => Some(qid)
                case Some(Value.Node(qid, _, _)) => Some(qid)
                case _ => None
              }
            }

            targetNodeOpt match {
              case Some(targetQid) if currentNodeId.contains(targetQid) =>
                actor ! QuinePatternCommand.SetProperties(props)
              case Some(targetQid) =>
                QPTrace.log(s"SetProperties: remote dispatch to ${idProvider.qidToPrettyString(targetQid)}")
                val stqid = SpaceTimeQuineId(targetQid, namespace, None)
                graph.relayTell(stqid, QuinePatternCommand.SetProperties(props))
              case None =>
                actor ! QuinePatternCommand.SetProperties(props)
            }
          case _ =>
            ()
        }

      case LocalQueryEffect.SetLabels(targetBindingOpt, labels) =>
        // Determine which node should receive these labels
        val targetNodeOpt: Option[com.thatdot.common.quineid.QuineId] = targetBindingOpt.flatMap { targetBinding =>
          ctx.bindings.get(targetBinding) match {
            case Some(Value.NodeId(qid)) => Some(qid)
            case Some(Value.Node(qid, _, _)) => Some(qid)
            case _ => None
          }
        }

        targetNodeOpt match {
          case Some(targetQid) if currentNodeId.contains(targetQid) =>
            actor ! QuinePatternCommand.SetLabels(labels)
          case Some(targetQid) =>
            QPTrace.log(s"SetLabels: remote dispatch to ${idProvider.qidToPrettyString(targetQid)}")
            val stqid = SpaceTimeQuineId(targetQid, namespace, None)
            graph.relayTell(stqid, QuinePatternCommand.SetLabels(labels))
          case None =>
            actor ! QuinePatternCommand.SetLabels(labels)
        }

      case LocalQueryEffect.CreateHalfEdge(sourceBindingOpt, label, direction, otherExpr) =>
        // Evaluate the "other" node (the far end of the edge)
        val otherNodeOpt: Option[com.thatdot.common.quineid.QuineId] = eval(otherExpr).run(env) match {
          case Right(Value.NodeId(qid)) => Some(qid)
          case Right(Value.Node(qid, _, _)) => Some(qid)
          case _ => None
        }

        // Determine which node should create this half-edge
        val sourceNodeOpt: Option[com.thatdot.common.quineid.QuineId] = sourceBindingOpt.flatMap { sourceBinding =>
          ctx.bindings.get(sourceBinding) match {
            case Some(Value.NodeId(qid)) => Some(qid)
            case Some(Value.Node(qid, _, _)) => Some(qid)
            case _ => None
          }
        }

        (otherNodeOpt, sourceNodeOpt) match {
          case (Some(otherQid), Some(sourceQid)) if currentNodeId.contains(sourceQid) =>
            // Source matches current node - create edge locally
            QPTrace.log(
              s"CreateHalfEdge: local edge creation on ${idProvider.qidToPrettyString(sourceQid)} to ${idProvider.qidToPrettyString(otherQid)}",
            )
            actor ! QuinePatternCommand.CreateEdge(otherQid, direction, label)

          case (Some(otherQid), Some(sourceQid)) =>
            // Source is a different node - dispatch via relayTell
            QPTrace.log(
              s"CreateHalfEdge: remote dispatch to ${idProvider.qidToPrettyString(sourceQid)} for edge to ${idProvider.qidToPrettyString(otherQid)}",
            )
            val stqid = SpaceTimeQuineId(sourceQid, namespace, None)
            graph.relayTell(stqid, QuinePatternCommand.CreateEdge(otherQid, direction, label))

          case (Some(otherQid), None) =>
            // No source binding - use current actor (legacy behavior)
            actor ! QuinePatternCommand.CreateEdge(otherQid, direction, label)

          case _ =>
            // Couldn't evaluate nodes - skip this effect
            ()
        }

      case LocalQueryEffect.Foreach(binding, listExpr, nestedEffects) =>
        QPTrace.log(
          s"FOREACH: evaluating listExpr=$listExpr with context bindings=${ctx.bindings.keys.map(_.name).mkString(",")}",
        )
        val evalResult = eval(listExpr).run(env)
        QPTrace.log(s"FOREACH: eval result=$evalResult")
        evalResult match {
          case Right(Value.List(values)) =>
            QPTrace.log(s"FOREACH: iterating over ${values.size} values")
            values.foreach { value =>
              val loopCtx = QueryContext(ctx.bindings + (binding -> value))
              nestedEffects.foreach(nestedEffect => applyEffect(nestedEffect, loopCtx, actor))
            }
          case Right(other) =>
            QPTrace.log(s"FOREACH: listExpr evaluated to non-List: $other")
          case Left(err) =>
            QPTrace.log(s"FOREACH: listExpr evaluation failed: $err")
        }
    }
  }
}

class AggregateState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val aggregations: List[Aggregation],
  val groupBy: List[Symbol],
  val inputId: StandingQueryId,
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.language.ast.Value

  // Accumulated state: all input contexts with their multiplicities
  private var accumulatedState: Delta.T = Delta.empty
  private var hasEmittedAggregate: Boolean = false

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    // Lazy mode aggregation is not supported - it requires materialization which
    // doesn't fit the incremental streaming model of standing queries
    if (mode == RuntimeMode.Lazy) {
      throw new UnsupportedOperationException(
        "Aggregation (count, sum, collect, etc.) is not supported in lazy/standing query mode. " +
        "Aggregation requires materialization of all results before computing, which conflicts with " +
        "the incremental streaming model. Use eager mode (iterate) for queries with aggregations.",
      )
    }

    // Accumulate inputs
    accumulatedState = Delta.add(accumulatedState, delta)

    // In Eager mode, emit once (even if empty) to signal completion
    if (!hasEmittedAggregate) {
      val result = if (accumulatedState.nonEmpty) computeAggregate() else Delta.empty
      emit(result, actor)
      hasEmittedAggregate = true
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()

  private def computeAggregate(): Delta.T = {
    // Group contexts by groupBy keys
    val groups = if (groupBy.isEmpty) {
      // Single group - all inputs
      Map(QueryContext.empty -> expandDelta(accumulatedState))
    } else {
      // Group by specified keys
      expandDelta(accumulatedState)
        .groupBy(ctx => QueryContext(groupBy.flatMap(k => ctx.bindings.get(k).map(k -> _)).toMap))
    }

    // Compute aggregations for each group
    val results = groups.map { case (groupKey, contexts) =>
      val aggBindings = aggregations.zipWithIndex.flatMap { case (agg, idx) =>
        val binding = Symbol(s"agg_$idx") // Default binding name
        computeSingleAggregation(agg, contexts).map(binding -> _)
      }.toMap
      QueryContext(groupKey.bindings ++ aggBindings) -> 1
    }

    results.toMap
  }

  // Expand delta to list of contexts (respecting multiplicities)
  private def expandDelta(delta: Delta.T): List[QueryContext] =
    delta.toList.flatMap { case (ctx, mult) =>
      if (mult > 0) List.fill(mult)(ctx)
      else Nil
    }

  private def computeSingleAggregation(agg: Aggregation, contexts: List[QueryContext]): Option[Value] =
    agg match {
      case Aggregation.Count(distinct) =>
        val count = if (distinct) contexts.toSet.size else contexts.size
        Some(Value.Integer(count.toLong))

      case Aggregation.Collect(expr, distinct) =>
        // For simplicity, collect all values of the expression binding
        // A full implementation would evaluate expr for each context
        val values = contexts.flatMap(_.bindings.values.headOption)
        val finalValues = if (distinct) values.distinct else values
        Some(Value.List(finalValues))

      case _ =>
        // Sum, Avg, Min, Max would need expression evaluation
        // Leaving as placeholder
        None
    }
}

class SortState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val orderBy: List[SortKey],
  val inputId: StandingQueryId,
  val graph: com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph,
  val params: Map[Symbol, com.thatdot.quine.language.ast.Value],
) extends QueryState
    with PublishingState {

  import com.thatdot.quine.language.ast.Value
  import QuinePatternExpressionInterpreter.{EvalEnvironment, eval}

  implicit private val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  // Accumulated state
  private var accumulatedState: Delta.T = Delta.empty
  private var hasEmittedSort: Boolean = false

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    // Lazy mode sort is not supported - it requires materialization which
    // doesn't fit the incremental streaming model of standing queries
    if (mode == RuntimeMode.Lazy) {
      throw new UnsupportedOperationException(
        "ORDER BY is not supported in lazy/standing query mode. " +
        "Sorting requires materialization of all results before ordering, which conflicts with " +
        "the incremental streaming model. Use eager mode (iterate) for queries with ORDER BY.",
      )
    }

    // Accumulate inputs
    accumulatedState = Delta.add(accumulatedState, delta)

    // In Eager mode, emit once (even if empty) to signal completion
    if (!hasEmittedSort) {
      val result = if (accumulatedState.nonEmpty) computeSorted() else Delta.empty
      emit(result, actor)
      hasEmittedSort = true
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()

  private def computeSorted(): Delta.T = {
    // Expand to list, sort, then convert back to delta
    val expanded = accumulatedState.toList.flatMap { case (ctx, mult) =>
      if (mult > 0) List.fill(mult)(ctx) else Nil
    }

    // Sort using the orderBy keys
    val sorted = expanded.sortWith { (a, b) =>
      compareContexts(a, b) < 0
    }

    // Convert back to delta (preserving order by using position-based identity)
    // Note: In a real streaming system, sorted results would be emitted as a stream
    sorted.zipWithIndex.map { case (ctx, _) => ctx -> 1 }.toMap
  }

  private def compareContexts(a: QueryContext, b: QueryContext): Int =
    orderBy.foldLeft(0) { (result, sortKey) =>
      if (result != 0) result
      else {
        // Evaluate sort key expression for both contexts
        val valA = evaluateSortKey(sortKey, a)
        val valB = evaluateSortKey(sortKey, b)
        val cmp = compareValues(valA, valB)
        if (sortKey.ascending) cmp else -cmp
      }
    }

  private def evaluateSortKey(sortKey: SortKey, ctx: QueryContext): Option[Value] = {
    // Pass QueryContext directly - no conversion needed since EvalEnvironment now uses Pattern.Value
    val env = EvalEnvironment(ctx, params)
    eval(sortKey.expression).run(env).toOption
  }

  private def compareValues(a: Option[Value], b: Option[Value]): Int =
    (a, b) match {
      case (None, None) => 0
      case (None, _) => 1 // Nulls last
      case (_, None) => -1
      case (Some(Value.Integer(x)), Some(Value.Integer(y))) => x.compare(y)
      case (Some(Value.Real(x)), Some(Value.Real(y))) => x.compare(y)
      case (Some(Value.Text(x)), Some(Value.Text(y))) => x.compare(y)
      case (Some(Value.True), Some(Value.False)) => 1
      case (Some(Value.False), Some(Value.True)) => -1
      case _ => 0 // Incomparable types
    }
}

class LimitState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val count: Long,
  val inputId: StandingQueryId,
) extends QueryState
    with PublishingState {

  // Track how many results we've emitted
  private var emittedCount: Long = 0

  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    if (emittedCount >= count) {
      // Already at limit - ignore further results
      return
    }

    val outputDelta = mutable.Map.empty[QueryContext, Int]
    var remaining = count - emittedCount

    // Process assertions first (positive multiplicities)
    delta.toList.sortBy(-_._2).foreach { case (ctx, mult) =>
      if (remaining > 0 && mult > 0) {
        val toEmit = math.min(mult.toLong, remaining).toInt
        if (toEmit > 0) {
          outputDelta(ctx) = outputDelta.getOrElse(ctx, 0) + toEmit
          remaining -= toEmit
          emittedCount += toEmit
        }
      }
    }

    // Emit if we have results
    val nonZero = outputDelta.filter(_._2 != 0).toMap
    // In Eager mode, emit even if empty - this signals "processed input, no output"
    if (nonZero.nonEmpty || mode == RuntimeMode.Eager) {
      emit(nonZero, actor)
    }
  }

  override def kickstart(context: NodeContext, actor: ActorRef): Unit = ()
}

class SubscribeToQueryPartState(
  val id: StandingQueryId,
  val publishTo: StandingQueryId,
  val mode: RuntimeMode,
  val queryPartId: QueryPartId,
  val projection: Map[Symbol, Symbol],
) extends QueryState
    with PublishingState {
  override def notify(delta: Delta.T, from: StandingQueryId, actor: ActorRef): Unit = {
    // TODO: Receive deltas from subscribed query part
  }
  override def kickstart(context: NodeContext, actor: ActorRef): Unit = {
    // TODO: Subscribe to query part, receive initial snapshot
  }
}
