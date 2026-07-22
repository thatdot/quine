package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._

/** Fires once per completed query, as the trigger for [[ResultCountIndicator]]'s fade-in. The
  * indicator's displayed numbers come from the live canvas node/edge count signals (the same
  * ones [[Counters]] reads), not from this event's payload — see [[ResultCountIndicator]]'s doc.
  * Kept as a distinct event (rather than deriving "a query just completed" from the count signals
  * themselves) because those signals go through an intermediate `None` at the start of every
  * submit; the event marks the specific moment — completion — the indicator should animate on.
  */
final case class ResultCountEvent(resultCount: Int, nodeCount: Option[Int])

/** Ephemeral, non-interactive canvas overlay: on query completion, the node/edge counters — the
  * same counter visual [[Counters]] renders (same ionicons, same "icon + subscript count"
  * structure) — fade in over the canvas, hold, then fade out. Design doc §5 — explicitly **not**
  * a toast: same layer/position/fade family as the existing query-progress `.loader`. Errors
  * never appear here (they surface as an error-accented card elsewhere).
  *
  * This replaces the counters' old home permanently docked in the top bar (removed from
  * [[TopBar]]): the counters now only appear as this transient overlay, recognizable as *the*
  * node/edge counters because it reuses [[Counters.nodeEdgeCounters]] verbatim rather than a
  * differently-shaped result-count string.
  *
  * Host-agnostic: takes the completion trigger and the live counts as `Signal`s/`EventStream`s
  * and mounts wherever the caller places it (over the canvas, alongside the existing `Loader`).
  *
  * @param completions one event per completed query — the trigger that starts the fade-in.
  *   Payload unused for content (see [[ResultCountEvent]]'s doc); only its arrival matters.
  * @param nodeCount live canvas node count, e.g. `QueryUi`'s `stateVar.foundNodesCount` — the same
  *   signal `Counters` was fed in the top bar.
  * @param edgeCount live canvas edge count, the `Counters` edge-side counterpart of `nodeCount`.
  * @param fadeMs total lifetime in the DOM: fade-in + hold + fade-out, split evenly across the
  *   three phases (`CardDefaults.IndicatorFadeMs` ~4000ms per the design doc's §7 table — passed
  *   in rather than imported so this file stays a leaf with no dependency on the card defaults
  *   object)
  */
object ResultCountIndicator {

  private val FadeInMs = 250.0
  private val FadeOutMs = 400.0

  def apply(
    completions: EventStream[ResultCountEvent],
    nodeCount: Signal[Option[Int]],
    edgeCount: Signal[Option[Int]],
    fadeMs: Int = 4000,
  ): HtmlElement = {
    val visibleVar = Var(false)
    var pendingTimers: List[js.timers.SetTimeoutHandle] = Nil

    def clearTimers(): Unit = {
      pendingTimers.foreach(js.timers.clearTimeout)
      pendingTimers = Nil
    }

    def schedule(delayMs: Double)(action: () => Unit): Unit =
      pendingTimers = js.timers.setTimeout(delayMs)(action()) :: pendingTimers

    def show(): Unit = {
      clearTimers()
      // Two rAF-free ticks: flip the visibility class on the next microtask so the CSS
      // transition actually animates in (mirrors Toast's re-trigger-safe pattern — cancel any
      // in-flight timers before restarting) even when two completions land back to back.
      visibleVar.set(false)
      schedule(0) { () =>
        visibleVar.set(true)
        val holdMs = math.max(0.0, fadeMs.toDouble - FadeInMs - FadeOutMs)
        schedule(FadeInMs + holdMs) { () =>
          visibleVar.set(false)
        }
      }
    }

    div(
      cls := JunkDrawerStyles.resultCountIndicator,
      cls(JunkDrawerStyles.resultCountIndicatorVisible) <-- visibleVar.signal,
      // Non-interactive: never intercepts canvas mouse/click events.
      pointerEvents := "none",
      // Only fade in when there is something to display: text-path completions never
      // repopulate the canvas node/edge counts (both stay None from submit-start), and
      // both counters render `display:none` when empty — showing then would fade in a
      // bare pill shell over the canvas.
      completions.withCurrentValueOf(nodeCount, edgeCount) --> { case (_, nodes, edges) =>
        if (nodes.isDefined || edges.isDefined) show()
      },
      onUnmountCallback(_ => clearTimers()),
      // Rebuilds the counters element whenever the live counts change, so the indicator always
      // shows the canvas's current totals the next time it fades in (nodeCount/edgeCount go
      // through an intermediate None at submit-start and repopulate on completion — the same
      // signals Counters read while it lived in the top bar).
      child <-- nodeCount.combineWith(edgeCount).map { case (n, e) =>
        Counters.nodeEdgeCounters(
          nodeCount = n,
          edgeCount = e,
          wrapClass = JunkDrawerStyles.resultCountIndicatorCounters,
          iconClass = JunkDrawerStyles.resultCountIndicatorIcon,
        )
      },
    )
  }
}
