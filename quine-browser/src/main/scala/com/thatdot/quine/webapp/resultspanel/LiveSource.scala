package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import io.circe.Json

/** The kind of a source — display metadata only, never a rendering branch. */
sealed abstract class SourceKind
object SourceKind {
  case object Query extends SourceKind
  case object Tap extends SourceKind
}

/** Display-only provenance for a source, e.g. `Provenance(Tap, "fraud/highRisk · post")`.
  * The viewer labels with this; it never branches rendering on it.
  */
final case class Provenance(kind: SourceKind, label: String)

/** Producer-reported lifecycle of a live source. Follow/Pause is a *view* concern and lives
  * in the panel's `LiveStream`, not here.
  */
sealed abstract class SourceStatus
object SourceStatus {
  case object Connecting extends SourceStatus
  case object Live extends SourceStatus
  case object Ended extends SourceStatus
  final case class Error(message: String) extends SourceStatus
}

/** A live, externally-owned source the results panel can view.
  *
  * A plain record of the values a producer hands the panel — not a behavioural abstraction (its
  * former "implementations" only ever wired up these same fields). Producers build one with the
  * factory that fits them ([[WiretapSources.adapt]], [[streaming.StubLiveSource]]).
  *
  * Produced and managed by a creator elsewhere — the Explore toolbox, the Dashboard diagram, the
  * Streams page — never by the panel. The panel only *reads* it: it does not create, configure,
  * start, or stop a source. This is the producer→panel seam in
  * `design-context-tap/taps-and-monitors.md` (Source vs Surface). The host hands the panel a
  * `Signal[Vector[LiveSource]]`; the panel selects among them by [[id]] and renders the selected
  * one, branching only on whether it is a snapshot or a stream.
  *
  * @param id         stable identity, for selection and de-duplication
  * @param provenance display-only provenance (kind + label)
  * @param status     producer-reported lifecycle
  * @param records    the live frame stream — one JSON per record, in the
  *                   [[streaming.StandingTapFrame]] shape; the producer owns the underlying
  *                   transport (e.g. a WebSocket), the panel only subscribes
  * @param stop       ask the producer to stop this source (e.g. close the tap on the server); the
  *                   panel still keeps the captured rows as an ended snapshot. No-op by default
  *                   for sources that can't be stopped (a query run, or a dev stub)
  * @param tapTarget  for a standing-query tap, the `(sqName, tapPoint)` it observes — so the panel
  *                   can close / restart it through the capability without parsing [[id]]; `None`
  *                   for non-taps or producers that don't report it
  */
final case class LiveSource(
  id: String,
  provenance: Provenance,
  status: Signal[SourceStatus],
  records: EventStream[Json],
  stop: () => Unit = () => (),
  tapTarget: Option[TapTarget] = None,
)
