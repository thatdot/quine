package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

/** Adapts the wiretap lifecycle ([[WiretapStore]] / [[WiretapHandler]]) into the results
  * panel's source-agnostic [[LiveSource]] records, so server taps become selectable,
  * viewable — and stoppable — sources in the panel. The panel never sees
  * `WiretapStore`/`WiretapHandler`; it only reads the resulting `Signal[Vector[LiveSource]]`.
  */
object WiretapSources {

  /** The current set of viewable tap sources for a (possibly absent) per-namespace store.
    * One source per tap target (`sqName` + output): several handlers may share a target
    * (e.g. a monitor and a manage-taps view); they share a socket, so any one's `matches`
    * carries the stream.
    */
  def liveSources(store: Signal[Option[WiretapStore]]): Signal[Vector[LiveSource]] =
    store.flatMapSwitch {
      case Some(s) =>
        s.active.map { handlers =>
          handlers.values.toVector
            .groupBy(_.target)
            .values
            .map(_.minBy(_.key)) // deterministic handler per target
            .toVector
            .sortBy(_.target.key)
            .map(adapt)
        }
      case None => Signal.fromValue(Vector.empty)
    }

  // Sources are read-only to the panel: it closes a tap through the capability
  // (keyed by `tapTarget`), never via the source — so `stop` keeps its default no-op.
  private def adapt(h: WiretapHandler): LiveSource =
    LiveSource(
      id = h.target.key,
      provenance = Provenance(SourceKind.Tap, h.target.label),
      status = h.status.signal.map(toSourceStatus),
      records = h.matches,
      tapTarget = Some(h.target),
    )

  private def toSourceStatus(s: WiretapStatus): SourceStatus = s match {
    case WiretapStatus.Connecting => SourceStatus.Connecting
    case WiretapStatus.Live => SourceStatus.Live
    case WiretapStatus.Error(m) => SourceStatus.Error(m)
    case WiretapStatus.Closed => SourceStatus.Ended
  }
}
