package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.Signal

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackpressureSnapshot

/** Backpressure capability: the system's flow-control state. Read-only — no commands. */
trait BackpressureService {

  /** Latest backpressure snapshot (global valve, per-ingest stages, standing-query queues,
    * persistor latencies); `Pot` distinguishes loading and fetch failure from an empty system.
    */
  def backpressureSignal: Signal[Pot[V2BackpressureSnapshot]]
}
