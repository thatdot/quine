package com.thatdot.quine.webapp

import com.raquo.airstream.eventbus.EventBus

/** App-wide reactive auth event streams. */
object AuthEvents {

  /** Fires whenever an HTTP 401 response is received.
    * Used by Quine Enterprise when auth is enabled; otherwise a no-op.
    */
  val unauthorized: EventBus[Unit] = new EventBus[Unit]
}
