package com.thatdot.quine.webapp.resultspanel.streaming

import scala.scalajs.js.timers.{SetIntervalHandle, clearInterval, setInterval}

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.resultspanel.{LiveSource, Provenance, SourceKind, SourceStatus, TapTarget}

/** A synthetic [[LiveSource]] for developing the results panel before real producers (the
  * Explore toolbox, the Dashboard diagram, the Streams page) are wired in. Emits a fake
  * frame on a timer, with an occasional retraction; `stop()` halts emission. Not mounted
  * in production.
  */
object StubLiveSource {

  def apply(
    sourceId: String,
    label: String,
    target: Option[TapTarget] = None,
    periodMs: Double = 1200,
  ): LiveSource = {
    val bus = new EventBus[Json]
    var n = 0
    val timer: SetIntervalHandle = setInterval(periodMs) {
      n += 1
      val data = Json.obj(
        "id" -> Json.fromString(s"node-$n"),
        "label" -> Json.fromString(if (n % 2 == 0) "Person" else "Account"),
        "score" -> Json.fromInt((n * 13) % 100),
      )
      // Every 7th frame is a retraction (isPositiveMatch = false).
      bus.emit(
        Json.obj(
          "meta" -> Json.obj("isPositiveMatch" -> Json.fromBoolean(n % 7 != 0)),
          "data" -> data,
        ),
      )
    }
    LiveSource(
      id = sourceId,
      provenance = Provenance(SourceKind.Tap, label),
      status = Var[SourceStatus](SourceStatus.Live).signal,
      records = bus.events,
      stop = () => clearInterval(timer),
      tapTarget = target,
    )
  }
}
