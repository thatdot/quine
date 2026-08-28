package com.thatdot.quine.webapp.resultspanel

import com.thatdot.quine.webapp.dataservice.{BackgroundQueryTapHandler, BqTapStatus}

/** Adapts a background-query tap handler into the [[LiveSource]] the results surfaces consume.
  *
  * Shared rather than private to either consumer: the Explorer's result cards and the Streams
  * page's tap viewer both watch the same taps, and an id or status mapping that differed between
  * them would show the same run as two different things.
  */
object BackgroundQueryLiveSource {

  def apply(handler: BackgroundQueryTapHandler): LiveSource =
    LiveSource(
      id = s"bqtap:${handler.executionId}",
      provenance = Provenance(SourceKind.Tap, handler.displayName),
      status = handler.status.signal.map {
        case BqTapStatus.Connecting => SourceStatus.Connecting
        case BqTapStatus.Live => SourceStatus.Live
        case _: BqTapStatus.Ended => SourceStatus.Ended
        case BqTapStatus.Error(message) => SourceStatus.Error(message)
      },
      records = handler.rows,
      tapTarget = Some(TapTarget.BackgroundQuery(handler.executionId, handler.displayName)),
    )
}
