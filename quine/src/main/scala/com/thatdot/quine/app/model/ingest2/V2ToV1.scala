package com.thatdot.quine.app.model.ingest2

import com.thatdot.quine.app.model.ingest2.{V2IngestEntities => V2}
import com.thatdot.quine.{routes => V1}

object V2ToV1 {
  def apply(status: V2.IngestStreamStatus): V1.IngestStreamStatus = status match {
    case V2.IngestStreamStatus.Running => V1.IngestStreamStatus.Running
    case V2.IngestStreamStatus.Paused => V1.IngestStreamStatus.Paused
    case V2.IngestStreamStatus.Restored => V1.IngestStreamStatus.Restored
    case V2.IngestStreamStatus.Completed => V1.IngestStreamStatus.Completed
    case V2.IngestStreamStatus.Terminated => V1.IngestStreamStatus.Terminated
    case V2.IngestStreamStatus.Failed => V1.IngestStreamStatus.Failed
  }
}
