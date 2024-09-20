package com.thatdot.quine.app.ingest

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.stream.connectors.sse.scaladsl.EventSource
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, ImportFormat}
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.SwitchMode

final case class ServerSentEventsSrcDef(
  override val name: String,
  override val intoNamespace: NamespaceId,
  url: String,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder],
)(implicit val graph: CypherOpsGraph, protected val logConfig: LogConfig)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      decoders,
      s"$name (SSE ingest)",
      intoNamespace,
    ) {

  type InputType = ServerSentEvent

  override val ingestToken: IngestSrcExecToken = IngestSrcExecToken(s"$name: $url")

  def source(): Source[ServerSentEvent, NotUsed] = EventSource(
    uri = Uri(url),
    send = Http().singleRequest(_),
  )

  def rawBytes(event: ServerSentEvent): Array[Byte] = event.data.getBytes

}
