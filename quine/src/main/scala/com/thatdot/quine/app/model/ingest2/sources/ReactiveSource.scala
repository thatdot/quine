package com.thatdot.quine.app.model.ingest2.sources

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Framing, Tcp}

import cats.data.{Validated, ValidatedNel}

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.model.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.util.BaseError

case class ReactiveSource(
  url: String,
  port: Int,
  meter: IngestMeter,
  maximumFrameLength: Int = 10 * 1024 * 1024, // 10MB default max frame size
)(implicit system: ActorSystem)
    extends FramedSourceProvider[Array[Byte]] {

  /** Attempt to build a framed source. Validation failures
    * are returned as part of the ValidatedNel failures.
    */
  override def framedSource: ValidatedNel[BaseError, FramedSource] = {
    import org.apache.pekko.stream.scaladsl.Source

    // Frame the byte stream using length-field framing (4-byte length prefix)
    val framing = Framing.lengthField(
      fieldLength = 4,
      fieldOffset = 0,
      maximumFrameLength = maximumFrameLength,
      byteOrder = java.nio.ByteOrder.BIG_ENDIAN,
    )

    val connection = Tcp().outgoingConnection(url, port)

    // Create a source that never emits anything but keeps the connection open
    // The server will push data to us
    // Using Source.empty would just terminate the connection immediately, while Source.maybe keeps the connection open
    // https://stackoverflow.com/questions/35398852/reading-tcp-as-client-via-akka-stream
    val source: Source[Array[Byte], NotUsed] = Source
      .maybe[org.apache.pekko.util.ByteString]
      .via(connection)
      .via(framing)
      .map(_.drop(4)) // Drop the 4-byte length prefix
      .via(metered(meter, bs => bs.length)) // Report metrics
      .map(_.toArray)
      .mapMaterializedValue(_ => NotUsed) // We never need to send data to the server

    val framedSource = FramedSource[Array[Byte]](
      withKillSwitches(source),
      meter,
      frame => frame,
      DataFoldableFrom.bytesDataFoldable,
    )
    Validated.valid(framedSource)
  }
}
