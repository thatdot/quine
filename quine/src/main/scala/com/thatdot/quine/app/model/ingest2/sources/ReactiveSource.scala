package com.thatdot.quine.app.model.ingest2.sources

import org.apache.pekko.stream.scaladsl.Source

import cats.data.{Validated, ValidatedNel}
import io.rsocket.RSocket
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload
import reactor.util.retry.Retry

import com.thatdot.quine.app.RSocketKillSwitch
import com.thatdot.quine.app.model.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.util.BaseError

case class ReactiveSource(
  url: String,
  port: Int,
  meter: IngestMeter,
) extends FramedSourceProvider[Array[Byte]] {

  /** Attempt to build a framed source. Validation failures
    * are returned as part of the ValidatedNel failures.
    */
  override def framedSource: ValidatedNel[BaseError, FramedSource] = {
    val socket: RSocket =
      RSocketConnector
        .create()
        .setupPayload(DefaultPayload.create("", ""))
        .reconnect(Retry.indefinitely())
        .connect(TcpClientTransport.create(url, port))
        .retry()
        .block()

    val payload = DefaultPayload.create("")
    val s = socket
      .requestStream(payload)
      .map(_.getData.array())
    val source = Source
      .fromPublisher(s)
      .mapMaterializedValue(_ => RSocketKillSwitch(socket))
    val framedSource = FramedSource[Array[Byte]](
      source,
      meter,
      frame => frame,
    )
    Validated.valid(framedSource)
  }
}
