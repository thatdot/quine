package com.thatdot.model.v2.outputs.destination

import java.nio.ByteBuffer

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import io.rsocket.{Payload, SocketAcceptor}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import reactor.core.publisher.Flux

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class ReactiveStream(
  address: String = "localhost",
  port: Int,
) extends ResultDestination.Bytes.ReactiveStream {
  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed] =
    Sink
      .asPublisher(fanout = false)
      .mapMaterializedValue { pub: Publisher[Payload] =>
        val server = RSocketServer
          .create(SocketAcceptor.forRequestStream(_ => Flux.from(pub)))
          .bindNow(TcpServerTransport.create(address, port))
        pub.subscribe(new Subscriber[Payload] {
          override def onComplete(): Unit = if (!server.isDisposed) server.dispose()
          override def onError(t: Throwable): Unit = if (!server.isDisposed) server.dispose()
          override def onNext(t: Payload): Unit = ()
          override def onSubscribe(s: Subscription): Unit = s.request(Long.MaxValue)
        })
        NotUsed
      }
      .contramap[Array[Byte]](bytes => DefaultPayload.create(ByteBuffer.wrap(bytes)))
}
