package com.thatdot.quine.app.model.outputs

import java.nio.ByteBuffer

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.util.ByteString

import io.circe.Json
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.{CloseableChannel, TcpServerTransport}
import io.rsocket.util.DefaultPayload
import io.rsocket.{Payload, SocketAcceptor}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import reactor.core.publisher.Flux

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig}
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.ReactiveStream

object ReactiveStreamOutput {
  def makeServer(address: String, port: Int): Sink[ByteBuffer, CloseableChannel] = {
    val sink: Sink[Payload, CloseableChannel] =
      Sink.asPublisher(true).mapMaterializedValue { pub: Publisher[Payload] =>
        val server = RSocketServer
          .create(SocketAcceptor.forRequestStream(_ => Flux.from(pub)))
          .bindNow(TcpServerTransport.create(address, port))
        pub.subscribe(new Subscriber[Payload] {
          override def onComplete(): Unit =
            if (!server.isDisposed()) server.dispose()
          override def onError(t: Throwable): Unit =
            if (!server.isDisposed()) server.dispose()
          override def onNext(t: Payload): Unit = ()
          override def onSubscribe(s: Subscription): Unit = s.request(Long.MaxValue)
        })
        server
      }
    Flow[ByteBuffer]
      .map(DefaultPayload.create)
      .toMat(sink)(Keep.right)
  }
}

class ReactiveStreamOutput(val streamConfig: ReactiveStream)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {

  //todo: Support different formats
  override def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)
    Flow[StandingQueryResult]
      .alsoToMat(
        Flow[StandingQueryResult]
          .map { result =>
            val json = Json.fromFields(result.data.view.mapValues(QuineValue.toJson(_)(graph.idProvider, logConfig)))
            ByteString(json.noSpaces).asByteBuffer
          }
          .to(ReactiveStreamOutput.makeServer(streamConfig.address, streamConfig.port)),
      )(Keep.left)
      .map(_ => token)
  }
}
