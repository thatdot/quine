package com.thatdot.outputs2.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Tcp}
import org.apache.pekko.util.ByteString

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.outputs2.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class ReactiveStream(
  address: String = "localhost",
  port: Int,
)(implicit system: ActorSystem)
    extends ResultDestination.Bytes.ReactiveStream {
  override def slug: String = "reactive-stream"

  override def sink(name: String, inNamespace: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[Array[Byte], NotUsed] = {

    // Convert Array[Byte] to length-prefixed ByteString for framing
    val lengthFieldFraming = Flow[Array[Byte]].map { bytes =>
      val data = ByteString(bytes)
      val length = ByteString.fromArray(java.nio.ByteBuffer.allocate(4).putInt(data.length).array())
      length ++ data
    }

    // BroadcastHub with a dummy sink attached to prevent blocking when no consumers
    // When TCP consumers connect, BroadcastHub backpressures to the slowest one
    Flow[Array[Byte]]
      .via(lengthFieldFraming)
      .toMat(
        Sink.fromGraph(
          BroadcastHub.sink[ByteString](bufferSize = 256),
        ),
      )(Keep.right)
      .mapMaterializedValue { broadcastSource =>
        // Attach a dummy sink that drops all messages - prevents backpressure when no TCP clients
        broadcastSource.runWith(Sink.ignore)

        // Bind TCP server that connects each client to the broadcast source
        Tcp()
          .bind(address, port)
          .to(Sink.foreach { connection: Tcp.IncomingConnection =>
            // Each client gets data from BroadcastHub
            // Silences the non-Unit value of type org.apache.pekko.NotUsed
            val _ = broadcastSource
              .via(connection.flow)
              .to(Sink.ignore)
              .run()
          })
          .run()
        NotUsed
      }
      .named(sinkName(name))
  }
}
