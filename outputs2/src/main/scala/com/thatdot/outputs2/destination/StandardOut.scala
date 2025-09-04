package com.thatdot.outputs2.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.outputs2.ResultDestination
import com.thatdot.quine.graph.NamespaceId

case object StandardOut extends ResultDestination.Bytes.StandardOut {
  override def slug: String = "standard-out"
  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed] =
    Sink
      .foreach[Array[Byte]](System.out.write)
      .mapMaterializedValue(_ => NotUsed)
      .named(sinkName(name))
}
