package com.thatdot.model.v2.outputs.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId

case object Drop extends ResultDestination.AnyData.Drop {
  override def slug: String = "drop"
  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Any, NotUsed] =
    Sink.ignore.mapMaterializedValue(_ => NotUsed).named(sinkName(name))
}
