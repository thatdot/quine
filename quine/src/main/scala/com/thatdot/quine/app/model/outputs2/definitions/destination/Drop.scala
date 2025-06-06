package com.thatdot.quine.app.model.outputs2.definitions.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.outputs2.definitions.ResultDestination
import com.thatdot.quine.graph.NamespaceId

case object Drop extends ResultDestination.AnyData.Drop {
  override def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Any, NotUsed] =
    Sink.ignore.mapMaterializedValue(_ => NotUsed)
}
