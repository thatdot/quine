package com.thatdot.model.v2.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

trait DataFoldableSink {
  def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[In, NotUsed]
}

trait DataNonFoldableSink {
  def sink[In: BytesOutputEncoder](outputName: String, namespaceId: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[In, NotUsed]
}

trait ByteArraySink {
  def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Array[Byte], NotUsed]
}

trait AnySink {
  def sink(name: String, inNamespace: NamespaceId)(implicit logConfig: LogConfig): Sink[Any, NotUsed]
}
