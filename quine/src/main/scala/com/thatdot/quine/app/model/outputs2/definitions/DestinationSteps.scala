package com.thatdot.quine.app.model.outputs2.definitions

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.ingest2.core.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

sealed trait DestinationSteps {
  def destination: ResultDestination
  def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[In, NotUsed]
}

object DestinationSteps {
  case class WithByteEncoding(
    formatAndEncode: OutputEncoder,
    destination: ResultDestination.Bytes,
  ) extends DestinationSteps {
    def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] = {
      val inToRepr = DataFoldableFrom[In].to(formatAndEncode.folderTo, formatAndEncode.reprTag)
      val inToBytes = inToRepr.andThen(formatAndEncode.bytes)
      Flow.fromFunction(inToBytes).to(destination.sink(outputName, namespaceId))
    }
  }

  case class WithGenericFoldable(destination: ResultDestination.FoldableData) extends DestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId)
  }

  case class WithAny(destination: ResultDestination.AnyData) extends DestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId)
  }
}
