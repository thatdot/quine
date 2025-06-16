package com.thatdot.model.v2.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

sealed trait DestinationSteps extends DataFoldableSink {
  // TODO def post-enrichment transform
  // def transform: Option[Core.PostEnrichmentTransform]
  def destination: ResultDestination
}

object DestinationSteps {
  case class WithByteEncoding(
    formatAndEncode: OutputEncoder,
    destination: ResultDestination.Bytes,
  ) extends DestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] = {
      val inToRepr = DataFoldableFrom[In].to(formatAndEncode.folderTo, formatAndEncode.reprTag)
      val inToBytes = inToRepr.andThen(formatAndEncode.bytes)
      Flow.fromFunction(inToBytes).to(destination.sink(outputName, namespaceId))
    }
  }

  case class WithDataFoldable(destination: ResultDestination.FoldableData) extends DestinationSteps {
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
