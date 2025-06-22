package com.thatdot.model.v2.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

/** The steps that are executed to ultimately write a result to a destination
  *
  * Sub abstractions are:
  * - For foldable destination steps [[FoldableDestinationSteps]]
  * - For non-foldable destination steps [[NonFoldableDestinationSteps]]
  */
sealed trait DestinationSteps {
  // TODO def post-enrichment transform
  // def transform: Option[Core.PostEnrichmentTransform]
  def destination: ResultDestination
}

sealed trait FoldableDestinationSteps extends DestinationSteps with DataFoldableSink {
  def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[In, NotUsed]
}

sealed trait NonFoldableDestinationSteps extends DestinationSteps with DataNonFoldableSink

object NonFoldableDestinationSteps {

  case class WithRawBytes(
    destination: ResultDestination.Bytes,
  ) extends NonFoldableDestinationSteps {
    def sink[In: BytesOutputEncoder](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId).contramap[In](implicitly[BytesOutputEncoder[In]].bytes)
  }
}

object FoldableDestinationSteps {
  case class WithByteEncoding(
    formatAndEncode: OutputEncoder,
    destination: ResultDestination.Bytes,
  ) extends FoldableDestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] = {
      val inToRepr = DataFoldableFrom[In].to(formatAndEncode.folderTo, formatAndEncode.reprTag)
      val inToBytes = inToRepr.andThen(formatAndEncode.bytes)
      Flow.fromFunction(inToBytes).to(destination.sink(outputName, namespaceId))
    }
  }

  case class WithDataFoldable(destination: ResultDestination.FoldableData) extends FoldableDestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId)
  }

  case class WithAny(destination: ResultDestination.AnyData) extends FoldableDestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId)
  }
}
