package com.thatdot.quine.app.model.outputs2

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.outputs2.DataFoldableSink
import com.thatdot.quine.graph.NamespaceId

sealed trait QuineDestinationSteps extends DataFoldableSink {
  // def transform: Option[Core.PostEnrichmentTransform]

  def destination: QuineResultDestination
}

object QuineDestinationSteps {
  case class WithDataFoldable(destination: QuineResultDestination.FoldableData) extends QuineDestinationSteps {
    override def sink[In: DataFoldableFrom](outputName: String, namespaceId: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[In, NotUsed] =
      destination.sink(outputName, namespaceId)
  }
}
