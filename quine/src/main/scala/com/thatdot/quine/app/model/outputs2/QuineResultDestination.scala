package com.thatdot.quine.app.model.outputs2

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Sink

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.graph.NamespaceId

sealed trait QuineResultDestination

object QuineResultDestination {
  sealed trait FoldableData {
    def sink[A: DataFoldableFrom](name: String, inNamespace: NamespaceId)(implicit
      logConfig: LogConfig,
    ): Sink[A, NotUsed]
  }

  object FoldableData {
    trait Slack extends FoldableData {
      def hookUrl: String
      def onlyPositiveMatchData: Boolean
      def intervalSeconds: Int
    }
  }
}
