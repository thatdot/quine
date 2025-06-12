package com.thatdot.quine.app.model.outputs2

import com.thatdot.model.v2.outputs.{DataFoldableSink, SinkName}

sealed trait QuineResultDestination extends DataFoldableSink with SinkName

object QuineResultDestination {
  sealed trait FoldableData extends QuineResultDestination

  object FoldableData {
    trait Slack extends FoldableData {
      def hookUrl: String
      def onlyPositiveMatchData: Boolean
      def intervalSeconds: Int
    }
    trait CypherQuery extends FoldableData {
      def queryText: String
      def parameter: String
      def parallelism: Int
      def allowAllNodeScan: Boolean
      def shouldRetry: Boolean
    }
  }
}
