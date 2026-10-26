package com.thatdot.quine.app.model.outputs2

import com.thatdot.outputs2.{DataFoldableSink, SinkName}

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

    /** Streams each result to one of several independent Novelty instances for scoring, then writes
      * each score returned into the graph — a single bidirectional, backpressured flow.
      */
    trait NoveltyStream extends FoldableData {
      def ownerId: String
      def endpoints: List[String]
      def model: String
      def scoreEdgeLabel: String
      def parallelism: Int
      def shouldRetry: Boolean
    }
  }
}
