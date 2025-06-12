package com.thatdot.quine.app.v2api.converters

import scala.concurrent.Future

import org.apache.pekko.actor.ActorSystem

import com.thatdot.quine.app.model.{outputs2 => OutputModels}
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.definitions.{outputs => ApiOutput}
import com.thatdot.quine.graph.CypherOpsGraph

object Api2ToOutputs2 {
  def toEnrichmentQuery(q: ApiOutput.QuineDestinationSteps.CypherQuery): OutputModels.query.CypherQuery =
    OutputModels.query.CypherQuery(
      queryText = q.query,
      parameter = q.parameter,
      parallelism = q.parallelism,
      allowAllNodeScan = q.allowAllNodeScan,
      shouldRetry = q.shouldRetry,
    )

  private def apply(
    q: ApiOutput.QuineDestinationSteps.CypherQuery,
  )(implicit graph: CypherOpsGraph): OutputModels.QuineDestinationSteps =
    OutputModels.QuineDestinationSteps.WithDataFoldable(
      OutputModels.destination.CypherQueryDestination(
        queryText = q.query,
        parameter = q.parameter,
        parallelism = q.parallelism,
        allowAllNodeScan = q.allowAllNodeScan,
        shouldRetry = q.shouldRetry,
      ),
    )

  private def apply(
    s: ApiOutput.QuineDestinationSteps.Slack,
  )(implicit system: ActorSystem): OutputModels.QuineDestinationSteps =
    OutputModels.QuineDestinationSteps.WithDataFoldable(
      OutputModels.destination.Slack(
        hookUrl = s.hookUrl,
        onlyPositiveMatchData = s.onlyPositiveMatchData,
        intervalSeconds = s.intervalSeconds,
      ),
    )

  def apply(
    steps: ApiOutput.QuineDestinationSteps,
  )(implicit graph: CypherOpsGraph): Future[OutputModels.QuineDestinationSteps] = steps match {
    case q: ApiOutput.QuineDestinationSteps.CypherQuery => Future.successful(apply(q))
    case s: ApiOutput.QuineDestinationSteps.Slack => Future.successful(apply(s)(graph.system))
  }

  def apply(
    filter: ApiStanding.Predicate,
  ): OutputModels.query.standing.Predicate = filter match {
    case ApiStanding.Predicate.OnlyPositiveMatch => OutputModels.query.standing.Predicate.OnlyPositiveMatch
  }
}
