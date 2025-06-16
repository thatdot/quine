package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.{outputs2 => OutputModels}
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.definitions.{outputs => ApiOutput}

object Outputs2ToApi2 {
  def fromCypherQuery(q: OutputModels.query.CypherQuery): ApiOutput.QuineDestinationSteps.CypherQuery =
    ApiOutput.QuineDestinationSteps.CypherQuery(
      query = q.queryText,
      parameter = q.parameter,
      parallelism = q.parallelism,
      allowAllNodeScan = q.allowAllNodeScan,
      shouldRetry = q.shouldRetry,
    )

  def fromCypherQueryDestination(
    q: OutputModels.destination.CypherQueryDestination,
  ): ApiOutput.QuineDestinationSteps.CypherQuery =
    ApiOutput.QuineDestinationSteps.CypherQuery(
      query = q.queryText,
      parameter = q.parameter,
      parallelism = q.parallelism,
      allowAllNodeScan = q.allowAllNodeScan,
      shouldRetry = q.shouldRetry,
    )

  def apply(
    steps: OutputModels.QuineDestinationSteps,
  ): ApiOutput.QuineDestinationSteps = steps match {
    case OutputModels.QuineDestinationSteps.WithDataFoldable(destination) =>
      destination match {
        case slack: OutputModels.QuineResultDestination.FoldableData.Slack =>
          ApiOutput.QuineDestinationSteps.Slack(
            hookUrl = slack.hookUrl,
            onlyPositiveMatchData = slack.onlyPositiveMatchData,
            intervalSeconds = slack.intervalSeconds,
          )
        case query: OutputModels.QuineResultDestination.FoldableData.CypherQuery =>
          ApiOutput.QuineDestinationSteps.CypherQuery(
            query = query.queryText,
            parameter = query.parameter,
            parallelism = query.parallelism,
            allowAllNodeScan = query.allowAllNodeScan,
            shouldRetry = query.shouldRetry,
          )
      }
  }

  def apply(filter: OutputModels.query.standing.Predicate): ApiStanding.Predicate = filter match {
    case OutputModels.query.standing.Predicate.OnlyPositiveMatch => ApiStanding.Predicate.OnlyPositiveMatch
  }
}
