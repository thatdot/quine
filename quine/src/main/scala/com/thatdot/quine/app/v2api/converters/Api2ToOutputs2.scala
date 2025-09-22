package com.thatdot.quine.app.v2api.converters

import scala.concurrent.Future

import org.apache.pekko.actor.ActorSystem

import com.thatdot.api.v2.{outputs => CoreApiOutputs}
import com.thatdot.outputs2.DataFoldableSink
import com.thatdot.quine.app.model.{outputs2 => OutputModels}
import com.thatdot.quine.app.v2api.definitions.outputs.MirrorOfCore
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.definitions.{outputs => ApiOutput}
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.{convert => CoreConvert}

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

  /** Convenience method for converting [[ApiOutput.QuineDestinationSteps]] to
    * [[CoreApiOutputs.DestinationSteps]] when there exists an "equivalent" core type
    * that the local API type is meant to "mirror".
    */
  protected[converters] def quineDestinationStepsToCoreDestinationSteps(
    steps: ApiOutput.QuineDestinationSteps with MirrorOfCore,
  ): CoreApiOutputs.DestinationSteps = steps match {
    //// Objects ////
    case ApiOutput.QuineDestinationSteps.Drop =>
      CoreApiOutputs.DestinationSteps.Drop()
    case ApiOutput.QuineDestinationSteps.StandardOut =>
      CoreApiOutputs.DestinationSteps.StandardOut()
    //// Core Single-Parameter Classes ////
    case ApiOutput.QuineDestinationSteps.File(path) =>
      CoreApiOutputs.DestinationSteps.File(path)
    //// Core Multi-Parameter Classes ////
    // Note that the `.get` calls are guaranteed //
    case o: ApiOutput.QuineDestinationSteps.HttpEndpoint =>
      val args = ApiOutput.QuineDestinationSteps.HttpEndpoint.unapply(o).get
      (CoreApiOutputs.DestinationSteps.HttpEndpoint.apply _).tupled(args)
    case o: ApiOutput.QuineDestinationSteps.Kafka =>
      val args = ApiOutput.QuineDestinationSteps.Kafka.unapply(o).get
      (CoreApiOutputs.DestinationSteps.Kafka.apply _).tupled(args)
    case o: ApiOutput.QuineDestinationSteps.Kinesis =>
      val args = ApiOutput.QuineDestinationSteps.Kinesis.unapply(o).get
      (CoreApiOutputs.DestinationSteps.Kinesis.apply _).tupled(args)
    case o: ApiOutput.QuineDestinationSteps.ReactiveStream =>
      val args = ApiOutput.QuineDestinationSteps.ReactiveStream.unapply(o).get
      (CoreApiOutputs.DestinationSteps.ReactiveStream.apply _).tupled(args)
    case o: ApiOutput.QuineDestinationSteps.SNS =>
      val args = ApiOutput.QuineDestinationSteps.SNS.unapply(o).get
      (CoreApiOutputs.DestinationSteps.SNS.apply _).tupled(args)
  }

  def apply(
    steps: ApiOutput.QuineDestinationSteps,
  )(implicit graph: CypherOpsGraph, protobufSchemaCache: ProtobufSchemaCache): Future[DataFoldableSink] = steps match {
    case x: ApiOutput.QuineDestinationSteps.CypherQuery =>
      Future.successful(apply(x))
    case x: ApiOutput.QuineDestinationSteps.Slack =>
      Future.successful(apply(x)(graph.system))
    case x: ApiOutput.QuineDestinationSteps with MirrorOfCore =>
      CoreConvert.Api2ToOutputs2.apply(quineDestinationStepsToCoreDestinationSteps(x))(
        graph = graph,
        ec = graph.dispatchers.nodeDispatcherEC,
        protobufSchemaCache = protobufSchemaCache,
      )
  }

  def apply(
    filter: ApiStanding.Predicate,
  ): OutputModels.query.standing.Predicate = filter match {
    case ApiStanding.Predicate.OnlyPositiveMatch => OutputModels.query.standing.Predicate.OnlyPositiveMatch
  }
}
