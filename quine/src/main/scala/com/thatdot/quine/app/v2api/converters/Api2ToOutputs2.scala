package com.thatdot.quine.app.v2api.converters

import scala.concurrent.Future
import scala.util.Try

import org.apache.pekko.actor.ActorSystem

import com.thatdot.api.v2.{outputs => CoreApiOutputs}
import com.thatdot.outputs2.DataFoldableSink
import com.thatdot.outputs2.kafka.KafkaExtensionProvider
import com.thatdot.quine.app.model.outputs2.destination.{NoveltyDestinationSupport, NoveltyStreamSettings}
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
        intervalSeconds = s.interval.toSeconds.toInt,
      ),
    )

  private def noveltyStreamSettings(
    n: ApiOutput.QuineDestinationSteps.Novelty,
    ownerId: String,
  ): NoveltyStreamSettings =
    NoveltyStreamSettings(
      ownerId = ownerId,
      endpoints = n.endpoints,
      model = n.model,
      scoreEdgeLabel = n.scoreEdgeLabel,
      parallelism = n.parallelism,
      shouldRetry = n.shouldRetry,
      auth = n.auth,
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
  )(implicit
    graph: CypherOpsGraph,
    protobufSchemaCache: ProtobufSchemaCache,
    kafkaExtensions: KafkaExtensionProvider[com.thatdot.api.v2.SaslJaasConfig],
    noveltyDestinationSupport: NoveltyDestinationSupport,
  ): Future[DataFoldableSink] = steps match {
    case x: ApiOutput.QuineDestinationSteps.CypherQuery =>
      Future.successful(apply(x))
    case x: ApiOutput.QuineDestinationSteps.Slack =>
      Future.successful(apply(x)(graph.system))
    case x: ApiOutput.QuineDestinationSteps.Novelty =>
      // Enterprise-only: the destination is supplied by the capability, so the OSS build has nothing
      // to construct even on a path that bypasses QuineApiMethods.validateWorkflow (which gives the
      // clean 400).
      noveltyDestinationSupport match {
        case NoveltyDestinationSupport.Available(build) =>
          // The owner id is assigned once, where the output is created, so that every host builds this
          // destination with the same id; an output without one never went through that path.
          x.ownerId match {
            case Some(ownerId) =>
              Future.fromTry(
                Try(OutputModels.QuineDestinationSteps.WithDataFoldable(build(noveltyStreamSettings(x, ownerId)))),
              )
            case None =>
              Future.failed(
                new IllegalArgumentException(
                  "This Novelty output has no owner id; Novelty outputs must be created through the v2 API.",
                ),
              )
          }
        case NoveltyDestinationSupport.Unavailable =>
          Future.failed(new NoveltyDestinationSupport.UnavailableException)
      }
    case x: ApiOutput.QuineDestinationSteps with MirrorOfCore =>
      CoreConvert.Api2ToOutputs2.apply(quineDestinationStepsToCoreDestinationSteps(x))(
        graph = graph,
        ec = graph.dispatchers.nodeDispatcherEC,
        protobufSchemaCache = protobufSchemaCache,
        kafkaExtensions = kafkaExtensions,
      )
  }

  def apply(
    filter: ApiStanding.Predicate,
  ): OutputModels.query.standing.Predicate = filter match {
    case ApiStanding.Predicate.OnlyPositiveMatch => OutputModels.query.standing.Predicate.OnlyPositiveMatch
  }
}
