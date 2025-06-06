package com.thatdot.quine.app.v2api.converters

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.actor.ActorSystem

import com.thatdot.quine.app.model.outputs2.{definitions => Output}
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.app.v2api.definitions.{outputs2 => Api}
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.util.StringInput

/** Conversions from internal models in [[com.thatdot.quine.app.model.outputs2.definitions]]
  * to API models in [[com.thatdot.quine.app.v2api.definitions.outputs2]].
  */
object ApiToOutput {

  def apply(
    format: Api.OutputFormat,
  )(implicit protobufSchemaCache: ProtobufSchemaCache, ec: ExecutionContext): Future[Output.OutputEncoder] =
    format match {
      case Api.OutputFormat.JSON => Future.successful(Output.OutputEncoder.JSON())
      case Api.OutputFormat.Protobuf(schemaUrl, typeName) =>
        protobufSchemaCache
          .getMessageDescriptor(StringInput.filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
          .map(desc => Output.OutputEncoder.Protobuf(schemaUrl, typeName, desc))
    }

  private def apply(logLevel: Api.DestinationSteps.StandardOut.LogLevel): Output.destination.StandardOut.LogLevel =
    logLevel match {
      case Api.DestinationSteps.StandardOut.LogLevel.Trace => Output.destination.StandardOut.LogLevel.Trace
      case Api.DestinationSteps.StandardOut.LogLevel.Debug => Output.destination.StandardOut.LogLevel.Debug
      case Api.DestinationSteps.StandardOut.LogLevel.Info => Output.destination.StandardOut.LogLevel.Info
      case Api.DestinationSteps.StandardOut.LogLevel.Warn => Output.destination.StandardOut.LogLevel.Warn
      case Api.DestinationSteps.StandardOut.LogLevel.Error => Output.destination.StandardOut.LogLevel.Error
    }

  private def apply(logMode: Api.DestinationSteps.StandardOut.LogMode): Output.destination.StandardOut.LogMode =
    logMode match {
      case Api.DestinationSteps.StandardOut.LogMode.Complete => Output.destination.StandardOut.LogMode.Complete
      case Api.DestinationSteps.StandardOut.LogMode.FastSampling => Output.destination.StandardOut.LogMode.FastSampling
    }

  def apply(
    destinationSteps: Api.DestinationSteps,
  )(implicit
    graph: BaseGraph,
    ec: ExecutionContext,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[Output.DestinationSteps] = {
    implicit val system: ActorSystem = graph.system

    destinationSteps match {
      case Api.DestinationSteps.Drop =>
        Future.successful(
          Output.DestinationSteps.WithAny(
            destination = Output.destination.Drop,
          ),
        )
      case Api.DestinationSteps.File(path, format) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.File(
              path = path,
            ),
          ),
        )
      case Api.DestinationSteps.HttpEndpoint(url, parallelism) =>
        Future.successful(
          Output.DestinationSteps.WithGenericFoldable(
            destination = Output.destination.HttpEndpoint(
              url = url,
              parallelism = parallelism,
            ),
          ),
        )
      case Api.DestinationSteps.Kafka(topic, bootstrapServers, format, kafkaProperties) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.Kafka(
              topic = topic,
              bootstrapServers = bootstrapServers,
              kafkaProperties = kafkaProperties,
            ),
          ),
        )
      case Api.DestinationSteps.Kinesis(
            credentials,
            region,
            streamName,
            format,
            kinesisParallelism,
            kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond,
          ) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.Kinesis(
              credentials = credentials.map(ApiToInternal.apply),
              region = region.map(ApiToInternal.apply),
              streamName = streamName,
              kinesisParallelism = kinesisParallelism,
              kinesisMaxBatchSize = kinesisMaxBatchSize,
              kinesisMaxRecordsPerSecond = kinesisMaxRecordsPerSecond,
              kinesisMaxBytesPerSecond = kinesisMaxBytesPerSecond,
            ),
          ),
        )
      case Api.DestinationSteps.ReactiveStream(address, port, format) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.ReactiveStream(
              address = address,
              port = port,
            ),
          ),
        )
      case Api.DestinationSteps.Slack(hookUrl, onlyPositiveMatchData, intervalSeconds) =>
        Future.successful(
          Output.DestinationSteps.WithGenericFoldable(
            destination = Output.destination.Slack(
              hookUrl = hookUrl,
              onlyPositiveMatchData = onlyPositiveMatchData,
              intervalSeconds = intervalSeconds,
            ),
          ),
        )
      case Api.DestinationSteps.SNS(credentials, region, topic, format) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.SNS(
              credentials = credentials.map(ApiToInternal.apply),
              region = region.map(ApiToInternal.apply),
              topic = topic,
            ),
          ),
        )
      case Api.DestinationSteps.StandardOut(logLevel, logMode, format) =>
        apply(format).map(enc =>
          Output.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = Output.destination.StandardOut(
              logLevel = apply(logLevel),
              logMode = apply(logMode),
            ),
          ),
        )
    }
  }

}
