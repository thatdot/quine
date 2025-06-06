package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.outputs2.definitions.DestinationSteps
import com.thatdot.quine.app.model.outputs2.definitions.ResultDestination.{AnyData, Bytes, FoldableData}
import com.thatdot.quine.app.model.outputs2.{definitions => Output}
import com.thatdot.quine.app.v2api.definitions.{outputs2 => Api}

/** Conversions from API models in [[com.thatdot.quine.app.v2api.definitions.outputs2]]
  * to internal models in [[com.thatdot.quine.app.model.outputs2.definitions]].
  */
object OutputToApi {

  private def apply(format: Output.OutputEncoder): Api.OutputFormat = format match {
    case Output.OutputEncoder.JSON(_) => Api.OutputFormat.JSON
    case Output.OutputEncoder.Protobuf(schemaUrl, typeName, _) => Api.OutputFormat.Protobuf(schemaUrl, typeName)
  }

  private def apply(logLevel: Output.destination.StandardOut.LogLevel): Api.DestinationSteps.StandardOut.LogLevel =
    logLevel match {
      case Output.destination.StandardOut.LogLevel.Trace => Api.DestinationSteps.StandardOut.LogLevel.Trace
      case Output.destination.StandardOut.LogLevel.Debug => Api.DestinationSteps.StandardOut.LogLevel.Debug
      case Output.destination.StandardOut.LogLevel.Info => Api.DestinationSteps.StandardOut.LogLevel.Info
      case Output.destination.StandardOut.LogLevel.Warn => Api.DestinationSteps.StandardOut.LogLevel.Warn
      case Output.destination.StandardOut.LogLevel.Error => Api.DestinationSteps.StandardOut.LogLevel.Error
    }

  private def apply(logMode: Output.destination.StandardOut.LogMode): Api.DestinationSteps.StandardOut.LogMode =
    logMode match {
      case Output.destination.StandardOut.LogMode.Complete => Api.DestinationSteps.StandardOut.LogMode.Complete
      case Output.destination.StandardOut.LogMode.FastSampling => Api.DestinationSteps.StandardOut.LogMode.FastSampling
    }

  def apply(destinationSteps: Output.DestinationSteps): Api.DestinationSteps = destinationSteps match {
    case Output.DestinationSteps.WithByteEncoding(formatAndEncode, destination) =>
      val format = apply(formatAndEncode)
      destination match {
        case slack: FoldableData.Slack =>
          Api.DestinationSteps.Slack(
            hookUrl = slack.hookUrl,
            onlyPositiveMatchData = slack.onlyPositiveMatchData,
            intervalSeconds = slack.intervalSeconds,
          )
        case endpoint: FoldableData.HttpEndpoint =>
          Api.DestinationSteps.HttpEndpoint(
            url = endpoint.url,
            parallelism = endpoint.parallelism,
          )
        case stream: Bytes.ReactiveStream =>
          Api.DestinationSteps.ReactiveStream(
            address = stream.address,
            port = stream.port,
            format = format,
          )
        case out: Bytes.StandardOut =>
          Api.DestinationSteps.StandardOut(
            logLevel = apply(out.logLevel),
            logMode = apply(out.logMode),
            format = format,
          )
        case sns: Bytes.SNS =>
          Api.DestinationSteps.SNS(
            credentials = sns.credentials.map(InternalToApi.apply),
            region = sns.region.map(InternalToApi.apply),
            topic = sns.topic,
            format = format,
          )
        case kafka: Bytes.Kafka =>
          Api.DestinationSteps.Kafka(
            topic = kafka.topic,
            bootstrapServers = kafka.bootstrapServers,
            format = format,
            kafkaProperties = kafka.kafkaProperties,
          )
        case kinesis: Bytes.Kinesis =>
          Api.DestinationSteps.Kinesis(
            credentials = kinesis.credentials.map(InternalToApi.apply),
            region = kinesis.region.map(InternalToApi.apply),
            streamName = kinesis.streamName,
            format = format,
            kinesisParallelism = kinesis.kinesisParallelism,
            kinesisMaxBatchSize = kinesis.kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond = kinesis.kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond = kinesis.kinesisMaxBytesPerSecond,
          )
        case file: Bytes.File =>
          Api.DestinationSteps.File(
            path = file.path,
            format = format,
          )
      }
    case DestinationSteps.WithGenericFoldable(destination) =>
      destination match {
        case endpoint: FoldableData.HttpEndpoint =>
          Api.DestinationSteps.HttpEndpoint(
            endpoint.url,
            endpoint.parallelism,
          )
        case slack: FoldableData.Slack =>
          Api.DestinationSteps.Slack(
            slack.hookUrl,
            slack.onlyPositiveMatchData,
            slack.intervalSeconds,
          )
      }
    case DestinationSteps.WithAny(destination) =>
      destination match {
        case _: AnyData.Drop => Api.DestinationSteps.Drop
      }
  }

}
