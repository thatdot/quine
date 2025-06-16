package com.thatdot.convert

import com.thatdot.{api, model}

/** Conversions from API models in [[com.thatdot.api.v2.outputs]]
  * to internal models in [[com.thatdot.model.v2.outputs]].
  */
object Outputs2ToApi2 {

  private def apply(format: model.v2.outputs.OutputEncoder): api.v2.outputs.OutputFormat = format match {
    case model.v2.outputs.OutputEncoder.JSON(_) => api.v2.outputs.OutputFormat.JSON
    case model.v2.outputs.OutputEncoder.Protobuf(schemaUrl, typeName, _) =>
      api.v2.outputs.OutputFormat.Protobuf(schemaUrl, typeName)
  }

  private def apply(
    logLevel: model.v2.outputs.destination.StandardOut.LogLevel,
  ): api.v2.outputs.DestinationSteps.StandardOut.LogLevel =
    logLevel match {
      case model.v2.outputs.destination.StandardOut.LogLevel.Trace =>
        api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Trace
      case model.v2.outputs.destination.StandardOut.LogLevel.Debug =>
        api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Debug
      case model.v2.outputs.destination.StandardOut.LogLevel.Info =>
        api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Info
      case model.v2.outputs.destination.StandardOut.LogLevel.Warn =>
        api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Warn
      case model.v2.outputs.destination.StandardOut.LogLevel.Error =>
        api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Error
    }

  private def apply(
    logMode: model.v2.outputs.destination.StandardOut.LogMode,
  ): api.v2.outputs.DestinationSteps.StandardOut.LogMode =
    logMode match {
      case model.v2.outputs.destination.StandardOut.LogMode.Complete =>
        api.v2.outputs.DestinationSteps.StandardOut.LogMode.Complete
      case model.v2.outputs.destination.StandardOut.LogMode.FastSampling =>
        api.v2.outputs.DestinationSteps.StandardOut.LogMode.FastSampling
    }

  def apply(destinationSteps: model.v2.outputs.DestinationSteps): api.v2.outputs.DestinationSteps =
    destinationSteps match {
      case model.v2.outputs.DestinationSteps.WithByteEncoding(formatAndEncode, destination) =>
        val format = apply(formatAndEncode)
        destination match {
          case endpoint: model.v2.outputs.ResultDestination.FoldableData.HttpEndpoint =>
            api.v2.outputs.DestinationSteps.HttpEndpoint(
              url = endpoint.url,
              parallelism = endpoint.parallelism,
            )
          case stream: model.v2.outputs.ResultDestination.Bytes.ReactiveStream =>
            api.v2.outputs.DestinationSteps.ReactiveStream(
              address = stream.address,
              port = stream.port,
              format = format,
            )
          case out: model.v2.outputs.ResultDestination.Bytes.StandardOut =>
            api.v2.outputs.DestinationSteps.StandardOut(
              logLevel = apply(out.logLevel),
              logMode = apply(out.logMode),
              format = format,
            )
          case sns: model.v2.outputs.ResultDestination.Bytes.SNS =>
            api.v2.outputs.DestinationSteps.SNS(
              credentials = sns.credentials.map(Model2ToApi2.apply),
              region = sns.region.map(Model2ToApi2.apply),
              topic = sns.topic,
              format = format,
            )
          case kafka: model.v2.outputs.ResultDestination.Bytes.Kafka =>
            api.v2.outputs.DestinationSteps.Kafka(
              topic = kafka.topic,
              bootstrapServers = kafka.bootstrapServers,
              format = format,
              kafkaProperties = kafka.kafkaProperties,
            )
          case kinesis: model.v2.outputs.ResultDestination.Bytes.Kinesis =>
            api.v2.outputs.DestinationSteps.Kinesis(
              credentials = kinesis.credentials.map(Model2ToApi2.apply),
              region = kinesis.region.map(Model2ToApi2.apply),
              streamName = kinesis.streamName,
              format = format,
              kinesisParallelism = kinesis.kinesisParallelism,
              kinesisMaxBatchSize = kinesis.kinesisMaxBatchSize,
              kinesisMaxRecordsPerSecond = kinesis.kinesisMaxRecordsPerSecond,
              kinesisMaxBytesPerSecond = kinesis.kinesisMaxBytesPerSecond,
            )
          case file: model.v2.outputs.ResultDestination.Bytes.File =>
            api.v2.outputs.DestinationSteps.File(
              path = file.path,
              format = format,
            )
        }
      case model.v2.outputs.DestinationSteps.WithDataFoldable(destination) =>
        destination match {
          case endpoint: model.v2.outputs.ResultDestination.FoldableData.HttpEndpoint =>
            api.v2.outputs.DestinationSteps.HttpEndpoint(
              endpoint.url,
              endpoint.parallelism,
            )
        }
      case model.v2.outputs.DestinationSteps.WithAny(destination) =>
        destination match {
          case _: model.v2.outputs.ResultDestination.AnyData.Drop => api.v2.outputs.DestinationSteps.Drop
        }
    }

}
