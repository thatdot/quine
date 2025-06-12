package com.thatdot.convert

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.actor.ActorSystem

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.StringInput
import com.thatdot.{api, model}

/** Conversions from internal models in [[com.thatdot.model.v2.outputs]] and [[com.thatdot.quine.app.model.outputs2]]
  * to API models in [[com.thatdot.api.v2.outputs]].
  */
object Api2ToOutputs2 {

  def apply(
    format: api.v2.outputs.OutputFormat,
  )(implicit protobufSchemaCache: ProtobufSchemaCache, ec: ExecutionContext): Future[model.v2.outputs.OutputEncoder] =
    format match {
      case api.v2.outputs.OutputFormat.JSON =>
        Future.successful(model.v2.outputs.OutputEncoder.JSON())
      case api.v2.outputs.OutputFormat.Protobuf(schemaUrl, typeName) =>
        protobufSchemaCache
          .getMessageDescriptor(StringInput.filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
          .map(desc => model.v2.outputs.OutputEncoder.Protobuf(schemaUrl, typeName, desc))
    }

  private def apply(
    logLevel: api.v2.outputs.DestinationSteps.StandardOut.LogLevel,
  ): model.v2.outputs.destination.StandardOut.LogLevel =
    logLevel match {
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Trace =>
        model.v2.outputs.destination.StandardOut.LogLevel.Trace
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Debug =>
        model.v2.outputs.destination.StandardOut.LogLevel.Debug
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Info =>
        model.v2.outputs.destination.StandardOut.LogLevel.Info
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Warn =>
        model.v2.outputs.destination.StandardOut.LogLevel.Warn
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Error =>
        model.v2.outputs.destination.StandardOut.LogLevel.Error
    }

  private def apply(
    logMode: api.v2.outputs.DestinationSteps.StandardOut.LogMode,
  ): model.v2.outputs.destination.StandardOut.LogMode =
    logMode match {
      case api.v2.outputs.DestinationSteps.StandardOut.LogMode.Complete =>
        model.v2.outputs.destination.StandardOut.LogMode.Complete
      case api.v2.outputs.DestinationSteps.StandardOut.LogMode.FastSampling =>
        model.v2.outputs.destination.StandardOut.LogMode.FastSampling
    }

  def apply(
    destinationSteps: api.v2.outputs.DestinationSteps,
  )(implicit
    graph: BaseGraph,
    ec: ExecutionContext,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[model.v2.outputs.DestinationSteps] = {
    implicit val system: ActorSystem = graph.system

    destinationSteps match {
      case api.v2.outputs.DestinationSteps.Drop =>
        Future.successful(
          model.v2.outputs.DestinationSteps.WithAny(
            destination = model.v2.outputs.destination.Drop,
          ),
        )
      case api.v2.outputs.DestinationSteps.File(path, format) =>
        apply(format).map(enc =>
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.File(
              path = path,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.HttpEndpoint(url, parallelism) =>
        Future.successful(
          model.v2.outputs.DestinationSteps.WithDataFoldable(
            destination = model.v2.outputs.destination.HttpEndpoint(
              url = url,
              parallelism = parallelism,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.Kafka(topic, bootstrapServers, format, kafkaProperties) =>
        apply(format).map(enc =>
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.Kafka(
              topic = topic,
              bootstrapServers = bootstrapServers,
              kafkaProperties = kafkaProperties,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.Kinesis(
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
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.Kinesis(
              credentials = credentials.map(Api2ToModel2.apply),
              region = region.map(Api2ToModel2.apply),
              streamName = streamName,
              kinesisParallelism = kinesisParallelism,
              kinesisMaxBatchSize = kinesisMaxBatchSize,
              kinesisMaxRecordsPerSecond = kinesisMaxRecordsPerSecond,
              kinesisMaxBytesPerSecond = kinesisMaxBytesPerSecond,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.ReactiveStream(address, port, format) =>
        apply(format).map(enc =>
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.ReactiveStream(
              address = address,
              port = port,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.SNS(credentials, region, topic, format) =>
        apply(format).map(enc =>
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.SNS(
              credentials = credentials.map(Api2ToModel2.apply),
              region = region.map(Api2ToModel2.apply),
              topic = topic,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.StandardOut(logLevel, logMode, format) =>
        apply(format).map(enc =>
          model.v2.outputs.DestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = model.v2.outputs.destination.StandardOut(
              logLevel = apply(logLevel),
              logMode = apply(logMode),
            ),
          ),
        )
    }
  }

}
