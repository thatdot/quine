package com.thatdot.convert

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.actor.ActorSystem

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.StringInput
import com.thatdot.{api, outputs2}

/** Conversions from API models in [[api.v2.outputs]]
  * to internal models in [[outputs2]] and [[com.thatdot.quine.app.model.outputs2]].
  */
object Api2ToOutputs2 {

  def apply(
    format: api.v2.outputs.OutputFormat,
  )(implicit protobufSchemaCache: ProtobufSchemaCache, ec: ExecutionContext): Future[outputs2.OutputEncoder] =
    format match {
      case api.v2.outputs.OutputFormat.JSON =>
        Future.successful(outputs2.OutputEncoder.JSON())
      case api.v2.outputs.OutputFormat.Protobuf(schemaUrl, typeName) =>
        protobufSchemaCache
          .getMessageDescriptor(StringInput.filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
          .map(desc => outputs2.OutputEncoder.Protobuf(schemaUrl, typeName, desc))
    }

  private def apply(
    logLevel: api.v2.outputs.DestinationSteps.StandardOut.LogLevel,
  ): outputs2.destination.StandardOut.LogLevel =
    logLevel match {
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Trace =>
        outputs2.destination.StandardOut.LogLevel.Trace
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Debug =>
        outputs2.destination.StandardOut.LogLevel.Debug
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Info =>
        outputs2.destination.StandardOut.LogLevel.Info
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Warn =>
        outputs2.destination.StandardOut.LogLevel.Warn
      case api.v2.outputs.DestinationSteps.StandardOut.LogLevel.Error =>
        outputs2.destination.StandardOut.LogLevel.Error
    }

  private def apply(
    logMode: api.v2.outputs.DestinationSteps.StandardOut.LogMode,
  ): outputs2.destination.StandardOut.LogMode =
    logMode match {
      case api.v2.outputs.DestinationSteps.StandardOut.LogMode.Complete =>
        outputs2.destination.StandardOut.LogMode.Complete
      case api.v2.outputs.DestinationSteps.StandardOut.LogMode.FastSampling =>
        outputs2.destination.StandardOut.LogMode.FastSampling
    }

  def apply(
    destinationSteps: api.v2.outputs.DestinationSteps,
  )(implicit
    graph: BaseGraph,
    ec: ExecutionContext,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[outputs2.FoldableDestinationSteps] = {
    implicit val system: ActorSystem = graph.system

    destinationSteps match {
      case api.v2.outputs.DestinationSteps.Drop =>
        Future.successful(
          outputs2.FoldableDestinationSteps.WithAny(
            destination = outputs2.destination.Drop,
          ),
        )
      case api.v2.outputs.DestinationSteps.File(path) =>
        Future.successful(
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = outputs2.OutputEncoder.JSON(),
            destination = outputs2.destination.File(
              path = path,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.HttpEndpoint(url, parallelism) =>
        Future.successful(
          outputs2.FoldableDestinationSteps.WithDataFoldable(
            destination = outputs2.destination.HttpEndpoint(
              url = url,
              parallelism = parallelism,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.Kafka(topic, bootstrapServers, format, kafkaProperties) =>
        apply(format).map(enc =>
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = outputs2.destination.Kafka(
              topic = topic,
              bootstrapServers = bootstrapServers,
              kafkaProperties = kafkaProperties.view.mapValues(_.toString).toMap,
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
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = outputs2.destination.Kinesis(
              credentials = credentials.map(Api2ToAws.apply),
              region = region.map(Api2ToAws.apply),
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
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = outputs2.destination.ReactiveStream(
              address = address,
              port = port,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.SNS(credentials, region, topic, format) =>
        apply(format).map(enc =>
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = outputs2.destination.SNS(
              credentials = credentials.map(Api2ToAws.apply),
              region = region.map(Api2ToAws.apply),
              topic = topic,
            ),
          ),
        )
      case api.v2.outputs.DestinationSteps.StandardOut(logLevel, logMode, format) =>
        apply(format).map(enc =>
          outputs2.FoldableDestinationSteps.WithByteEncoding(
            formatAndEncode = enc,
            destination = outputs2.destination.StandardOut(
              logLevel = apply(logLevel),
              logMode = apply(logMode),
            ),
          ),
        )
    }
  }

}
