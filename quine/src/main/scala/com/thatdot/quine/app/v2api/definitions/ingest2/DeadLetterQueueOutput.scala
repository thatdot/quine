package com.thatdot.quine.app.v2api.definitions.ingest2

import sttp.tapir.Schema.annotations.{default, description, title}

import com.thatdot.api.v2.outputs.{DestinationSteps, DestinationSteps => Outputs, OutputFormat => OutputFormats}
import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.app.v2api.definitions.ingest2.OutputFormat.JSON

sealed trait DeadLetterQueueOutput

@title("Error Output Format")
sealed trait OutputFormat

object OutputFormat {
  case object Bytes extends OutputFormat

  @title("JSON")
  case class JSON(
    @default(false)
    @description("Should extra information be included about the cause of a record ending up in the dead letter queue")
    withInfoEnvelope: Boolean = false,
  ) extends OutputFormat

  @title("Protobuf")
  final case class Protobuf(
    @description(
      "URL (or local filename) of the Protobuf .desc file to load that contains the desired typeName to serialize to",
    )
    schemaUrl: String,
    @description("message type name to use (from the given .desc file) as the message type")
    typeName: String,
    @default(false)
    @description("Should extra information be included about the cause of a record ending up in the dead letter queue")
    withInfoEnvelope: Boolean = false,
  ) extends OutputFormat
}

case class DeadLetterQueueSettings(
  @description("The list of dead letter queue destinations to send failing records to")
  destinations: List[DeadLetterQueueOutput] = Nil,
)

object DeadLetterQueueOutput {

  @title("POST to HTTP[S] Webhook")
  @description(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\".",
  )
  final case class HttpEndpoint(
    url: String,
    @default(8)
    parallelism: Int = 8,
    @default(JSON())
    outputFormat: JSON = JSON(),
  ) extends DeadLetterQueueOutput

  final case class File(
    path: String,
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  final case class Kafka(
    topic: String,
    bootstrapServers: String,
    @description(
      """Map of Kafka producer properties.
                              |See <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>""".stripMargin,
    )
    @default(Map.empty[String, String])
    kafkaProperties: Map[String, String] = Map.empty[String, String],
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  @title("Publish to Kinesis Data Stream")
  @description(
    """Publishes a JSON record for each result to the provided Kinesis stream.
      |For the format of the result record, see "StandingQueryResult".""".stripMargin,
  )
  final case class Kinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    streamName: String,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int],
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  @title("Broadcast to Reactive Stream")
  @description(
    """Creates a 1 to many reactive stream output that other thatDot products can subscribe to.
      |Warning: Reactive Stream outputs do not function correctly when running in a cluster.""".stripMargin,
  )
  final case class ReactiveStream(
    @description("The address to bind the reactive stream server on")
    @default("localhost")
    address: String = "localhost",
    @description("The port to bind the reactive stream server on")
    port: Int,
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  @title("Publish to SNS Topic")
  @description(
    """Publishes an AWS SNS record to the provided topic containing JSON for each result.
      |
      |**Double check your credentials and topic ARN.** If writing to SNS fails, the write will
      |be retried indefinitely. If the error is unfixable (eg, the topic or credentials
      |cannot be found), the outputs will never be emitted and the Standing Query this output
      |is attached to may stop running.""".stripMargin,
  )
  final case class SNS(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("ARN of the topic to publish to")
    topic: String,
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  @title("Log JSON to Console")
  @description("Prints each result as a single-line JSON object to stdout on the Quine server.")
  final case class StandardOut(
    @default(StandardOut.LogLevel.Info)
    logLevel: StandardOut.LogLevel = StandardOut.LogLevel.Info,
    @default(StandardOut.LogMode.Complete)
    logMode: StandardOut.LogMode = StandardOut.LogMode.Complete,
    outputFormat: OutputFormat,
  ) extends DeadLetterQueueOutput

  object StandardOut {

    /** @see [[StandingQuerySchemas.logModeSchema]]
      */
    sealed abstract class LogMode

    object LogMode {
      case object Complete extends LogMode

      case object FastSampling extends LogMode

      val modes: Seq[LogMode] = Vector(Complete, FastSampling)
    }

    sealed abstract class LogLevel

    object LogLevel {
      case object Trace extends LogLevel

      case object Debug extends LogLevel

      case object Info extends LogLevel

      case object Warn extends LogLevel

      case object Error extends LogLevel

      val levels: Seq[LogLevel] = Vector(Trace, Debug, Info, Warn, Error)
    }
  }

  private def formatMatchesOutput(outputFormat: OutputFormats): OutputFormat = outputFormat match {
    case OutputFormats.JSON => OutputFormat.JSON()
    case OutputFormats.Protobuf(schemaUrl, typeName) =>
      OutputFormat.Protobuf(schemaUrl, typeName)
  }

  private def dlqLogLevelMatchesOutputs(
    level: DestinationSteps.StandardOut.LogLevel,
  ): DeadLetterQueueOutput.StandardOut.LogLevel =
    level match {
      case DestinationSteps.StandardOut.LogLevel.Trace =>
        DeadLetterQueueOutput.StandardOut.LogLevel.Trace
      case DestinationSteps.StandardOut.LogLevel.Debug =>
        DeadLetterQueueOutput.StandardOut.LogLevel.Debug
      case DestinationSteps.StandardOut.LogLevel.Info =>
        DeadLetterQueueOutput.StandardOut.LogLevel.Info
      case DestinationSteps.StandardOut.LogLevel.Warn =>
        DeadLetterQueueOutput.StandardOut.LogLevel.Warn
      case DestinationSteps.StandardOut.LogLevel.Error =>
        DeadLetterQueueOutput.StandardOut.LogLevel.Error
    }

  /** Convert a DestinationSteps `LogMode` to its DLQ counterpart */
  private def dlqLogModeMatchesOutputs(
    mode: DestinationSteps.StandardOut.LogMode,
  ): DeadLetterQueueOutput.StandardOut.LogMode =
    mode match {
      case DestinationSteps.StandardOut.LogMode.Complete =>
        DeadLetterQueueOutput.StandardOut.LogMode.Complete
      case DestinationSteps.StandardOut.LogMode.FastSampling =>
        DeadLetterQueueOutput.StandardOut.LogMode.FastSampling
    }

  /** The intention for this function is to throw warnings for any output format that is not supported
    * as a dead letter queue output.  Please consider whether that really should be the case and update the outputs
    * supported by dead letter queues to match if they start to diverge.
    */
  def dlqMatchesOutputs(outputs: Outputs): DeadLetterQueueOutput = outputs match {

    case DestinationSteps.Drop =>
      throw new IllegalArgumentException(
        "Drop cannot be used as a Dead-Letter-Queue destination",
      )

    case DestinationSteps.File(path, format) =>
      DeadLetterQueueOutput.File(path, formatMatchesOutput(format))

    case DestinationSteps.HttpEndpoint(url, parallelism) =>
      DeadLetterQueueOutput.HttpEndpoint(url, parallelism)

    case DestinationSteps.ReactiveStream(address, port, format) =>
      DeadLetterQueueOutput.ReactiveStream(address, port, formatMatchesOutput(format))

    case DestinationSteps.StandardOut(logLevel, logMode, format) =>
      DeadLetterQueueOutput.StandardOut(
        dlqLogLevelMatchesOutputs(logLevel),
        dlqLogModeMatchesOutputs(logMode),
        formatMatchesOutput(format),
      )

    // ──────────────── mappings with reordered params ────────────────
    case DestinationSteps.Kafka(topic, bootstrapServers, format, kafkaProperties) =>
      DeadLetterQueueOutput.Kafka(topic, bootstrapServers, kafkaProperties, formatMatchesOutput(format))

    case DestinationSteps.Kinesis(
          credentials,
          region,
          streamName,
          format,
          kinesisParallelism,
          kinesisMaxBatchSize,
          kinesisMaxRecordsPerSecond,
          kinesisMaxBytesPerSecond,
        ) =>
      DeadLetterQueueOutput.Kinesis(
        credentials,
        region,
        streamName,
        kinesisParallelism,
        kinesisMaxBatchSize,
        kinesisMaxRecordsPerSecond,
        kinesisMaxBytesPerSecond,
        formatMatchesOutput(format),
      )

    case DestinationSteps.SNS(credentials, region, topic, format) =>
      DeadLetterQueueOutput.SNS(credentials, region, topic, formatMatchesOutput(format))
  }

}
