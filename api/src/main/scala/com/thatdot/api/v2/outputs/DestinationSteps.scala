package com.thatdot.api.v2.outputs

import sttp.tapir.Schema.annotations.{default, description, title}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

/** The ADT for API definitions of result destinations.
  * Using a sealed ADT for ease of Tapir schema derivation.
  */
@title("Destination Steps")
@description("Steps that transform results on their way to a destination.")
sealed trait DestinationSteps

object DestinationSteps {

  @title("Drop")
  final case object Drop extends DestinationSteps

  @title("Log JSON to File")
  @description(
    """Writes each result as a single-line JSON record.
      |For the format of the result, see "Standing Query Result Output".""".stripMargin,
  )
  final case class File(
    path: String,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  @title("POST to HTTP[S] Webhook")
  @description(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\".",
  )
  final case class HttpEndpoint(
    url: String,
    @default(8)
    parallelism: Int = 8,
  ) extends DestinationSteps

  @title("Publish to Kafka Topic")
  @description(
    """Publishes a JSON record for each result to the provided Apache Kafka topic.
      |For the format of the result record, see "Standing Query Result Output".""".stripMargin,
  )
  final case class Kafka(
    topic: String,
    bootstrapServers: String,
    @default(OutputFormat.JSON)
    format: OutputFormat = OutputFormat.JSON,
    @default(Map.empty[String, String])
    @description(
      """Map of Kafka producer properties.
        |See <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>""".stripMargin,
    )
    kafkaProperties: Map[String, String] = Map.empty[String, String],
  ) extends DestinationSteps
      with Format

  @title("Publish to Kinesis Data Stream")
  @description(
    """Publishes a JSON record for each result to the provided Kinesis stream.
      |For the format of the result record, see "StandingQueryResult".""".stripMargin,
  )
  final case class Kinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    streamName: String,
    @default(OutputFormat.JSON)
    format: OutputFormat = OutputFormat.JSON,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int],
  ) extends DestinationSteps
      with Format

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
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  @title("Publish to SNS Topic")
  @description(
    """Publishes an AWS SNS record to the provided topic containing JSON for each result.
      |For the format of the result, see "Standing Query Result Output".
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
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  @title("Log JSON to Console")
  @description("Prints each result as a single-line JSON object to stdout on the Quine server.")
  final case class StandardOut(
    @default(StandardOut.LogLevel.Info)
    logLevel: StandardOut.LogLevel = StandardOut.LogLevel.Info,
    @default(StandardOut.LogMode.Complete)
    logMode: StandardOut.LogMode = StandardOut.LogMode.Complete,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

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
}
