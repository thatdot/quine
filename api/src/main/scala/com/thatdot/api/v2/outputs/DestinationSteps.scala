package com.thatdot.api.v2.outputs

import sttp.tapir.Schema.annotations.{default, description, encodedExample, title}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

/** The ADT for API definitions of result destinations.
  * Using a sealed ADT for ease of Tapir schema derivation.
  */
@title("Destination Steps")
@description("Steps that propagate results to a destination.")
sealed trait DestinationSteps

object DestinationSteps {

  @title("Drop")
  @description("Effectively no destination at all, this does nothing but forget the data sent to it.")
  final case object Drop extends DestinationSteps

  @title("Write JSON to File")
  @description(
    """Writes each result as a single-line JSON record.
      |For the format of the result, see "Standing Query Result Output".""".stripMargin,
  )
  final case class File(
    @encodedExample("/temp/results.out")
    path: String,
  ) extends DestinationSteps
//      with Format // Return this when prepared to support Protobuf (or more) in File writes

  @title("POST to HTTP[S] Webhook")
  @description(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\".",
  )
  final case class HttpEndpoint(
    @encodedExample("https://results.example.com/result-type")
    url: String,
    @default(8)
    parallelism: Int = 8,
  ) extends DestinationSteps

  case class KafkaPropertyValue(s: String) extends AnyVal

  @title("Publish to Kafka Topic")
  @description("Publishes provided data to the specified Apache Kafka topic.")
  final case class Kafka(
    @encodedExample("example-topic")
    topic: String,
    @encodedExample("kafka.svc.cluster.local:9092")
    bootstrapServers: String,
    @default(OutputFormat.JSON)
    format: OutputFormat = OutputFormat.JSON,
    // Encoded example provided in V2StandingApiSchemas
    @default(Map.empty[String, KafkaPropertyValue], Some("{}"))
    @description(
      """Map of Kafka producer properties.
        |See <https://kafka.apache.org/documentation.html#producerconfigs>""".stripMargin,
    )
    kafkaProperties: Map[String, KafkaPropertyValue] = Map.empty,
  ) extends DestinationSteps
      with Format

  @title("Publish to Kinesis Data Stream")
  @description("Publishes provided data to the specified Amazon Kinesis stream.")
  final case class Kinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @encodedExample("example-stream")
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
    """Broadcasts data to a created Reactive Stream. Other thatDot products can subscribe to Reactive Streams.
      |Warning: Reactive Stream outputs do not function correctly when running in a cluster.""".stripMargin,
  )
  final case class ReactiveStream(
    @description("The address to bind the reactive stream server on.")
    @default("localhost")
    address: String = "localhost",
    @description("The port to bind the reactive stream server on.")
    port: Int,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  @title("Publish to SNS Topic")
  @description(
    """Publishes an AWS SNS record to the provided topic.
      |⚠️ <b><em>Double check your credentials and topic ARN!</em></b> If writing to SNS fails, the write will
      |be retried indefinitely. If the error is unfixable (e.g., the topic or credentials
      |cannot be found), the outputs will never be emitted and the Standing Query this output
      |is attached to may stop running.""".stripMargin,
  )
  final case class SNS(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("ARN of the topic to publish to.")
    @encodedExample("example-topic")
    topic: String,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  @title("Log to Console")
  @description("Prints each result as a single line to stdout on the Quine server.")
  final case class StandardOut(
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

}
