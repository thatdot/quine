package com.thatdot.api.v2.outputs

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

/** The ADT for shared result destinations. These correspond to the API types in each product, but only exist
  * for more convenient lowering to an interpreter. It's easier to automatically derive a conversion between
  * structurally identical case classes than to separately write the lowering function that allocates resources
  * for the interpreter.
  *
  * They also provide a place to define metadata for use in Tapir Schema annotations.
  */
sealed trait DestinationSteps

object DestinationSteps {
  val title = "Destination Steps"
  val description = "Steps that transform results on their way to a destination."

  final case class Drop() extends DestinationSteps

  object Drop {
    val title = "Drop"
    val description = "Effectively no destination at all, this does nothing but forget the data sent to it."
  }

  final case class File(
    path: String,
  ) extends DestinationSteps
//      with Format // Return this when prepared to support Protobuf (or more) in File writes

  object File {
    val propertyEncodedExampleForPath = "/temp/results.out"
    val description: String = """Writes each result as a single-line JSON record.
      |For the format of the result, see "Standing Query Result Output".""".stripMargin
    val title = "Write JSON to File"
  }

  final case class HttpEndpoint(
    url: String,
    parallelism: Int = HttpEndpoint.propertyDefaultValueForParallelism,
  ) extends DestinationSteps

  object HttpEndpoint {
    val propertyEncodedExampleForUrl = "https://results.example.com/result-type"
    val propertyDefaultValueForParallelism = 8
    val description =
      "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\"."
    val title = "POST to HTTP[S] Webhook"
  }

  case class KafkaPropertyValue(s: String) extends AnyVal

  final case class Kafka(
    topic: String,
    bootstrapServers: String,
    format: OutputFormat = Kafka.propertyDefaultValueForFormat,
    kafkaProperties: Map[String, KafkaPropertyValue] = Kafka.propertyDefaultValueForKafkaProperties,
  ) extends DestinationSteps
      with Format

  object Kafka {
    val propertyEncodedExampleForBootstrapServers = "kafka.svc.cluster.local:9092"
    val propertyEncodedExampleForTopic = "example-topic"
    val propertyDefaultValueForFormat: OutputFormat = OutputFormat.JSON
    val propertyDefaultValueForKafkaProperties: Map[String, KafkaPropertyValue] = Map.empty
    val propertyDefaultValueEncodedForKafkaProperties: Some[String] = Some("{}")
    val propertyDescriptionForKafkaProperties: String = """Map of Kafka producer properties.
        |See <https://kafka.apache.org/documentation.html#producerconfigs>""".stripMargin
    val description = "Publishes provided data to the specified Apache Kafka topic."

    val title = "Publish to Kafka Topic"
  }

  final case class Kinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    streamName: String,
    format: OutputFormat = Kinesis.propertyDefaultValueForFormat,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int],
  ) extends DestinationSteps
      with Format

  object Kinesis {
    val propertyEncodedExampleForStreamName = "example-stream"
    val propertyDefaultValueForFormat: OutputFormat = OutputFormat.JSON
    val description = "Publishes provided data to the specified Amazon Kinesis stream."
    val title = "Publish to Kinesis Data Stream"
  }

  final case class ReactiveStream(
    address: String = ReactiveStream.propertyDefaultValueForAddress,
    port: Int,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  object ReactiveStream {
    val propertyDescriptionForAddress = "The address to bind the reactive stream server on."
    val propertyDefaultValueForAddress = "localhost"
    val propertyDescriptionForPort = "The port to bind the reactive stream server on."
    val description: String =
      """Broadcasts data to a created Reactive Stream. Other thatDot products can subscribe to Reactive Streams.
      |⚠️ Warning: Reactive Stream outputs do not function correctly when running in a cluster.""".stripMargin
    val title = "Broadcast to Reactive Stream"
  }

  final case class SNS(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    topic: String,
    format: OutputFormat,
  ) extends DestinationSteps
      with Format

  object SNS {
    val propertyEncodedExampleForTopic = "example-topic"
    val propertyDescriptionForTopic = "ARN of the topic to publish to."
    val description: String = """Publishes an AWS SNS record to the provided topic.
      |⚠️ <b><em>Double check your credentials and topic ARN!</em></b> If writing to SNS fails, the write will
      |be retried indefinitely. If the error is unfixable (e.g., the topic or credentials
      |cannot be found), the outputs will never be emitted and the Standing Query this output
      |is attached to may stop running.""".stripMargin // Use StringOps#asOneLine when that is accessible
    val title = "Publish to SNS Topic"
  }

  final case class StandardOut() extends DestinationSteps

  object StandardOut {
    val description = "Prints each result as a single-line JSON object to stdout on the application server."
    val title = "Log JSON to Console"
  }

}
