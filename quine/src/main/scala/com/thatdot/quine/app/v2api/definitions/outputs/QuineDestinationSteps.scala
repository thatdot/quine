package com.thatdot.quine.app.v2api.definitions.outputs

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{default, description, encodedExample, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.{DestinationSteps, Format, OutputFormat}
import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.common.security.Secret

/** The Quine-local ADT for result destinations. Note that it includes both "copies" of ADT values defined also in
  * [[DestinationSteps]] <em>and</em> unique ADT values that are supported in Quine but not necessarily other products.
  * This is an outcome of philosophies inferred from Circe and Tapir that encourage—by design—APIs to be based on
  * sealed-trait ADTs rather than unsealed hierarchies. Effectively, we consider each product's API to be <em>its own
  * API</em> that should refer to a shared ADT of references when possible (rather than considering there to be a
  * "shared" API that is "extended" by certain products).
  */
@title(DestinationSteps.title)
@description(DestinationSteps.description)
sealed trait QuineDestinationSteps

/** Not intended for user visibility. Simply a helper trait to distinguish [[QuineDestinationSteps]]
  * types (particularly for conversion purposes).
  */
sealed trait MirrorOfCore

object QuineDestinationSteps {
  import com.thatdot.quine.app.util.StringOps.syntax._

  @title(DestinationSteps.Drop.title)
  @description(DestinationSteps.Drop.description)
  final case object Drop extends QuineDestinationSteps with MirrorOfCore

  @title(DestinationSteps.File.title)
  @description(DestinationSteps.File.description)
  final case class File(
    @encodedExample(DestinationSteps.File.propertyEncodedExampleForPath)
    path: String,
  ) extends QuineDestinationSteps
      with MirrorOfCore
  //      with Format // Return this when prepared to support Protobuf (or more) in File writes

  @title(DestinationSteps.HttpEndpoint.title)
  @description(DestinationSteps.HttpEndpoint.description)
  final case class HttpEndpoint(
    @encodedExample(DestinationSteps.HttpEndpoint.propertyEncodedExampleForUrl)
    url: String,
    @default(DestinationSteps.HttpEndpoint.propertyDefaultValueForParallelism)
    parallelism: Int = DestinationSteps.HttpEndpoint.propertyDefaultValueForParallelism,
  ) extends QuineDestinationSteps
      with MirrorOfCore

  @title(DestinationSteps.Kafka.title)
  @description(DestinationSteps.Kafka.description)
  final case class Kafka(
    @encodedExample(DestinationSteps.Kafka.propertyEncodedExampleForTopic)
    topic: String,
    @encodedExample(DestinationSteps.Kafka.propertyEncodedExampleForBootstrapServers)
    bootstrapServers: String,
    @default(DestinationSteps.Kafka.propertyDefaultValueForFormat)
    format: OutputFormat = DestinationSteps.Kafka.propertyDefaultValueForFormat,
    // @encodedExample provided in V2ApiSchemas (not working as annotation)
    @default(
      DestinationSteps.Kafka.propertyDefaultValueForKafkaProperties,
      DestinationSteps.Kafka.propertyDefaultValueEncodedForKafkaProperties,
    )
    @description(DestinationSteps.Kafka.propertyDescriptionForKafkaProperties)
    kafkaProperties: Map[String, KafkaPropertyValue] = DestinationSteps.Kafka.propertyDefaultValueForKafkaProperties,
  ) extends QuineDestinationSteps
      with MirrorOfCore
      with Format

  @title(DestinationSteps.Kinesis.title)
  @description(DestinationSteps.Kinesis.description)
  final case class Kinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @encodedExample(DestinationSteps.Kinesis.propertyEncodedExampleForStreamName)
    streamName: String,
    @default(DestinationSteps.Kinesis.propertyDefaultValueForFormat)
    format: OutputFormat = DestinationSteps.Kinesis.propertyDefaultValueForFormat,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int],
  ) extends QuineDestinationSteps
      with MirrorOfCore
      with Format

  @title(DestinationSteps.ReactiveStream.title)
  @description(DestinationSteps.ReactiveStream.description)
  final case class ReactiveStream(
    @description(DestinationSteps.ReactiveStream.propertyDescriptionForAddress)
    @default(DestinationSteps.ReactiveStream.propertyDefaultValueForAddress)
    address: String = DestinationSteps.ReactiveStream.propertyDefaultValueForAddress,
    @description(DestinationSteps.ReactiveStream.propertyDescriptionForPort)
    port: Int,
    format: OutputFormat,
  ) extends QuineDestinationSteps
      with MirrorOfCore
      with Format

  @title(DestinationSteps.SNS.title)
  @description(DestinationSteps.SNS.description)
  final case class SNS(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description(DestinationSteps.SNS.propertyDescriptionForTopic)
    @encodedExample(DestinationSteps.SNS.propertyEncodedExampleForTopic)
    topic: String,
    format: OutputFormat,
  ) extends QuineDestinationSteps
      with MirrorOfCore
      with Format

  @title(DestinationSteps.StandardOut.title)
  @description(DestinationSteps.StandardOut.description)
  final case object StandardOut extends QuineDestinationSteps with MirrorOfCore

  /** @param query what to execute for every standing query result or other provided data
    * @param parameter name of the parameter associated with SQ results
    * @param parallelism how many queries to run at once
    * @param allowAllNodeScan to prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true
    */
  @title("Run Cypher Query")
  @description(
    """Runs the `query`, where the given `parameter` is used to reference the data that is passed in.
      |Runs at most `parallelism` queries simultaneously.""".asOneLine,
  )
  final case class CypherQuery(
    @description(CypherQuery.queryDescription)
    @encodedExample(CypherQuery.exampleQuery)
    query: String,
    @description("Name of the Cypher parameter to assign incoming data to.")
    @default("that")
    parameter: String = "that",
    @description("Maximum number of queries of this kind allowed to run at once.")
    @default(com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism)
    parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
    @description(
      """To prevent unintentional resource use, if the Cypher query may contain an all-node scan,
        |this parameter must be `true`.""".asOneLine,
    )
    @default(false)
    allowAllNodeScan: Boolean = false,
    @description(
      """Whether queries that raise a potentially-recoverable error should be retried. If set to `true` (the default),
        |such errors will be retried until they succeed. ⚠️ Note that if the query is not idempotent, the query's
        |effects may occur multiple times in the case of external system failure. Query idempotency
        |can be checked with the EXPLAIN keyword. If set to `false`, results and effects will not be duplicated,
        |but may be dropped in the case of external system failure""".asOneLine,
    )
    @default(true)
    shouldRetry: Boolean = true,
  ) extends QuineDestinationSteps

  object CypherQuery {
    val queryDescription: String = "Cypher query to execute on Standing Query result"
    val exampleQuery: String = "MATCH (n) WHERE id(n) = $that.id RETURN (n)"

    implicit val encoder: Encoder[CypherQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[CypherQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[CypherQuery] = Schema.derived
  }

  @title("Publish to Slack Webhook")
  @description(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>.",
  )
  final case class Slack(
    @encodedExample("https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")
    hookUrl: String,
    @default(false)
    onlyPositiveMatchData: Boolean = false,
    @description("Number of seconds to wait between messages; minimum 1.")
    @default(20)
    intervalSeconds: Int = 20,
  ) extends QuineDestinationSteps

  implicit val encoder: Encoder[QuineDestinationSteps] = deriveConfiguredEncoder
  implicit val decoder: Decoder[QuineDestinationSteps] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[QuineDestinationSteps] = Schema.derived

  /** Encoder that preserves credential values for persistence.
    * Requires witness (`import Secret.Unsafe._`) to call.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[QuineDestinationSteps] = {
    implicit val awsCredsEnc: Encoder[AwsCredentials] = AwsCredentials.preservingEncoder
    deriveConfiguredEncoder
  }
}
