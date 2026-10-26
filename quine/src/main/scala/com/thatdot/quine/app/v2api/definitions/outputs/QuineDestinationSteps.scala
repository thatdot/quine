package com.thatdot.quine.app.v2api.definitions.outputs

import scala.concurrent.duration._

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema.annotations.{customise, default, description, encodedExample, title, validate}
import sttp.tapir.{Schema, Validator}

import com.thatdot.api.codec.SecretCodecs._
import com.thatdot.api.schema.SecretSchemas._
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
import com.thatdot.api.v2.codec.ThirdPartyCodecs.scala.{finiteDurationDecoder, finiteDurationEncoder}
import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.{DestinationSteps, Format, OutputFormat}
import com.thatdot.api.v2.schema.ThirdPartySchemas.scala.finiteDurationSchema
import com.thatdot.api.v2.{AwsCredentials, AwsRegion, ProductSpecific, SaslJaasConfig}
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.outputs2.destination.NoveltyAuth

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
    @description(DestinationSteps.HttpEndpoint.propertyDescriptionForHeaders)
    @default(DestinationSteps.HttpEndpoint.propertyDefaultValueForHeaders)
    headers: Map[String, Secret] = DestinationSteps.HttpEndpoint.propertyDefaultValueForHeaders,
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
    @description("Password for the SSL keystore. Redacted in API responses.")
    sslKeystorePassword: Option[Secret] = None,
    @description("Password for the SSL truststore. Redacted in API responses.")
    sslTruststorePassword: Option[Secret] = None,
    @description("Password for the SSL key. Redacted in API responses.")
    sslKeyPassword: Option[Secret] = None,
    @description("SASL/JAAS configuration for Kafka authentication. Secrets are redacted in API responses.")
    saslJaasConfig: Option[SaslJaasConfig] = None,
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
        |such errors will be retried until they succeed. Additionally, if the query is not idempotent, the query's
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
    @description("Time to wait between messages; minimum 1 second.")
    @encodedExample("20s")
    interval: FiniteDuration = 20.seconds,
  ) extends QuineDestinationSteps

  @title("Stream to Novelty")
  @customise(ProductSpecific.enterpriseOnly)
  @description(
    "Streams each standing-query result to a Novelty instance for scoring and writes each score back into " +
    "the graph. A score is stored on the node `idFrom(<score prefix>, <model, lowercased>, observation)`, with label " +
    "`NoveltyScore` and, as properties, every field of Novelty's scored result except `tag` (`score`, `observation`, " +
    "`totalObsScore`, `probability`, `uniqueness`, `infoContent`, `sequence`, and `mostNovelComponent` as a map). The score prefix is set cluster-wide by `quine.novelty.score-prefix` " +
    "(default `novelty-score`). The same observation scored against the same model always maps to the " +
    "same node, so its latest score overwrites the previous one. When the scored result carries a tag that " +
    "is a node id (as returned by `strId(n)`), an edge labelled `scoreEdgeLabel` is added from that node " +
    "to the score node. Observations are " +
    "load-balanced across the configured `endpoints` (independent Novelty instances); the connection " +
    "carries backpressure end-to-end.",
  )
  final case class Novelty(
    @description(
      "Novelty base URLs, e.g. `http://host:8382`. Each is an independent Novelty instance; observations " +
      "are load-balanced across them. At least one is required.",
    )
    @encodedExample("http://localhost:8382")
    @validate(Validator.minSize[String, List](1))
    endpoints: List[String],
    @description(
      "Novelty model (namespace) to score against. Created on each endpoint if it doesn't exist there yet; it is " +
      "never deleted. Model names are case-insensitive.",
    )
    @encodedExample("default")
    model: String,
    @description(
      "Optional name of a transformation (registered on Novelty) that maps each result into an observation; " +
      "applied on the Novelty side. Ignored when `transformationFunction` is set.",
    )
    transformation: Option[String] = None,
    @description(
      "Optional inline JavaScript transformation that maps each result into an observation, applied on the " +
      "Novelty side as part of the ingest Quine manages there for this output. Takes precedence over `transformation`, so no transformation " +
      "need be pre-registered on Novelty. Example: `that => ({ observation: [String(that.data.num)], tag: that.data.id })`, " +
      "where the standing query returns `strId(n) AS id` so the tag links the score to that node.",
    )
    transformationFunction: Option[String] = None,
    @description(
      "Record format of `transformationFunction`'s output: `TAGGED` (returns `{observation, tag}`) or `BARE` " +
      "(returns a bare array of strings). Required when `transformationFunction` is set, and only allowed then.",
    )
    @encodedExample("TAGGED")
    transformationFormat: Option[NoveltyRecordFormat] = None,
    @description(
      "Label of the edge added from a tagged node to its score node, when the tag is a node id.",
    )
    @default("HAS_NOVELTY_SCORE")
    scoreEdgeLabel: String = "HAS_NOVELTY_SCORE",
    @description("Maximum number of scores to write to the graph at once.")
    @default(com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism)
    parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
    @description(
      "Whether score writes that raise a potentially-recoverable error should be retried (at-least-once).",
    )
    @default(true)
    shouldRetry: Boolean = true,
    @description(
      "Optional OAuth authentication to Novelty, by client secret or certificate. Used only when Novelty has " +
      "auth enabled, both for the scoring connections and for managing this output's ingest on Novelty, which " +
      "includes creating the model if it is missing (so the client needs permission to create models). A token " +
      "is reused until shortly before it expires.",
    )
    auth: Option[NoveltyAuth] = None,
    @description(
      "Identifies this output to Novelty: the Novelty ingest that serves it is named from this id. " +
      "Assigned by Quine when the output is created; any value supplied in a request is replaced.",
    )
    ownerId: Option[String] = None,
  ) extends QuineDestinationSteps

  object Novelty {
    implicit val encoder: Encoder[Novelty] = deriveConfiguredEncoder
    implicit val decoder: Decoder[Novelty] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[Novelty] = Schema.derived
  }

  /** What a Novelty output's inline transformation function returns for each result. */
  sealed abstract class NoveltyRecordFormat(val name: String)
  object NoveltyRecordFormat {

    /** `{observation, tag}`: each score comes back with its tag. */
    case object Tagged extends NoveltyRecordFormat("TAGGED")

    /** A bare array of strings: the observation alone. */
    case object Bare extends NoveltyRecordFormat("BARE")

    val values: Seq[NoveltyRecordFormat] = Seq(Tagged, Bare)

    implicit val encoder: Encoder[NoveltyRecordFormat] = Encoder.encodeString.contramap(_.name)
    implicit val decoder: Decoder[NoveltyRecordFormat] = Decoder.decodeString.emap { name =>
      values.find(_.name == name).toRight(s"`transformationFormat` must be `TAGGED` or `BARE`, but was `$name`.")
    }
    implicit lazy val schema: Schema[NoveltyRecordFormat] =
      Schema.derivedEnumeration[NoveltyRecordFormat](encode = Some(_.name))
  }

  implicit val encoder: Encoder[QuineDestinationSteps] = deriveConfiguredEncoder
  implicit val decoder: Decoder[QuineDestinationSteps] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[QuineDestinationSteps] = Schema.derived

  /** Encoder that preserves credential values for persistence.
    * Requires witness (`import Secret.Unsafe._`) to call.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[QuineDestinationSteps] = {
    import com.thatdot.api.codec.SecretCodecs
    // Shadow the redacting encoders (names must match to shadow)
    implicit val secretEncoder: Encoder[Secret] = SecretCodecs.preservingEncoder
    implicit val awsCredentialsEncoder: Encoder[AwsCredentials] = AwsCredentials.preservingEncoder
    implicit val saslJaasConfigEncoder: Encoder[SaslJaasConfig] = SaslJaasConfig.preservingEncoder
    // Derive encoders for subtypes that contain secrets
    implicit val httpEndpointEncoder: Encoder[HttpEndpoint] = deriveConfiguredEncoder
    implicit val kafkaEncoder: Encoder[Kafka] = deriveConfiguredEncoder
    implicit val kinesisEncoder: Encoder[Kinesis] = deriveConfiguredEncoder
    implicit val snsEncoder: Encoder[SNS] = deriveConfiguredEncoder
    // Novelty's `auth` carries Secrets (client secret / keystore password) — preserve through persistence.
    implicit val noveltyAuthEncoder: Encoder[NoveltyAuth] = NoveltyAuth.preservingEncoder
    implicit val noveltyEncoder: Encoder[Novelty] = deriveConfiguredEncoder
    deriveConfiguredEncoder
  }
}
