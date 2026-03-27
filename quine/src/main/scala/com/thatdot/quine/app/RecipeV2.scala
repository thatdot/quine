package com.thatdot.quine.app

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{default, description, title}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.{
  IngestSource,
  OnRecordErrorHandler,
  OnStreamErrorHandler,
  Transformation,
}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.{
  Predicate,
  StandingQueryPattern,
  StandingQueryResultTransformation,
}

/** V2 Recipe Schema - aligned with V2 API structure */
object RecipeV2 {

  // Use the same configuration as the V2 API types (with "type" discriminator)
  // This ensures proper decoding of nested sealed traits like IngestSource, StandingQueryPattern, etc.
  implicit private val circeConfig: Configuration =
    Configuration.default.withDefaults.withDiscriminator("type")

  val currentVersion: Int = 2

  // ─────────────────────────────────────────────────────────────────────────────
  // Ingest Stream Configuration (V2 style)
  // ─────────────────────────────────────────────────────────────────────────────

  @title("V2 Ingest Stream Configuration")
  @description("Configuration for a data ingest stream in V2 recipe format.")
  final case class IngestStreamV2(
    @description("Optional name identifying the ingest stream. If not provided, a name will be generated.")
    name: Option[String] = None,
    @description("Data source configuration.")
    source: IngestSource,
    @description("Cypher query to execute on each record.")
    query: String,
    @description("Name of the Cypher parameter to populate with the input value.")
    @default("that")
    parameter: String = "that",
    @description("Optional JavaScript transformation function to pre-process input before Cypher query.")
    transformation: Option[Transformation] = None,
    @description("Maximum number of records to process at once.")
    @default(16)
    parallelism: Int = 16,
    @description("Maximum number of records to process per second.")
    maxPerSecond: Option[Int] = None,
    @description("Action to take on a single failed record.")
    @default(OnRecordErrorHandler())
    onRecordError: OnRecordErrorHandler = OnRecordErrorHandler(),
    @description("Action to take on a failure of the input stream.")
    onStreamError: Option[OnStreamErrorHandler] = None,
  )

  object IngestStreamV2 {
    implicit val encoder: Encoder[IngestStreamV2] = deriveConfiguredEncoder
    implicit val decoder: Decoder[IngestStreamV2] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[IngestStreamV2] = Schema.derived
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Standing Query Configuration (V2 workflow style)
  // ─────────────────────────────────────────────────────────────────────────────

  @title("Result Enrichment Cypher Query")
  @description("A Cypher query used to enrich standing query results.")
  final case class ResultEnrichmentQuery(
    @description("Cypher query to execute for enrichment.")
    query: String,
    @description("Name of the Cypher parameter to assign incoming data to.")
    @default("that")
    parameter: String = "that",
  )

  object ResultEnrichmentQuery {
    implicit val encoder: Encoder[ResultEnrichmentQuery] = deriveConfiguredEncoder
    implicit val decoder: Decoder[ResultEnrichmentQuery] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[ResultEnrichmentQuery] = Schema.derived
  }

  @title("Standing Query Result Workflow")
  @description(
    """A workflow comprising steps toward sending data derived from StandingQueryResults to destinations.
      |The workflow steps are processed in order: filter → preEnrichmentTransformation → resultEnrichment → destinations.""".stripMargin,
  )
  final case class StandingQueryResultWorkflowV2(
    @description("Optional name for this output workflow. If not provided, a name will be generated.")
    name: Option[String] = None,
    @description("Optional filter to apply to results before processing.")
    filter: Option[Predicate] = None,
    @description("Optional transformation to apply to results before enrichment.")
    preEnrichmentTransformation: Option[StandingQueryResultTransformation] = None,
    @description("Optional Cypher query to enrich results.")
    resultEnrichment: Option[ResultEnrichmentQuery] = None,
    @description("Destinations to send the processed results to (at least one required).")
    destinations: NonEmptyList[QuineDestinationSteps],
  )

  object StandingQueryResultWorkflowV2 {
    import com.thatdot.api.v2.schema.ThirdPartySchemas.cats._

    implicit val encoder: Encoder[StandingQueryResultWorkflowV2] = deriveConfiguredEncoder
    implicit val decoder: Decoder[StandingQueryResultWorkflowV2] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[StandingQueryResultWorkflowV2] = Schema.derived
  }

  @title("V2 Standing Query Definition")
  @description("A standing query definition in V2 recipe format with workflow-based outputs.")
  final case class StandingQueryDefinitionV2(
    @description("Optional name for this Standing Query. If not provided, a name will be generated.")
    name: Option[String] = None,
    @description("Pattern to match in the graph.")
    pattern: StandingQueryPattern,
    @description("Output workflows to process results.")
    @default(Seq.empty)
    outputs: Seq[StandingQueryResultWorkflowV2] = Seq.empty,
    @description("Whether or not to include cancellations in the results.")
    @default(false)
    includeCancellations: Boolean = false,
    @description("How many Standing Query results to buffer before backpressuring.")
    @default(32)
    inputBufferSize: Int = 32,
  )

  object StandingQueryDefinitionV2 {
    implicit val encoder: Encoder[StandingQueryDefinitionV2] = deriveConfiguredEncoder
    implicit val decoder: Decoder[StandingQueryDefinitionV2] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[StandingQueryDefinitionV2] = Schema.derived
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Status Query (same as V1)
  // ─────────────────────────────────────────────────────────────────────────────

  @title("Status Query")
  @description("A Cypher query to be run periodically while Recipe is running.")
  final case class StatusQueryV2(
    @description("Cypher query to execute periodically.")
    cypherQuery: String,
  )

  object StatusQueryV2 {
    implicit val encoder: Encoder[StatusQueryV2] = deriveConfiguredEncoder
    implicit val decoder: Decoder[StatusQueryV2] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[StatusQueryV2] = Schema.derived
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Main Recipe V2 Case Class
  // ─────────────────────────────────────────────────────────────────────────────

  @title("Quine Recipe V2")
  @description("A specification of a Quine Recipe using V2 API structure.")
  final case class Recipe(
    @description("Schema version (must be 2 for V2 recipes).")
    @default(currentVersion)
    version: Int = currentVersion,
    @description("Identifies the Recipe but is not necessarily unique.")
    title: String,
    @description("URL to social profile of the person or organization responsible for this Recipe.")
    contributor: Option[String] = None,
    @description("Brief description of this Recipe.")
    summary: Option[String] = None,
    @description("Longer form description of this Recipe.")
    description: Option[String] = None,
    @description("URL to image asset for this Recipe.")
    iconImage: Option[String] = None,
    @description("Ingest streams that load data into the graph.")
    @default(List.empty)
    ingestStreams: List[IngestStreamV2] = List.empty,
    @description("Standing queries that respond to graph updates.")
    @default(List.empty)
    standingQueries: List[StandingQueryDefinitionV2] = List.empty,
    @description("Node appearance customization for the web UI.")
    @default(List.empty)
    nodeAppearances: List[UiNodeAppearance] = List.empty,
    @description("Quick queries for the web UI context menu.")
    @default(List.empty)
    quickQueries: List[UiNodeQuickQuery] = List.empty,
    @description("Sample queries for the web UI dropdown.")
    @default(List.empty)
    sampleQueries: List[SampleQuery] = List.empty,
    @description("Cypher query to be run periodically while Recipe is running.")
    statusQuery: Option[StatusQueryV2] = None,
  ) {
    def isVersion(testVersion: Int): Boolean = version == testVersion
  }

  object Recipe {
    implicit val encoder: Encoder[Recipe] = deriveConfiguredEncoder
    implicit val decoder: Decoder[Recipe] = deriveConfiguredDecoder
    implicit lazy val schema: Schema[Recipe] = Schema.derived
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Variable Substitution
  // ─────────────────────────────────────────────────────────────────────────────

  /** Error for missing recipe variable */
  final case class UnboundVariableError(name: String)

  /** Apply variable substitution to a string.
    * If the string starts with '$', treat it as a variable reference.
    * '$$' escapes to a single '$'.
    */
  def applySubstitution(input: String, values: Map[String, String]): ValidatedNel[UnboundVariableError, String] =
    if (input.startsWith("$")) {
      val key = input.slice(1, input.length)
      if (input.startsWith("$$"))
        cats.data.Validated.valid(key)
      else
        values.get(key).toValidNel(UnboundVariableError(key))
    } else {
      cats.data.Validated.valid(input)
    }

  /** Apply substitutions to all relevant fields in a V2 recipe.
    * This includes paths, URLs, and other configurable strings.
    */
  def applySubstitutions(recipe: Recipe, values: Map[String, String]): ValidatedNel[UnboundVariableError, Recipe] = {
    import cats.data.Validated

    implicit class Subs(s: String) {
      def subs: ValidatedNel[UnboundVariableError, String] = applySubstitution(s, values)
    }

    implicit class SubSecret(s: Secret) {
      import Secret.Unsafe._
      def subs: ValidatedNel[UnboundVariableError, Secret] =
        applySubstitution(s.unsafeValue, values).map(Secret.apply)
    }

    implicit class SubCreds(c: AwsCredentials) {
      def subs: ValidatedNel[UnboundVariableError, AwsCredentials] =
        (c.accessKeyId.subs, c.secretAccessKey.subs).mapN(AwsCredentials(_, _))
    }

    implicit class SubRegion(r: AwsRegion) {
      def subs: ValidatedNel[UnboundVariableError, AwsRegion] =
        r.region.subs.map(AwsRegion(_))
    }

    // Substitute in ingest sources
    def substituteIngestSource(source: IngestSource): ValidatedNel[UnboundVariableError, IngestSource] =
      source match {
        case f: IngestSource.File =>
          f.path.subs.map(p => f.copy(path = p))
        case k: IngestSource.Kafka =>
          k.bootstrapServers.subs.map(bs => k.copy(bootstrapServers = bs))
        case s: IngestSource.S3 =>
          (s.bucket.subs, s.key.subs, s.credentials.traverse(_.subs)).mapN((b, k, c) =>
            s.copy(bucket = b, key = k, credentials = c),
          )
        case sse: IngestSource.ServerSentEvent =>
          sse.url.subs.map(u => sse.copy(url = u))
        case sqs: IngestSource.SQS =>
          (sqs.queueUrl.subs, sqs.credentials.traverse(_.subs), sqs.region.traverse(_.subs)).mapN((q, c, r) =>
            sqs.copy(queueUrl = q, credentials = c, region = r),
          )
        case ws: IngestSource.WebsocketClient =>
          (ws.url.subs, ws.initMessages.toList.traverse(_.subs)).mapN((u, m) => ws.copy(url = u, initMessages = m))
        case kin: IngestSource.Kinesis =>
          (kin.streamName.subs, kin.credentials.traverse(_.subs), kin.region.traverse(_.subs)).mapN((s, c, r) =>
            kin.copy(streamName = s, credentials = c, region = r),
          )
        case kcl: IngestSource.KinesisKCL =>
          (kcl.kinesisStreamName.subs, kcl.credentials.traverse(_.subs), kcl.region.traverse(_.subs)).mapN((s, c, r) =>
            kcl.copy(kinesisStreamName = s, credentials = c, region = r),
          )
        case other => Validated.valid(other)
      }

    // Substitute in destination steps
    def substituteDestination(dest: QuineDestinationSteps): ValidatedNel[UnboundVariableError, QuineDestinationSteps] =
      dest match {
        case f: QuineDestinationSteps.File =>
          f.path.subs.map(p => f.copy(path = p))
        case h: QuineDestinationSteps.HttpEndpoint =>
          h.url.subs.map(u => h.copy(url = u))
        case k: QuineDestinationSteps.Kafka =>
          (k.topic.subs, k.bootstrapServers.subs).mapN((t, bs) => k.copy(topic = t, bootstrapServers = bs))
        case kin: QuineDestinationSteps.Kinesis =>
          (kin.streamName.subs, kin.credentials.traverse(_.subs), kin.region.traverse(_.subs)).mapN((s, c, r) =>
            kin.copy(streamName = s, credentials = c, region = r),
          )
        case sns: QuineDestinationSteps.SNS =>
          (sns.topic.subs, sns.credentials.traverse(_.subs), sns.region.traverse(_.subs)).mapN((t, c, r) =>
            sns.copy(topic = t, credentials = c, region = r),
          )
        case cq: QuineDestinationSteps.CypherQuery =>
          cq.query.subs.map(q => cq.copy(query = q))
        case sl: QuineDestinationSteps.Slack =>
          sl.hookUrl.subs.map(u => sl.copy(hookUrl = u))
        case other => Validated.valid(other)
      }

    // Substitute in workflows
    def substituteWorkflow(
      wf: StandingQueryResultWorkflowV2,
    ): ValidatedNel[UnboundVariableError, StandingQueryResultWorkflowV2] = {
      val enrichmentSubs = wf.resultEnrichment.traverse(e => e.query.subs.map(q => e.copy(query = q)))
      val destsSubs = wf.destinations.traverse(substituteDestination)
      (enrichmentSubs, destsSubs).mapN((e, d) => wf.copy(resultEnrichment = e, destinations = d))
    }

    // Substitute in ingest streams
    def substituteIngest(ingest: IngestStreamV2): ValidatedNel[UnboundVariableError, IngestStreamV2] =
      (substituteIngestSource(ingest.source), ingest.query.subs).mapN((s, q) => ingest.copy(source = s, query = q))

    // Substitute in standing queries
    def substituteSQ(sq: StandingQueryDefinitionV2): ValidatedNel[UnboundVariableError, StandingQueryDefinitionV2] =
      sq.outputs.toList.traverse(substituteWorkflow).map(wfs => sq.copy(outputs = wfs))

    // Apply all substitutions
    (
      recipe.ingestStreams.traverse(substituteIngest),
      recipe.standingQueries.traverse(substituteSQ),
    ).mapN((iss, sqs) => recipe.copy(ingestStreams = iss, standingQueries = sqs))
  }
}
