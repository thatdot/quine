package com.thatdot.quine.app.v2api.endpoints

import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

import com.thatdot.api.v2.outputs.OutputFormat
import com.thatdot.api.v2.schema.V2ApiSchemas
import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.Predicate.OnlyPositiveMatch
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.{
  RegisteredStandingQuery,
  StandingQueryDefinition,
}
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultTransformation.InlineData
import com.thatdot.quine.app.v2api.definitions.query.standing.{
  Predicate,
  StandingQueryPattern,
  StandingQueryResultTransformation,
  StandingQueryResultWorkflow,
  StandingQueryStats,
}

trait V2StandingApiSchemas extends V2ApiSchemas {

  // Standing Query Mode codec, hand-rolled for [some reason]
  private val sqModesMap: Map[String, StandingQueryMode] = StandingQueryMode.values.map(s => s.toString -> s).toMap
  implicit val sqModeEncoder: Encoder[StandingQueryMode] = Encoder.encodeString.contramap(_.toString)
  implicit val sqModeDecoder: Decoder[StandingQueryMode] = Decoder.decodeString.map(sqModesMap(_))
  implicit val sqModeSchema: Schema[StandingQueryMode] = Schema.derivedEnumeration.defaultStringBased

  // Schema for Standing Query Result Workflow list because `@encodedExample` does not work as expected
  val exampleStandingQueryResultWorkflowToStandardOut: StandingQueryResultWorkflow = StandingQueryResultWorkflow(
    name = "stdout-example",
    filter = Some(OnlyPositiveMatch),
    preEnrichmentTransformation = Some(InlineData),
    resultEnrichment = Some(QuineDestinationSteps.CypherQuery(QuineDestinationSteps.CypherQuery.exampleQuery)),
    destinations = NonEmptyList.one(QuineDestinationSteps.StandardOut),
  )
  val exampleStandingQueryResultWorkflows: Seq[StandingQueryResultWorkflow] = Seq(
    exampleStandingQueryResultWorkflowToStandardOut,
  )

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Explicit Schema definitions to avoid repeated automatic derivation (QU-2417)
  // (This may not be the ideal long-term solution, but it's low-hanging fruit for faster compilation).
  implicit lazy val predicateSchema: Schema[Predicate] = Schema.derived
  implicit lazy val standingQueryResultTransformationSchema: Schema[StandingQueryResultTransformation] = Schema.derived
  implicit lazy val outputFormatSchema: Schema[OutputFormat] = Schema.derived
  // Note: AwsCredentials/AwsRegion also defined in V2IngestApiSchemas
  implicit lazy val standingAwsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val standingAwsRegionSchema: Schema[AwsRegion] = Schema.derived
  implicit lazy val destinationCypherQuerySchema: Schema[QuineDestinationSteps.CypherQuery] = Schema.derived
  implicit lazy val quineDestinationStepsSchema: Schema[QuineDestinationSteps] = Schema.derived

  // Consider adding tapir-cats in the future
  implicit def nonEmptyListSchema[A](implicit inner: Schema[A]): Schema[NonEmptyList[A]] =
    Schema.schemaForIterable[A, List].map(list => NonEmptyList.fromList(list))(_.toList)

  implicit lazy val standingQueryResultWorkflowSchema: Schema[StandingQueryResultWorkflow] = Schema.derived
  implicit lazy val standingQueryPatternSchema: Schema[StandingQueryPattern] = Schema.derived
  implicit lazy val standingQueryStatsSchema: Schema[StandingQueryStats] = Schema.derived
  implicit lazy val standingQueryDefinitionSchema: Schema[StandingQueryDefinition] = Schema.derived
  implicit lazy val registeredStandingQuerySchema: Schema[RegisteredStandingQuery] = Schema.derived

}
