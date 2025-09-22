package com.thatdot.quine.app.v2api.endpoints

import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}

import com.thatdot.api.v2.schema.V2ApiSchemas
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.Predicate.OnlyPositiveMatch
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultTransformation.InlineData
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow

trait V2StandingApiSchemas extends V2ApiSchemas {

  /** `io.circe.generic.extras.auto._` appears to require a local reference to a
    * Configuration in order to find implicit Encoders through a mixed-in TapirJsonCirce
    * (here provided via V2ApiConfiguration), even though such
    * a Configuration is also available in a mixin or ancestor. Thus, a trait-named
    * config that refers to our standard config.
    */
  implicit val v2StandingApiSchemasConfig: Configuration = typeDiscriminatorConfig

  // Standing Query Mode codec, hand-rolled for [some reason]
  private val sqModesMap: Map[String, StandingQueryMode] = StandingQueryMode.values.map(s => s.toString -> s).toMap
  implicit val sqModeEncoder: Encoder[StandingQueryMode] = Encoder.encodeString.contramap(_.toString)
  implicit val sqModeDecoder: Decoder[StandingQueryMode] = Decoder.decodeString.map(sqModesMap(_))

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

}
