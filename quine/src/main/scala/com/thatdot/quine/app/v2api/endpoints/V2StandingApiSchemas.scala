package com.thatdot.quine.app.v2api.endpoints

import cats.data.NonEmptyList
import io.circe.generic.extras.auto._
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.generic.auto._

import com.thatdot.api.v2.configuration.V2ApiConfiguration
import com.thatdot.api.v2.outputs.DestinationSteps
import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.Predicate.OnlyPositiveMatch
import com.thatdot.quine.app.v2api.definitions.query.standing.QuineSupportedDestinationSteps.CoreDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultTransform.InlineData
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow

trait V2StandingApiSchemas extends V2ApiConfiguration {

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

  // Schema for Standing Query Result Workflow map because `@encodedExample` does not work as expected
  val exampleStandingQueryResultWorkflowToStandardOut: StandingQueryResultWorkflow = StandingQueryResultWorkflow(
    filter = Some(OnlyPositiveMatch),
    preEnrichmentTransform = Some(InlineData),
    resultEnrichment = Some(QuineDestinationSteps.CypherQuery(QuineDestinationSteps.CypherQuery.exampleQuery)),
    destinations = NonEmptyList.one(CoreDestinationSteps(DestinationSteps.StandardOut)),
  )
  val exampleStandingQueryResultWorkflowMap: Map[String, StandingQueryResultWorkflow] = Map(
    "stdout" -> exampleStandingQueryResultWorkflowToStandardOut,
  )
  implicit lazy val stringStandingQueryResultWorkflowMapSchema: Schema[Map[String, StandingQueryResultWorkflow]] =
    Schema
      .schemaForMap[StandingQueryResultWorkflow]
      // Cannot get `.default` to work here
      .encodedExample(exampleStandingQueryResultWorkflowMap.asJson.deepDropNullValues)

  // Kafka Property Value codec, hand-rolled for like-a-String representation instead of as-an-Object with a field
  implicit val kafkaPropertyValueEncoder: Encoder[KafkaPropertyValue] = Encoder.encodeString.contramap(_.s)
  implicit val kafkaPropertyValueDecoder: Decoder[KafkaPropertyValue] =
    Decoder.decodeString.map(KafkaPropertyValue.apply)

  // Attempting schema for type alias to string without overriding all Map_String
  implicit class StringKafkaPropertyValue(s: String) {
    def toKafkaPropertyValue: KafkaPropertyValue = KafkaPropertyValue(s)
  }
  private val exampleKafkaProperties: Map[String, KafkaPropertyValue] = Map(
    "security.protocol" -> "SSL",
    "ssl.keystore.type" -> "PEM",
    "ssl.keystore.certificate.chain" -> "/path/to/file/containing/certificate/chain",
    "ssl.key.password" -> "private_key_password",
    "ssl.truststore.type" -> "PEM",
    "ssl.truststore.certificates" -> "/path/to/truststore/certificate",
  ).view.mapValues(_.toKafkaPropertyValue).toMap
  implicit lazy val stringKafkaPropertyMapSchema: Schema[Map[String, KafkaPropertyValue]] =
    Schema
      .schemaForMap[KafkaPropertyValue]
      // Cannot get `.default` to work here
      .encodedExample(exampleKafkaProperties.asJson)
}
