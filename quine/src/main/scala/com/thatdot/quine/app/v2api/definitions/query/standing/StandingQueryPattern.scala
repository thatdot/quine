package com.thatdot.quine.app.v2api.definitions.query.standing

import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema.annotations.{default, description, title}
import sttp.tapir.{Schema, Validator}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig

@title("Standing Query Pattern")
@description("A declarative structural graph pattern.")
sealed abstract class StandingQueryPattern
object StandingQueryPattern {
  import com.thatdot.quine.app.util.StringOps.syntax._

  implicit val encoder: Encoder[StandingQueryPattern] = deriveConfiguredEncoder
  implicit val decoder: Decoder[StandingQueryPattern] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[StandingQueryPattern] = Schema.derived

  @title("Cypher")
  final case class Cypher(
    @description(
      """Cypher query describing the Standing Query pattern. This must take the form of
        |`MATCH <pattern> WHERE <condition> RETURN <columns>`. When the `mode` is `DistinctId`,
        |the `RETURN` must also be `DISTINCT`.""".asOneLine,
    )
    query: String,
    @default(StandingQueryMode.DistinctId)
    mode: StandingQueryMode = StandingQueryMode.DistinctId,
  ) extends StandingQueryPattern

  sealed abstract class StandingQueryMode
  object StandingQueryMode {
    // DomainGraphBranch interpreter
    case object DistinctId extends StandingQueryMode
    // SQv4/Cypher interpreter
    case object MultipleValues extends StandingQueryMode

    // Not yet released. Still accepted by the encoder/decoder for internal use,
    // but intentionally excluded from `publicValues` so it does not appear in the OpenAPI schema.
    case object QuinePattern extends StandingQueryMode

    val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues, QuinePattern)
    private val publicValues: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues)

    implicit val encoder: Encoder[StandingQueryMode] = deriveEnumerationEncoder
    implicit val decoder: Decoder[StandingQueryMode] = deriveEnumerationDecoder
    implicit lazy val schema: Schema[StandingQueryMode] =
      Schema.string[StandingQueryMode].validate(Validator.enumeration(publicValues.toList, v => Some(v.toString)))
  }
}
