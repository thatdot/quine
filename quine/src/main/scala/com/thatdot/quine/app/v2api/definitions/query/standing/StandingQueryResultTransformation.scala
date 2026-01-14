package com.thatdot.quine.app.v2api.definitions.query.standing

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.description

import com.thatdot.api.v2.schema.V2ApiConfiguration._

sealed trait StandingQueryResultTransformation

object StandingQueryResultTransformation {
  import com.thatdot.quine.app.util.StringOps.syntax._

  implicit private val circeConfig: Configuration = typeDiscriminatorConfig.asCirce

  @description(
    """Extracts, or "lifts", the `data` field of a Standing Query Result such that the data is no longer wrapped,
      |but the root-level object. Assumes a Standing Query Result with a `data` field.""".asOneLine,
  )
  case object InlineData extends StandingQueryResultTransformation

  implicit val encoder: Encoder[StandingQueryResultTransformation] = deriveConfiguredEncoder
  implicit val decoder: Decoder[StandingQueryResultTransformation] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[StandingQueryResultTransformation] = Schema.derived
}
