package com.thatdot.quine.app.v2api.definitions.query.standing

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.description

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig

// TODO Consider lifting Quine and Novelty Predicates to shared modules
sealed trait Predicate

object Predicate {

  @description(
    "Returns `true` when a Standing Query Result's metadata includes a `true` value for the `isPositiveMatch` field.",
  )
  case object OnlyPositiveMatch extends Predicate

  implicit val encoder: Encoder[Predicate] = deriveConfiguredEncoder
  implicit val decoder: Decoder[Predicate] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[Predicate] = Schema.derived
}
