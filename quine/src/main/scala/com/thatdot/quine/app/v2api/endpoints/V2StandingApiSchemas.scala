package com.thatdot.quine.app.v2api.endpoints

import io.circe.{Decoder, Encoder}

import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode

trait V2StandingApiSchemas extends V2ApiConfiguration {

  private val sqModesMap: Map[String, StandingQueryMode] = StandingQueryMode.values.map(s => s.toString -> s).toMap
  implicit val sqModeEncoder: Encoder[StandingQueryMode] = Encoder.encodeString.contramap(_.toString)
  implicit val sqModeDecoder: Decoder[StandingQueryMode] = Decoder.decodeString.map(sqModesMap(_))

}
