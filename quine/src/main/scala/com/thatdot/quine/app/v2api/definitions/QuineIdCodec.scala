package com.thatdot.quine.app.v2api.definitions

import io.circe.{Decoder, Encoder}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.QuineIdProvider

/** Circe codecs for QuineId, for reusability. These require a [[QuineIdProvider]]. */
trait QuineIdCodec {
  val idProvider: QuineIdProvider

  implicit val quineIdEncoder: Encoder[QuineId] = Encoder.encodeString.contramap(idProvider.qidToPrettyString)
  implicit val quineIdDecoder: Decoder[QuineId] = Decoder.decodeString.emap { str =>
    idProvider.qidFromPrettyString(str).toEither.left.map(_.getMessage)
  }
}
