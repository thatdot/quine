package com.thatdot.api.v2.codec

import java.nio.charset.Charset
import java.time.Instant

import scala.util.Try

import io.circe.{Decoder, Encoder}

/** Circe codecs for third-party types that cannot have implicits in their companion objects.
  *
  * Usage:
  * {{{
  * import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk._
  * }}}
  *
  * @see [[com.thatdot.api.v2.schema.ThirdPartySchemas]] for Tapir schemas (OpenAPI documentation)
  */
object ThirdPartyCodecs {

  /** Circe codecs for JDK types */
  object jdk {
    implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
    implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))

    implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap(_.toString)
    implicit val instantDecoder: Decoder[Instant] = Decoder.decodeString.emapTry(s => Try(Instant.parse(s)))
  }
}
