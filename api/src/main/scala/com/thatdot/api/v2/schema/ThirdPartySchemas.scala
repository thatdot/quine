package com.thatdot.api.v2.schema

import java.nio.charset.Charset
import java.time.Instant

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList
import io.circe.Json
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.api.v2.codec.DurationFormat

/** Tapir schemas for third-party types that cannot have implicits in their companion objects.
  *
  * Usage:
  * {{{
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.cats._
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.circe._
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.jdk._
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.scala._
  * }}}
  *
  * @see [[com.thatdot.api.v2.codec.ThirdPartyCodecs]] for Circe codecs (JSON serialization)
  */
object ThirdPartySchemas {

  /** Schemas for `cats` data types */
  object cats {
    implicit def nonEmptyListSchema[A](implicit inner: Schema[A]): Schema[NonEmptyList[A]] =
      Schema.schemaForIterable[A, List].map(list => NonEmptyList.fromList(list))(_.toList)
  }

  /** Schemas for Circe types */
  object circe {
    implicit lazy val jsonSchema: Schema[Json] = Schema.any[Json]
    implicit lazy val mapStringJsonSchema: Schema[Map[String, Json]] = Schema.schemaForMap[String, Json](identity)
    implicit lazy val seqJsonSchema: Schema[Seq[Json]] = jsonSchema.asIterable[Seq]
    implicit lazy val seqSeqJsonSchema: Schema[Seq[Seq[Json]]] = seqJsonSchema.asIterable[Seq]
  }

  /** Schemas for JDK types */
  object jdk {
    implicit val charsetCodec: Codec[String, Charset, TextPlain] = Codec.string.mapDecode(s =>
      Try(Charset.forName(s)) match {
        case Success(charset) => DecodeResult.Value(charset)
        case Failure(e) => DecodeResult.Error(s"Invalid charset: $s", e)
      },
    )(_.toString)

    implicit lazy val charsetSchema: Schema[Charset] = charsetCodec.schema

    implicit val instantCodec: Codec[String, Instant, TextPlain] = Codec.string.mapDecode(s =>
      Try(Instant.parse(s)) match {
        case Success(instant) => DecodeResult.Value(instant)
        case Failure(e) => DecodeResult.Error(s"Invalid instant: $s", e)
      },
    )(_.toString)

    implicit lazy val instantSchema: Schema[Instant] = instantCodec.schema
  }

  /** Schemas for Scala stdlib types */
  object scala {

    /** Tapir codec for Go-style duration strings (`"20s"`, `"500ms"`, `"1.5m"`, `"2h45m"`)
      * per AIP-142. Used for both URL-encoded query params and JSON body fields.
      */
    implicit val finiteDurationCodec: Codec[String, FiniteDuration, TextPlain] = Codec.string.mapDecode { s =>
      DurationFormat.parse(s) match {
        case Right(d) => DecodeResult.Value(d)
        case Left(msg) => DecodeResult.Error(s, new IllegalArgumentException(msg))
      }
    }(DurationFormat.render)

    implicit lazy val finiteDurationSchema: Schema[FiniteDuration] = finiteDurationCodec.schema
      .description(
        "AIP-142 duration string. One or more `<number><unit>` segments concatenated; " +
        "units are `ns`, `us`, `ms`, `s`, `m`, `h`. Examples: `20s`, `500ms`, `1.5m`, `2h45m`.",
      )
      .encodedExample("20s")
  }
}
