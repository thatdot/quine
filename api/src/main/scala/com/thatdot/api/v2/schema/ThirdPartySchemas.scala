package com.thatdot.api.v2.schema

import java.nio.charset.Charset
import java.time.{Instant, LocalTime}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList
import io.circe.Json
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema, Validator}

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

    /** A `NonEmptyList[A]` is, by construction, a non-empty array — the decoder already rejects an
      * empty or missing one. The schema says so: the `minSize(1)` validator publishes `minItems: 1`,
      * and the schema is not optional, so a `NonEmptyList` field is listed as `required` (tapir treats
      * every collection as optional by default; an `Option[NonEmptyList[A]]` field is still optional).
      * Docs, schema-driven form rendering, and runtime then agree. This only documents
      * already-enforced constraints, so it is not a behavioral change for API consumers.
      */
    implicit def nonEmptyListSchema[A](implicit inner: Schema[A]): Schema[NonEmptyList[A]] =
      Schema
        .schemaForIterable[A, List]
        .validate(Validator.minSize(1))
        .map(list => NonEmptyList.fromList(list))(_.toList)
        .copy(isOptional = false)
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

    implicit val localTimeCodec: Codec[String, LocalTime, TextPlain] = Codec.string.mapDecode(s =>
      Try(LocalTime.parse(s)) match {
        case Success(time) => DecodeResult.Value(time)
        case Failure(e) => DecodeResult.Error(s"Invalid time of day: $s", e)
      },
    )(_.toString)

    implicit lazy val localTimeSchema: Schema[LocalTime] = localTimeCodec.schema
      .description("ISO-8601 local time of day, \"HH:mm\" or \"HH:mm:ss\" (e.g. \"09:30\").")
      .encodedExample("09:30")
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
