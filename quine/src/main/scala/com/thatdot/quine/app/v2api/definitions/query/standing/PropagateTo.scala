package com.thatdot.quine.app.v2api.definitions.query.standing

import io.circe.{Decoder, Encoder}
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema, Validator}

sealed abstract class PropagateTo
object PropagateTo {
  case object None extends PropagateTo
  case object ExcludeSleeping extends PropagateTo
  case class IncludeSleeping(wakeupParallelism: Int = 4) extends PropagateTo

  private def toWire(p: PropagateTo): String = p match {
    case None => "NONE"
    case ExcludeSleeping => "EXCLUDE_SLEEPING"
    case IncludeSleeping(_) => "INCLUDE_SLEEPING"
  }

  private val wireNames: Map[String, PropagateTo] = Map(
    "NONE" -> None,
    "EXCLUDE_SLEEPING" -> ExcludeSleeping,
    "INCLUDE_SLEEPING" -> IncludeSleeping(),
  )

  implicit val encoder: Encoder[PropagateTo] =
    Encoder.encodeString.contramap(toWire)

  implicit val decoder: Decoder[PropagateTo] =
    Decoder.decodeString.emap(s => wireNames.get(s).toRight(s"Unknown enum value: $s"))

  implicit lazy val schema: Schema[PropagateTo] =
    Schema.string.validate(Validator.enumeration(wireNames.values.toList, v => Some(toWire(v))))

  implicit val plainCodec: Codec[String, PropagateTo, CodecFormat.TextPlain] =
    Codec.string.mapDecode { s =>
      wireNames.get(s) match {
        case Some(v) => DecodeResult.Value(v)
        case scala.None =>
          DecodeResult.Error(
            s,
            new IllegalArgumentException(
              s"Unknown propagateTo value: $s. Expected one of: ${wireNames.keys.mkString(", ")}",
            ),
          )
      }
    }(toWire(_))
}
