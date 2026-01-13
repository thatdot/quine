package com.thatdot.api.v2.schema

import java.nio.charset.Charset

import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema}

/** Schemas and codecs for third-party types that cannot have implicits in their companion objects.
  *
  * Usage:
  * {{{
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.cats._
  * import com.thatdot.api.v2.schema.ThirdPartySchemas.jdk._
  * }}}
  */
object ThirdPartySchemas {

  /** Schemas for cats data types */
  object cats {
    implicit def nonEmptyListSchema[A](implicit inner: Schema[A]): Schema[NonEmptyList[A]] =
      Schema.schemaForIterable[A, List].map(list => NonEmptyList.fromList(list))(_.toList)
  }

  /** Schemas and codecs for JDK types */
  object jdk {
    implicit val charsetCodec: Codec[String, Charset, TextPlain] = Codec.string.mapDecode(s =>
      Try(Charset.forName(s)) match {
        case Success(charset) => DecodeResult.Value(charset)
        case Failure(e) => DecodeResult.Error(s"Invalid charset: $s", e)
      },
    )(_.toString)

    implicit val charsetSchema: Schema[Charset] = charsetCodec.schema
    implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
    implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))
  }
}
