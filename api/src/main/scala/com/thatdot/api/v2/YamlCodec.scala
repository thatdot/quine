package com.thatdot.api.v2

import io.circe._
import io.circe.syntax._
import sttp.model.MediaType
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}

case class YamlCodecFormat() extends CodecFormat {
  override val mediaType: MediaType = MediaType("application", "yaml")
}

object YamlCodec {

  def createCodec[T]()(implicit
    tSchema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): Codec[String, T, YamlCodecFormat] =
    new Codec[String, T, YamlCodecFormat] {
      override def rawDecode(s: String): DecodeResult[T] =
        yaml.parser.parse(s).flatMap(_.as[T]) match {
          case Left(fail: Error) => DecodeResult.Error(s, fail)
          case Right(t) => DecodeResult.Value[T](t)
        }

      override def encode(h: T): String = yaml.Printer(dropNullKeys = true).pretty(h.asJson)
      override def schema: Schema[T] = tSchema
      override def format = YamlCodecFormat()
    }

}
