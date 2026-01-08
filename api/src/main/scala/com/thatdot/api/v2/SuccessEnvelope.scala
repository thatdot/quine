package com.thatdot.api.v2

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

sealed trait SuccessEnvelope[+Content]
sealed trait CreatedOrNoContent[+Content] extends SuccessEnvelope[Content]
sealed trait CreatedOrOk[+Content] extends SuccessEnvelope[Content]
object SuccessEnvelope {

  implicit private val defaultConfig: Configuration = Configuration.default.withDefaults

  case class Ok[Content](content: Content, message: Option[String] = None, warnings: List[String] = Nil)
      extends SuccessEnvelope[Content]
      with CreatedOrOk[Content]
  object Ok {
    implicit def schema[A](implicit inner: Schema[A]): Schema[Ok[A]] = Schema.derived[Ok[A]]
    implicit def encoder[A: Encoder]: Encoder[Ok[A]] = deriveConfiguredEncoder
    implicit def decoder[A: Decoder]: Decoder[Ok[A]] = deriveConfiguredDecoder
  }

  case class Created[Content](content: Content, message: Option[String] = None, warnings: List[String] = Nil)
      extends SuccessEnvelope[Content]
      with CreatedOrNoContent[Content]
      with CreatedOrOk[Content]
  object Created {
    implicit def schema[A](implicit inner: Schema[A]): Schema[Created[A]] = Schema.derived[Created[A]]
    implicit def encoder[A: Encoder]: Encoder[Created[A]] = deriveConfiguredEncoder
    implicit def decoder[A: Decoder]: Decoder[Created[A]] = deriveConfiguredDecoder
  }

  case class Accepted(
    message: String = "Request accepted. Starting to process task.",
    monitorUrl: Option[String] = None,
  ) extends SuccessEnvelope[Nothing]
  object Accepted {
    implicit val schema: Schema[Accepted] = Schema.derived[Accepted]
    implicit val encoder: Encoder[Accepted] = deriveConfiguredEncoder
    implicit val decoder: Decoder[Accepted] = deriveConfiguredDecoder
  }

  case object NoContent extends SuccessEnvelope[Nothing] with CreatedOrNoContent[Nothing] {
    implicit val schema: Schema[NoContent.type] = Schema.derived[NoContent.type]
    implicit val encoder: Encoder[NoContent.type] = Encoder.encodeUnit.contramap(_ => ())
    implicit val decoder: Decoder[NoContent.type] = Decoder.decodeUnit.map(_ => NoContent)
  }
}
