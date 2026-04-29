package com.thatdot.api.v2

import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

/** Discriminator types for Tapir `oneOf` outputs that can return one of multiple
  * success status codes (e.g. an idempotent create that returns 201 with a body or
  * 204 empty when the resource already existed).
  *
  * Wire format is *not* a JSON envelope — `Created[T]` and `Ok[T]` serialize as `T`
  * directly, `NoContent` produces an empty body. The case classes exist purely as
  * Scala-level tags so `oneOfVariantFromMatchType` can dispatch.
  *
  * Endpoints that return only one success status code do **not** need these — use
  * the raw resource type directly as the output. Construction at the call site is
  * `Created(value)` etc., which adds zero JSON noise.
  */
sealed trait CreatedOrNoContent[+Content]
sealed trait CreatedOrOk[+Content]

final case class Created[+Content](content: Content) extends CreatedOrNoContent[Content] with CreatedOrOk[Content]
object Created {
  implicit def encoder[A](implicit enc: Encoder[A]): Encoder[Created[A]] = enc.contramap(_.content)
  implicit def decoder[A](implicit dec: Decoder[A]): Decoder[Created[A]] = dec.map(Created(_))
  implicit def schema[A](implicit inner: Schema[A]): Schema[Created[A]] =
    inner.map(a => Some(Created(a)))(_.content)
}

final case class Ok[+Content](content: Content) extends CreatedOrOk[Content]
object Ok {
  implicit def encoder[A](implicit enc: Encoder[A]): Encoder[Ok[A]] = enc.contramap(_.content)
  implicit def decoder[A](implicit dec: Decoder[A]): Decoder[Ok[A]] = dec.map(Ok(_))
  implicit def schema[A](implicit inner: Schema[A]): Schema[Ok[A]] =
    inner.map(a => Some(Ok(a)))(_.content)
}

case object NoContent extends CreatedOrNoContent[Nothing] {
  implicit lazy val schema: Schema[NoContent.type] = Schema.derived
  implicit val encoder: Encoder[NoContent.type] = Encoder.encodeUnit.contramap(_ => ())
  implicit val decoder: Decoder[NoContent.type] = Decoder.decodeUnit.map(_ => NoContent)
}
