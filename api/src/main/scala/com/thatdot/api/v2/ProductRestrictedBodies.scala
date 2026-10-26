package com.thatdot.api.v2

import io.circe.{Decoder, Encoder}
import sttp.tapir.DecodeResult.Value
import sttp.tapir.{DecodeResult, EndpointIO, Schema}

/** Request bodies that refuse, as they are decoded, every field and variant that [[productVersion]] does not support
  * (see [[ProductSpecific]]). Each product's endpoint definitions mix this in; definitions whose bodies hold nothing
  * product-specific, such as authentication's, need not.
  */
trait ProductRestrictedBodies extends V2EndpointDefinitions {

  /** The product whose API these endpoints belong to. */
  protected def productVersion: ProductVersion

  override def jsonBody[T: Encoder: Decoder: Schema]: EndpointIO.Body[String, T] =
    restricted(super.jsonBody[T])

  override def yamlBody[T]()(implicit
    schema: Schema[T],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ): EndpointIO.Body[String, T] = restricted(super.yamlBody[T]())

  private def restricted[T: Schema](body: EndpointIO.Body[String, T]): EndpointIO.Body[String, T] =
    body.mapDecode { t =>
      ProductSpecific.validate(t, productVersion).fold[DecodeResult[T]](Value(t))(DecodeResult.Error("", _))
    }(identity)
}
