package com.thatdot.quine.app.serialization

import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}

/** This exists to help tapir and endpoint4s play nicely together.
  * Both tapir and endpoint4s want to derive encoders and decoders, but this exists as
  * a trait that nether of them know about, so we can control how and where it is derived.
  * Once the v1 api and ingest are removed, this can be remove and replaced with a codec and an encoder and decoder
  */
trait EncoderDecoder[A] {
  def encoder: Encoder[A]
  def decoder: Decoder[A]
}

object EncoderDecoder {
  def ofEncodeDecode[A](implicit encode: Encoder[A], decode: Decoder[A]): EncoderDecoder[A] = new EncoderDecoder[A] {
    override def encoder: Encoder[A] = encode
    override def decoder: Decoder[A] = decode

  }
  def ofMap[K: KeyEncoder: KeyDecoder, V](implicit v: EncoderDecoder[V]): EncoderDecoder[Map[K, V]] =
    new EncoderDecoder[Map[K, V]] {

      override def encoder: Encoder[Map[K, V]] = Encoder.encodeMap(implicitly, v.encoder)
      override def decoder: Decoder[Map[K, V]] = Decoder.decodeMap(implicitly, v.decoder)

    }

  trait DeriveEndpoints4s extends endpoints4s.circe.JsonSchemas {

    implicit def ofJsonSchema[A](implicit jsonSchema: JsonSchema[A]): EncoderDecoder[A] =
      ofEncodeDecode(jsonSchema.encoder, jsonSchema.decoder)
  }
}
