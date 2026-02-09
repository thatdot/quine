package com.thatdot.api.codec

import io.circe.{Decoder, Encoder}

import com.thatdot.common.security.Secret

/** Circe codecs for [[Secret]] values. */
object SecretCodecs {

  /** Encoder that uses `Secret.toString` for redaction.
    * This is the default encoder and should be used for HTTP API responses.
    */
  implicit val secretEncoder: Encoder[Secret] = Encoder.encodeString.contramap(_.toString)

  /** Creates an encoder that preserves the actual value for persistence and cluster communication.
    * Requires a witness (`import Secret.Unsafe._`) to call, making the intent explicit.
    * WARNING: Only use this encoder for internal storage paths, never for external HTTP responses.
    * This method is intentionally NOT implicit to prevent accidental use in API contexts.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[Secret] =
    Encoder.encodeString.contramap(_.unsafeValue)

  /** Decoder that wraps incoming strings in a Secret. */
  implicit val secretDecoder: Decoder[Secret] = Decoder.decodeString.map(Secret(_))
}
