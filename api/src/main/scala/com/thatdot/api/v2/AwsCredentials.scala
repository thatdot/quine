package com.thatdot.api.v2

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

import com.thatdot.api.codec.SecretCodecs
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.common.security.Secret

@title("AWS Credentials")
@description(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.",
)
final case class AwsCredentials(
  @encodedExample("ATIAXNKBTSB57V2QF11X")
  accessKeyId: Secret,
  @encodedExample("MDwbQe5XT4uOA3jQB/FhPaZpJdFkW13ryAL29bAk")
  secretAccessKey: Secret,
)

object AwsCredentials {
  import com.thatdot.api.codec.SecretCodecs.{secretEncoder, secretDecoder}

  /** Encoder that redacts credential values for API responses. */
  implicit val encoder: Encoder[AwsCredentials] = deriveConfiguredEncoder
  implicit val decoder: Decoder[AwsCredentials] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[AwsCredentials] = {
    import com.thatdot.api.schema.SecretSchemas.secretSchema
    Schema.derived
  }

  /** Encoder that preserves credential values for persistence.
    * Requires witness (`import Secret.Unsafe._`) to call.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[AwsCredentials] =
    PreservingCodecs.encoder
}

/** Separate object to avoid implicit scope pollution. */
private object PreservingCodecs {
  def encoder(implicit ev: Secret.UnsafeAccess): Encoder[AwsCredentials] = {
    // Shadow the redacting encoder with the preserving version
    implicit val secretEncoder: Encoder[Secret] = SecretCodecs.preservingEncoder
    deriveConfiguredEncoder
  }
}
