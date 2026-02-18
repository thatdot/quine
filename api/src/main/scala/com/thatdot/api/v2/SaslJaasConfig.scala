package com.thatdot.api.v2

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

import com.thatdot.api.codec.SecretCodecs
import com.thatdot.api.codec.SecretCodecs._
import com.thatdot.api.schema.SecretSchemas._
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.common.logging.Log.AlwaysSafeLoggable
import com.thatdot.common.security.Secret

/** SASL/JAAS configuration for Kafka authentication.
  *
  * Represents the structured form of Kafka's `sasl.jaas.config` property. Each subtype
  * corresponds to a specific SASL mechanism supported by Kafka.
  *
  * @see [[https://kafka.apache.org/41/security/authentication-using-sasl Kafka SASL Authentication]]
  */
sealed trait SaslJaasConfig

object SaslJaasConfig {
  implicit val encoder: Encoder[SaslJaasConfig] = deriveConfiguredEncoder
  implicit val decoder: Decoder[SaslJaasConfig] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[SaslJaasConfig] = Schema.derived

  /** Encoder that preserves credential values for persistence.
    * Requires witness (`import Secret.Unsafe._`) to call.
    */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[SaslJaasConfig] = {
    // Shadow the redacting encoder with the preserving version
    implicit val secretEncoder: Encoder[Secret] = SecretCodecs.preservingEncoder
    // Derive encoders for subtypes that contain secrets
    implicit val plainLoginEncoder: Encoder[PlainLogin] = deriveConfiguredEncoder
    implicit val scramLoginEncoder: Encoder[ScramLogin] = deriveConfiguredEncoder
    implicit val oauthBearerLoginEncoder: Encoder[OAuthBearerLogin] = deriveConfiguredEncoder
    deriveConfiguredEncoder
  }

  /** Format a SASL/JAAS configuration as a Kafka JAAS config string.
    *
    * @param config
    *   the SASL/JAAS configuration to format
    * @param renderSecret
    *   function to render secret values (e.g., redact or expose)
    * @return
    *   a JAAS configuration string
    */
  private def formatJaasString(config: SaslJaasConfig, renderSecret: Secret => String): String = config match {
    case PlainLogin(username, password) =>
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$username" password="${renderSecret(
        password,
      )}";"""
    case ScramLogin(username, password) =>
      s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="${renderSecret(
        password,
      )}";"""
    case OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl) =>
      val scopePart = scope.map(s => s""" scope="$s"""").getOrElse("")
      val tokenUrlPart = tokenEndpointUrl.map(u => s""" sasl.oauthbearer.token.endpoint.url="$u"""").getOrElse("")
      s"""org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="$clientId" clientSecret="${renderSecret(
        clientSecret,
      )}"$scopePart$tokenUrlPart;"""
  }

  /** Loggable instance for SaslJaasConfig that outputs JAAS format with redacted secrets.
    *
    * Produces output in Kafka's native JAAS config string format, making logs directly
    * comparable to Kafka documentation and examples. Passwords and client secrets are
    * shown as "****".
    */
  implicit val logSaslJaasConfig: AlwaysSafeLoggable[SaslJaasConfig] =
    formatJaasString(_, _ => "****")

  /** Convert a SASL/JAAS configuration to Kafka's JAAS config string format.
    *
    * Requires an unsafe access witness to extract the secret values.
    *
    * @param config
    *   the SASL/JAAS configuration to convert
    * @param ev
    *   witness that the caller has acknowledged unsafe access to secrets
    * @return
    *   a JAAS configuration string suitable for Kafka's `sasl.jaas.config` property
    */
  def toJaasConfigString(config: SaslJaasConfig)(implicit ev: Secret.UnsafeAccess): String =
    formatJaasString(config, _.unsafeValue)
}

/** PLAIN authentication mechanism for Kafka SASL.
  *
  * Uses simple username/password authentication. The password is transmitted in cleartext
  * (though typically over TLS), so this mechanism should only be used with SSL/TLS encryption.
  *
  * Corresponds to Kafka's `org.apache.kafka.common.security.plain.PlainLoginModule`.
  *
  * @param username
  *   SASL username for authentication
  * @param password
  *   SASL password (redacted in API responses and logs)
  * @see [[https://kafka.apache.org/41/security/authentication-using-sasl/#authentication-using-saslplain Kafka SASL/PLAIN]]
  */
final case class PlainLogin(
  username: String,
  password: Secret,
) extends SaslJaasConfig

object PlainLogin {
  implicit val encoder: Encoder[PlainLogin] = deriveConfiguredEncoder
  implicit val decoder: Decoder[PlainLogin] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[PlainLogin] = Schema.derived
}

/** SCRAM (Salted Challenge Response Authentication Mechanism) for Kafka SASL.
  *
  * A more secure alternative to PLAIN that does not transmit the password in cleartext.
  * Kafka supports SCRAM-SHA-256 and SCRAM-SHA-512 variants.
  *
  * Corresponds to Kafka's `org.apache.kafka.common.security.scram.ScramLoginModule`.
  *
  * @param username
  *   SASL username for authentication
  * @param password
  *   SASL password (redacted in API responses and logs)
  * @see [[https://kafka.apache.org/41/security/authentication-using-sasl/#authentication-using-saslscram Kafka SASL/SCRAM]]
  */
final case class ScramLogin(
  username: String,
  password: Secret,
) extends SaslJaasConfig

object ScramLogin {
  implicit val encoder: Encoder[ScramLogin] = deriveConfiguredEncoder
  implicit val decoder: Decoder[ScramLogin] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[ScramLogin] = Schema.derived
}

/** OAuth Bearer authentication mechanism for Kafka SASL.
  *
  * Uses OAuth 2.0 client credentials flow to obtain access tokens for Kafka authentication.
  * The client authenticates with the OAuth provider using client ID and secret, then uses
  * the resulting token to authenticate with Kafka.
  *
  * Corresponds to Kafka's `org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule`.
  *
  * @param clientId
  *   OAuth 2.0 client identifier
  * @param clientSecret
  *   OAuth 2.0 client secret (redacted in API responses and logs)
  * @param scope
  *   Optional OAuth scope(s) to request
  * @param tokenEndpointUrl
  *   Optional OAuth token endpoint URL (if not using OIDC discovery)
  * @see [[https://kafka.apache.org/41/security/authentication-using-sasl/#authentication-using-sasloauthbearer Kafka SASL/OAUTHBEARER]]
  */
final case class OAuthBearerLogin(
  clientId: String,
  clientSecret: Secret,
  scope: Option[String] = None,
  tokenEndpointUrl: Option[String] = None,
) extends SaslJaasConfig

object OAuthBearerLogin {
  implicit val encoder: Encoder[OAuthBearerLogin] = deriveConfiguredEncoder
  implicit val decoder: Decoder[OAuthBearerLogin] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[OAuthBearerLogin] = Schema.derived
}
