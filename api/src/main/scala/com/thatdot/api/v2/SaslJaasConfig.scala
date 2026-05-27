package com.thatdot.api.v2

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

import com.thatdot.api.codec.SecretCodecs
import com.thatdot.api.codec.SecretCodecs._
import com.thatdot.api.schema.SecretSchemas._
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
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
    implicit val oauthBearerAssertionLoginEncoder: Encoder[OAuthBearerAssertionLogin] = deriveConfiguredEncoder
    deriveConfiguredEncoder
  }

  private val PlainModule = "org.apache.kafka.common.security.plain.PlainLoginModule"
  private val ScramModule = "org.apache.kafka.common.security.scram.ScramLoginModule"
  private val OAuthBearerModule = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"

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
      JaasFormatter.loginModule(PlainModule, Seq("username" -> username, "password" -> renderSecret(password)))
    case ScramLogin(username, password) =>
      JaasFormatter.loginModule(ScramModule, Seq("username" -> username, "password" -> renderSecret(password)))
    case OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl) =>
      JaasFormatter.loginModule(
        OAuthBearerModule,
        Seq("clientId" -> clientId, "clientSecret" -> renderSecret(clientSecret)) ++
        scope.map("scope" -> _) ++
        tokenEndpointUrl.map("sasl.oauthbearer.token.endpoint.url" -> _),
      )
    case a: OAuthBearerAssertionLogin =>
      JaasFormatter.loginModule(
        OAuthBearerModule,
        Seq(
          "clientId" -> a.clientId,
          "certFile" -> a.certFile,
          "certFilePassword" -> renderSecret(a.certFilePassword),
        ) ++
        a.certFileType.map("certFileType" -> _) ++
        a.certAlias.map("certAlias" -> _) ++
        a.keyAlias.map("keyAlias" -> _) ++
        Seq("resourceUri" -> a.resourceUri, "discoveryUrl" -> a.discoveryUrl) ++
        a.caCertPath.map("caCertPath" -> _) ++
        a.caCertPassword.map(s => "caCertPassword" -> renderSecret(s)),
      )
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

/** OAuth Bearer authentication via `private_key_jwt` client assertion (RFC 7521/7523).
  *
  * Used when the OAuth client is registered with a certificate rather than a shared secret
  * (e.g. JPMC IDAnywhere "Confidential Client" with X509). The Quine-provided callback
  * handler signs a JWT assertion with the private key from the supplied keystore and
  * exchanges it for a bearer token at the OIDC token endpoint.
  *
  * Requires `sasl.login.callback.handler.class` to be wired to
  * `com.thatdot.quine.auth.kafka.OAuthBearerAssertionLoginCallbackHandler`; this is emitted
  * automatically via the `KafkaSaslExtension` extension hook when the deployment is
  * Enterprise.
  *
  * @param clientId         OAuth client identifier registered in the IdP
  * @param certFile         Filesystem path to the JKS/PKCS12 keystore holding the client cert + private key
  * @param certFilePassword Keystore password (redacted in API responses and logs)
  * @param certFileType     Optional keystore format hint — usually `"PKCS12"` (the JDK 9+ default) or `"JKS"`.
  *                         If omitted, [[com.thatdot.quine.auth.oauth.X509Loader]] uses `KeyStore.getDefaultType`.
  * @param certAlias        Optional keystore alias for the certificate (first cert entry used if omitted)
  * @param keyAlias         Optional keystore alias for the private key (first key entry used if omitted)
  * @param resourceUri      IdP resource / audience to request the token for
  * @param discoveryUrl     OIDC discovery URL for the IdP (used to look up the token endpoint)
  * @param caCertPath       Optional filesystem path to a JKS/PKCS12 truststore the JVM should use when
  *                         making the HTTPS call to the OIDC token endpoint. If omitted, the JVM default
  *                         truststore applies (`-Djavax.net.ssl.trustStore=…`).
  * @param caCertPassword   Optional password for the truststore named by [[caCertPath]]
  */
final case class OAuthBearerAssertionLogin(
  clientId: String,
  certFile: String,
  certFilePassword: Secret,
  certFileType: Option[String] = None,
  certAlias: Option[String] = None,
  keyAlias: Option[String] = None,
  resourceUri: String,
  discoveryUrl: String,
  caCertPath: Option[String] = None,
  caCertPassword: Option[Secret] = None,
) extends SaslJaasConfig

object OAuthBearerAssertionLogin {
  implicit val encoder: Encoder[OAuthBearerAssertionLogin] = deriveConfiguredEncoder
  implicit val decoder: Decoder[OAuthBearerAssertionLogin] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[OAuthBearerAssertionLogin] = Schema.derived
}
