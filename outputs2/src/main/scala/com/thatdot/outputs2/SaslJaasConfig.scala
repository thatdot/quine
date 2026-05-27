package com.thatdot.outputs2

import com.thatdot.api.v2.JaasFormatter
import com.thatdot.common.logging.Log.AlwaysSafeLoggable
import com.thatdot.common.security.Secret

/** Internal SASL/JAAS configuration for Kafka authentication. */
sealed trait SaslJaasConfig

object SaslJaasConfig {

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

  /** Loggable instance that outputs JAAS format with redacted secrets. */
  implicit val loggable: AlwaysSafeLoggable[SaslJaasConfig] =
    formatJaasString(_, _ => "****")

  /** Convert to Kafka's JAAS config string format.
    *
    * Requires an unsafe access witness to extract the secret values.
    */
  def toJaasConfigString(config: SaslJaasConfig)(implicit ev: Secret.UnsafeAccess): String =
    formatJaasString(config, _.unsafeValue)
}

/** PLAIN authentication mechanism. */
final case class PlainLogin(
  username: String,
  password: Secret,
) extends SaslJaasConfig

/** SCRAM authentication mechanism. */
final case class ScramLogin(
  username: String,
  password: Secret,
) extends SaslJaasConfig

/** OAuth Bearer authentication mechanism. */
final case class OAuthBearerLogin(
  clientId: String,
  clientSecret: Secret,
  scope: Option[String] = None,
  tokenEndpointUrl: Option[String] = None,
) extends SaslJaasConfig

/** OAuth Bearer via `private_key_jwt` assertion. */
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
