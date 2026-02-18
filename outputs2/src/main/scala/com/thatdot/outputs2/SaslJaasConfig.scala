package com.thatdot.outputs2

import com.thatdot.common.logging.Log.AlwaysSafeLoggable
import com.thatdot.common.security.Secret

/** Internal SASL/JAAS configuration for Kafka authentication. */
sealed trait SaslJaasConfig

object SaslJaasConfig {

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
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$username" password="
          ${renderSecret(password)}";"""
    case ScramLogin(username, password) =>
      s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="
          ${renderSecret(password)}";"""
    case OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl) =>
      val scopePart = scope.map(s => s""" scope="$s"""").getOrElse("")
      val tokenUrlPart = tokenEndpointUrl.map(u => s""" sasl.oauthbearer.token.endpoint.url="$u"""").getOrElse("")
      s"""org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="$clientId" clientSecret="${renderSecret(
        clientSecret,
      )}"$scopePart$tokenUrlPart;"""
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
