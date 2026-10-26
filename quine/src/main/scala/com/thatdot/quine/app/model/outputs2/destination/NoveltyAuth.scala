package com.thatdot.quine.app.model.outputs2.destination

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.codec.SecretCodecs
import com.thatdot.api.schema.SecretSchemas._
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.{circeConfig, tapirConfig}
import com.thatdot.common.security.Secret

/** How the native Novelty output destination authenticates to Novelty: its scoring connections, and
  * the cluster manager's calls that manage the output's ingest.
  *
  * A discriminated union over OAuth2 **client-authentication methods** (the grant is always
  * `client_credentials`). Generality is the point: supporting a client with a different auth method
  * is one new variant here plus one `match` case in the token acquirer — nothing else in the
  * destination changes. Mirrors the repo's `DeltaSharingAuth` shape. Enterprise-only.
  *
  * Lives at the model layer (not the v2api layer) so [[NoveltyStreamSettings]] can carry it to the
  * destination without the model depending on the API package.
  */
sealed trait NoveltyAuth
object NoveltyAuth {

  @title("Client Secret")
  @description(
    "OAuth2 client_credentials using a client id + secret posted to the IdP token endpoint — the common " +
    "machine-to-machine method (e.g. a Keycloak confidential client).",
  )
  final case class ClientSecret(
    @description("OAuth token endpoint, e.g. https://idp/realms/novelty/protocol/openid-connect/token.")
    tokenUrl: String,
    @description("OAuth client ID.")
    clientId: String,
    @description("OAuth client secret. Redacted in API responses.")
    clientSecret: Secret,
    @description("OAuth scope, if the IdP requires one.")
    scope: Option[String] = None,
  ) extends NoveltyAuth

  @title("Certificate")
  @description(
    "OAuth2 private_key_jwt: sign a JWT client assertion with a private key from a keystore and exchange " +
    "it for an access token (the same mechanism as Kafka OAuth / Delta Sharing certificate auth).",
  )
  final case class Certificate(
    @description("OAuth client ID registered with the IdP.")
    clientId: String,
    @description("Path to the keystore (JKS/PKCS12/PEM) with the client certificate and private key.")
    certFile: String,
    @description("Password for the keystore. Redacted in API responses.")
    certFilePassword: Secret,
    @description("Keystore format: 'PKCS12', 'JKS', or 'PEM'. Auto-detected if omitted.")
    certFileType: Option[String] = None,
    @description("Alias of the certificate entry in the keystore, if multiple exist.")
    certAlias: Option[String] = None,
    @description("Alias of the private key entry, if different from certAlias.")
    keyAlias: Option[String] = None,
    @description("Resource URI / audience for the token request (must match Novelty's access-token-audience).")
    resourceUri: String,
    @description("OIDC discovery URL, e.g. https://idp/realms/novelty/.well-known/openid-configuration.")
    discoveryUrl: String,
    @description("Path to a truststore for HTTPS calls to the IdP (private CA).")
    caCertPath: Option[String] = None,
    @description("Password for the CA truststore. Redacted in API responses.")
    caCertPassword: Option[Secret] = None,
  ) extends NoveltyAuth

  implicit lazy val schema: Schema[NoveltyAuth] = Schema.derived

  implicit val encoder: Encoder[NoveltyAuth] = {
    import SecretCodecs.secretEncoder // redacting — API responses never leak secrets
    deriveConfiguredEncoder
  }
  implicit val decoder: Decoder[NoveltyAuth] = {
    import SecretCodecs.secretDecoder
    deriveConfiguredDecoder
  }

  /** Encoder that preserves secret values, for persisting the SQ output. Requires the unsafe witness. */
  def preservingEncoder(implicit ev: Secret.UnsafeAccess): Encoder[NoveltyAuth] = {
    implicit val secretEncoder: Encoder[Secret] = SecretCodecs.preservingEncoder
    deriveConfiguredEncoder
  }
}
