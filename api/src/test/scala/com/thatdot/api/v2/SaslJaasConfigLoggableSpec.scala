package com.thatdot.api.v2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.security.Secret

/** Tests for [[SaslJaasConfig]] Loggable instance behavior.
  *
  * Verifies that:
  *   - Sensitive fields (password, clientSecret) are redacted as "****" in logged output
  *   - Non-sensitive fields (username, clientId, scope, tokenEndpointUrl) are visible
  *   - The format matches the expected pattern for each subtype
  */
class SaslJaasConfigLoggableSpec extends AnyFunSuite with Matchers {

  import SaslJaasConfig.logSaslJaasConfig

  test("PlainLogin logs in JAAS format with username visible and password redacted") {
    val login = PlainLogin(username = "alice", password = Secret("jaas-queen"))
    val logged = logSaslJaasConfig.safe(login)

    logged shouldBe """org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="****";"""
  }

  test("ScramLogin logs in JAAS format with username visible and password redacted") {
    val login = ScramLogin(username = "bob", password = Secret("scram-secret"))
    val logged = logSaslJaasConfig.safe(login)

    logged shouldBe """org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="****";"""
  }

  test("OAuthBearerLogin logs in JAAS format with clientId visible and clientSecret redacted") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data"),
      tokenEndpointUrl = Some("https://auth.example.com/token"),
    )
    val logged = logSaslJaasConfig.safe(login)

    logged shouldBe """org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="my-client" clientSecret="****" scope="read:data" sasl.oauthbearer.token.endpoint.url="https://auth.example.com/token";"""
  }

  test("OAuthBearerLogin logs in JAAS format without optional fields when absent") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
    )
    val logged = logSaslJaasConfig.safe(login)

    logged shouldBe """org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="my-client" clientSecret="****";"""
  }

  test("PlainLogin password is indistinguishable regardless of actual value") {
    val login1 = PlainLogin(username = "alice", password = Secret("password1"))
    val login2 = PlainLogin(username = "alice", password = Secret("different-password"))

    val logged1 = logSaslJaasConfig.safe(login1)
    val logged2 = logSaslJaasConfig.safe(login2)

    logged1 shouldBe logged2
  }

  test("ScramLogin password is indistinguishable regardless of actual value") {
    val login1 = ScramLogin(username = "bob", password = Secret("password1"))
    val login2 = ScramLogin(username = "bob", password = Secret("different-password"))

    val logged1 = logSaslJaasConfig.safe(login1)
    val logged2 = logSaslJaasConfig.safe(login2)

    logged1 shouldBe logged2
  }

  test("OAuthBearerLogin clientSecret is indistinguishable regardless of actual value") {
    val login1 = OAuthBearerLogin(clientId = "client", clientSecret = Secret("secret1"))
    val login2 = OAuthBearerLogin(clientId = "client", clientSecret = Secret("different-secret"))

    val logged1 = logSaslJaasConfig.safe(login1)
    val logged2 = logSaslJaasConfig.safe(login2)

    logged1 shouldBe logged2
  }

  test("OAuthBearerAssertionLogin logs certFilePassword redacted while clientId, certFile, aliases, URIs are visible") {
    val login = OAuthBearerAssertionLogin(
      clientId = "CC-700025-G091167-434564-DEV",
      certFile = "/etc/quine/client.jks",
      certFilePassword = Secret("super-secret-keystore-pw"),
      certAlias = Some("alpha"),
      keyAlias = Some("beta"),
      resourceUri = "JPMC:URI:RS-102835-43568-gks-DEV",
      discoveryUrl = "https://idadg2.jpmorganchase.com/adfs/.well-known/openid-configuration",
    )
    val logged = logSaslJaasConfig.safe(login)

    logged should include("""certFilePassword="****"""")
    logged should not include "super-secret-keystore-pw"
    logged should include("""clientId="CC-700025-G091167-434564-DEV"""")
    logged should include("""certFile="/etc/quine/client.jks"""")
    logged should include("""certAlias="alpha"""")
    logged should include("""keyAlias="beta"""")
    logged should include("""resourceUri="JPMC:URI:RS-102835-43568-gks-DEV"""")
  }

  test("OAuthBearerAssertionLogin certFilePassword is indistinguishable regardless of actual value") {
    val login1 = OAuthBearerAssertionLogin(
      clientId = "c",
      certFile = "/x.jks",
      certFilePassword = Secret("password1"),
      resourceUri = "https://x",
      discoveryUrl = "https://x",
    )
    val login2 = login1.copy(certFilePassword = Secret("totally-different-password"))

    logSaslJaasConfig.safe(login1) shouldBe logSaslJaasConfig.safe(login2)
  }
}
