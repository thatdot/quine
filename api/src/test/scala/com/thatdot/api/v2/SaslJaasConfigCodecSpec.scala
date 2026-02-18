package com.thatdot.api.v2

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.security.Secret

/** Tests for [[SaslJaasConfig]] Circe codec behavior.
  *
  * Verifies that:
  *   - Secret fields (password, clientSecret) are redacted in JSON output
  *   - Non-sensitive fields (username, clientId) are NOT redacted
  *   - Decoder can reconstruct case classes from JSON
  */
class SaslJaasConfigCodecSpec extends AnyFunSuite with Matchers {

  test("PlainLogin encoder redacts password") {
    val login = PlainLogin(username = "alice", password = Secret("test-pw"))
    val json = login.asJson

    json.hcursor.get[String]("password") shouldBe Right("Secret(****)")
  }

  test("PlainLogin encoder does NOT redact username") {
    val login = PlainLogin(username = "alice", password = Secret("test-pw"))
    val json = login.asJson

    json.hcursor.get[String]("username") shouldBe Right("alice")
  }

  test("PlainLogin decoder reconstructs from JSON") {
    import Secret.Unsafe._
    val json = io.circe.parser
      .parse("""{"username": "alice", "password": "test-pw"}""")
      .getOrElse(fail("Failed to parse JSON"))

    val decoded = json.as[PlainLogin].getOrElse(fail("Failed to decode PlainLogin"))

    decoded.username shouldBe "alice"
    decoded.password.unsafeValue shouldBe "test-pw"
  }

  test("ScramLogin encoder redacts password") {
    val login = ScramLogin(username = "bob", password = Secret("secret123"))
    val json = login.asJson

    json.hcursor.get[String]("password") shouldBe Right("Secret(****)")
  }

  test("ScramLogin encoder does NOT redact username") {
    val login = ScramLogin(username = "bob", password = Secret("secret123"))
    val json = login.asJson

    json.hcursor.get[String]("username") shouldBe Right("bob")
  }

  test("ScramLogin decoder reconstructs from JSON") {
    import Secret.Unsafe._
    val json = io.circe.parser
      .parse("""{"username": "bob", "password": "secret123"}""")
      .getOrElse(fail("Failed to parse JSON"))

    val decoded = json.as[ScramLogin].getOrElse(fail("Failed to decode ScramLogin"))

    decoded.username shouldBe "bob"
    decoded.password.unsafeValue shouldBe "secret123"
  }

  test("OAuthBearerLogin encoder redacts clientSecret") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data"),
      tokenEndpointUrl = Some("https://auth.example.com/token"),
    )
    val json = login.asJson

    json.hcursor.get[String]("clientSecret") shouldBe Right("Secret(****)")
  }

  test("OAuthBearerLogin encoder does NOT redact clientId") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data"),
      tokenEndpointUrl = Some("https://auth.example.com/token"),
    )
    val json = login.asJson

    json.hcursor.get[String]("clientId") shouldBe Right("my-client")
  }

  test("OAuthBearerLogin encoder does NOT redact scope") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data"),
      tokenEndpointUrl = None,
    )
    val json = login.asJson

    json.hcursor.get[Option[String]]("scope") shouldBe Right(Some("read:data"))
  }

  test("OAuthBearerLogin encoder does NOT redact tokenEndpointUrl") {
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = None,
      tokenEndpointUrl = Some("https://auth.example.com/token"),
    )
    val json = login.asJson

    json.hcursor.get[Option[String]]("tokenEndpointUrl") shouldBe Right(Some("https://auth.example.com/token"))
  }

  test("OAuthBearerLogin decoder reconstructs from JSON with all fields") {
    import Secret.Unsafe._
    val json = io.circe.parser
      .parse(
        """{"clientId": "my-client", "clientSecret": "oauth-secret", "scope": "read:data", "tokenEndpointUrl": "https://auth.example.com/token"}""",
      )
      .getOrElse(fail("Failed to parse JSON"))

    val decoded = json.as[OAuthBearerLogin].getOrElse(fail("Failed to decode OAuthBearerLogin"))

    decoded.clientId shouldBe "my-client"
    decoded.clientSecret.unsafeValue shouldBe "oauth-secret"
    decoded.scope shouldBe Some("read:data")
    decoded.tokenEndpointUrl shouldBe Some("https://auth.example.com/token")
  }

  test("OAuthBearerLogin decoder applies defaults for optional fields") {
    import Secret.Unsafe._
    val json = io.circe.parser
      .parse("""{"clientId": "my-client", "clientSecret": "oauth-secret"}""")
      .getOrElse(fail("Failed to parse JSON"))

    val decoded = json.as[OAuthBearerLogin].getOrElse(fail("Failed to decode OAuthBearerLogin"))

    decoded.clientId shouldBe "my-client"
    decoded.clientSecret.unsafeValue shouldBe "oauth-secret"
    decoded.scope shouldBe None
    decoded.tokenEndpointUrl shouldBe None
  }

  test("SaslJaasConfig sealed trait encodes with type discriminator") {
    val plain: SaslJaasConfig = PlainLogin(username = "alice", password = Secret("pw"))
    val scram: SaslJaasConfig = ScramLogin(username = "bob", password = Secret("pw"))
    val oauth: SaslJaasConfig = OAuthBearerLogin(clientId = "client", clientSecret = Secret("secret"))

    plain.asJson.hcursor.get[String]("type") shouldBe Right("PlainLogin")
    scram.asJson.hcursor.get[String]("type") shouldBe Right("ScramLogin")
    oauth.asJson.hcursor.get[String]("type") shouldBe Right("OAuthBearerLogin")
  }

  test("SaslJaasConfig decoder routes to correct subtype via type discriminator") {
    import Secret.Unsafe._
    val plainJson = io.circe.parser
      .parse("""{"type": "PlainLogin", "username": "alice", "password": "pw"}""")
      .getOrElse(fail("Failed to parse JSON"))

    val decoded = plainJson.as[SaslJaasConfig].getOrElse(fail("Failed to decode SaslJaasConfig"))

    decoded shouldBe a[PlainLogin]
    val plain = decoded.asInstanceOf[PlainLogin]
    plain.username shouldBe "alice"
    plain.password.unsafeValue shouldBe "pw"
  }

  test("toJaasConfigString produces PlainLoginModule JAAS string for PlainLogin") {
    import Secret.Unsafe._
    val login = PlainLogin(username = "alice", password = Secret("my-password"))
    val jaasString = SaslJaasConfig.toJaasConfigString(login)

    jaasString shouldBe """org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="my-password";"""
  }

  test("toJaasConfigString produces ScramLoginModule JAAS string for ScramLogin") {
    import Secret.Unsafe._
    val login = ScramLogin(username = "bob", password = Secret("scram-secret"))
    val jaasString = SaslJaasConfig.toJaasConfigString(login)

    jaasString shouldBe """org.apache.kafka.common.security.scram.ScramLoginModule required username="bob" password="scram-secret";"""
  }

  test("toJaasConfigString produces OAuthBearerLoginModule JAAS string for OAuthBearerLogin") {
    import Secret.Unsafe._
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
    )
    val jaasString = SaslJaasConfig.toJaasConfigString(login)

    jaasString should include("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required")
    jaasString should include("""clientId="my-client"""")
    jaasString should include("""clientSecret="oauth-secret"""")
    jaasString should endWith(";")
  }

  test("toJaasConfigString includes scope in OAuthBearerLogin JAAS string when present") {
    import Secret.Unsafe._
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data write:data"),
    )
    val jaasString = SaslJaasConfig.toJaasConfigString(login)

    jaasString should include("""scope="read:data write:data"""")
  }

  test("toJaasConfigString includes tokenEndpointUrl in OAuthBearerLogin JAAS string when present") {
    import Secret.Unsafe._
    val login = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      tokenEndpointUrl = Some("https://auth.example.com/token"),
    )
    val jaasString = SaslJaasConfig.toJaasConfigString(login)

    jaasString should include("""sasl.oauthbearer.token.endpoint.url="https://auth.example.com/token"""")
  }

  test("preservingEncoder preserves PlainLogin password") {
    import Secret.Unsafe._
    val login: SaslJaasConfig = PlainLogin(username = "alice", password = Secret("real-password"))
    val encoder = SaslJaasConfig.preservingEncoder
    val json = encoder(login)

    json.hcursor.get[String]("password") shouldBe Right("real-password")
    json.hcursor.get[String]("username") shouldBe Right("alice")
  }

  test("preservingEncoder preserves ScramLogin password") {
    import Secret.Unsafe._
    val login: SaslJaasConfig = ScramLogin(username = "bob", password = Secret("scram-secret"))
    val encoder = SaslJaasConfig.preservingEncoder
    val json = encoder(login)

    json.hcursor.get[String]("password") shouldBe Right("scram-secret")
    json.hcursor.get[String]("username") shouldBe Right("bob")
  }

  test("preservingEncoder preserves OAuthBearerLogin clientSecret") {
    import Secret.Unsafe._
    val login: SaslJaasConfig = OAuthBearerLogin(
      clientId = "my-client",
      clientSecret = Secret("oauth-secret"),
      scope = Some("read:data"),
    )
    val encoder = SaslJaasConfig.preservingEncoder
    val json = encoder(login)

    json.hcursor.get[String]("clientSecret") shouldBe Right("oauth-secret")
    json.hcursor.get[String]("clientId") shouldBe Right("my-client")
    json.hcursor.get[Option[String]]("scope") shouldBe Right(Some("read:data"))
  }

  test("preservingEncoder includes type discriminator") {
    import Secret.Unsafe._
    val plain: SaslJaasConfig = PlainLogin(username = "alice", password = Secret("pw"))
    val scram: SaslJaasConfig = ScramLogin(username = "bob", password = Secret("pw"))
    val oauth: SaslJaasConfig = OAuthBearerLogin(clientId = "client", clientSecret = Secret("secret"))
    val encoder = SaslJaasConfig.preservingEncoder

    encoder(plain).hcursor.get[String]("type") shouldBe Right("PlainLogin")
    encoder(scram).hcursor.get[String]("type") shouldBe Right("ScramLogin")
    encoder(oauth).hcursor.get[String]("type") shouldBe Right("OAuthBearerLogin")
  }
}
