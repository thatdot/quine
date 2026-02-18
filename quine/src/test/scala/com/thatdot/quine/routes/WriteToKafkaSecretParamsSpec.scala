package com.thatdot.quine.routes

import endpoints4s.circe.JsonSchemas
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.common.security.Secret
import com.thatdot.quine.routes.exts.CirceJsonAnySchema

class WriteToKafkaSecretParamsSpec extends AnyWordSpec with Matchers {

  private object TestSchemas extends StandingQuerySchemas with JsonSchemas with CirceJsonAnySchema

  "WriteToKafka schema encoding" should {
    "redact sslKeystorePassword in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        sslKeystorePassword = Some(Secret("keystore-secret-123")),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      json.hcursor.downField("sslKeystorePassword").as[String] shouldBe Right("Secret(****)")
    }

    "redact sslTruststorePassword in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        sslTruststorePassword = Some(Secret("truststore-secret-456")),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      json.hcursor.downField("sslTruststorePassword").as[String] shouldBe Right("Secret(****)")
    }

    "redact sslKeyPassword in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        sslKeyPassword = Some(Secret("key-secret-789")),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      json.hcursor.downField("sslKeyPassword").as[String] shouldBe Right("Secret(****)")
    }

    "redact saslJaasConfig PlainLogin password in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        saslJaasConfig = Some(SaslJaasConfig.PlainLogin("alice", Secret("plain-password"))),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      val jaasJson = json.hcursor.downField("saslJaasConfig")
      jaasJson.downField("username").as[String] shouldBe Right("alice")
      jaasJson.downField("password").as[String] shouldBe Right("Secret(****)")
    }

    "redact saslJaasConfig ScramLogin password in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        saslJaasConfig = Some(SaslJaasConfig.ScramLogin("bob", Secret("scram-password"))),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      val jaasJson = json.hcursor.downField("saslJaasConfig")
      jaasJson.downField("username").as[String] shouldBe Right("bob")
      jaasJson.downField("password").as[String] shouldBe Right("Secret(****)")
    }

    "redact saslJaasConfig OAuthBearerLogin clientSecret in JSON" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        saslJaasConfig = Some(
          SaslJaasConfig.OAuthBearerLogin(
            "client-id",
            Secret("client-secret"),
            Some("my-scope"),
            Some("https://auth.example.com/token"),
          ),
        ),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      val jaasJson = json.hcursor.downField("saslJaasConfig")
      jaasJson.downField("clientId").as[String] shouldBe Right("client-id")
      jaasJson.downField("clientSecret").as[String] shouldBe Right("Secret(****)")
      jaasJson.downField("scope").as[Option[String]] shouldBe Right(Some("my-scope"))
      jaasJson.downField("tokenEndpointUrl").as[Option[String]] shouldBe Right(Some("https://auth.example.com/token"))
    }
  }

  "WriteToKafka schema roundtrip" should {
    "decode secrets from JSON" in {
      import Secret.Unsafe._

      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
        sslKeystorePassword = Some(Secret("ks-pass")),
        sslTruststorePassword = Some(Secret("ts-pass")),
        sslKeyPassword = Some(Secret("key-pass")),
        saslJaasConfig = Some(SaslJaasConfig.PlainLogin("user", Secret("sasl-pass"))),
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      val decoded = TestSchemas.standingQueryResultOutputSchema.decoder
        .decodeJson(json)
        .getOrElse(fail("Failed to decode WriteToKafka"))

      decoded match {
        case k: StandingQueryResultOutputUserDef.WriteToKafka =>
          k.sslKeystorePassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
          k.sslTruststorePassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
          k.sslKeyPassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
          k.saslJaasConfig match {
            case Some(SaslJaasConfig.PlainLogin(username, password)) =>
              username shouldBe "user"
              password.unsafeValue shouldBe "Secret(****)"
            case other => fail(s"Expected PlainLogin but got: $other")
          }
        case other =>
          fail(s"Expected WriteToKafka but got: $other")
      }
    }

    "roundtrip with None secrets" in {
      val output: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKafka(
        topic = "test-topic",
        bootstrapServers = "localhost:9092",
      )

      val json = TestSchemas.standingQueryResultOutputSchema.encoder(output)
      val decoded = TestSchemas.standingQueryResultOutputSchema.decoder
        .decodeJson(json)
        .getOrElse(fail("Failed to decode WriteToKafka"))

      decoded match {
        case k: StandingQueryResultOutputUserDef.WriteToKafka =>
          k.sslKeystorePassword shouldBe None
          k.sslTruststorePassword shouldBe None
          k.sslKeyPassword shouldBe None
          k.saslJaasConfig shouldBe None
        case other =>
          fail(s"Expected WriteToKafka but got: $other")
      }
    }
  }
}
