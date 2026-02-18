package com.thatdot.quine.app.model.ingest2.sources

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.{PlainLogin, SaslJaasConfig}
import com.thatdot.api.{v2 => api}
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.V1ToV2
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaSecurityProtocol, SaslJaasConfig => V1SaslJaasConfig}

class KafkaSourceSpec extends AnyFunSpec with Matchers {

  // Helper to create a minimal KafkaSource for testing effectiveProperties
  private def createKafkaSource(
    kafkaProperties: Map[String, String] = Map.empty,
    sslKeystorePassword: Option[Secret] = None,
    sslTruststorePassword: Option[Secret] = None,
    sslKeyPassword: Option[Secret] = None,
    saslJaasConfig: Option[SaslJaasConfig] = None,
  ): KafkaSource = KafkaSource(
    topics = Left(Set("test-topic")),
    bootstrapServers = "localhost:9092",
    groupId = "test-group",
    securityProtocol = KafkaSecurityProtocol.PlainText,
    maybeExplicitCommit = None,
    autoOffsetReset = KafkaAutoOffsetReset.Latest,
    kafkaProperties = kafkaProperties,
    endingOffset = None,
    decoders = Seq.empty,
    meter = null,
    system = null,
    sslKeystorePassword = sslKeystorePassword,
    sslTruststorePassword = sslTruststorePassword,
    sslKeyPassword = sslKeyPassword,
    saslJaasConfig = saslJaasConfig,
  )

  describe("KafkaSource.effectiveProperties") {

    it("merges sslKeystorePassword into effective properties") {
      val source = createKafkaSource(sslKeystorePassword = Some(Secret("my-keystore-pass")))

      source.effectiveProperties.get("ssl.keystore.password") shouldBe Some("my-keystore-pass")
    }

    it("merges sslTruststorePassword into effective properties") {
      val source = createKafkaSource(sslTruststorePassword = Some(Secret("my-truststore-pass")))

      source.effectiveProperties.get("ssl.truststore.password") shouldBe Some("my-truststore-pass")
    }

    it("merges sslKeyPassword into effective properties") {
      val source = createKafkaSource(sslKeyPassword = Some(Secret("my-key-pass")))

      source.effectiveProperties.get("ssl.key.password") shouldBe Some("my-key-pass")
    }

    it("merges saslJaasConfig into effective properties as JAAS config string") {
      val source = createKafkaSource(saslJaasConfig = Some(PlainLogin("alice", Secret("sasl-password"))))

      val jaasConfig = source.effectiveProperties.get("sasl.jaas.config")

      jaasConfig shouldBe defined
      jaasConfig.get should include("PlainLoginModule")
      jaasConfig.get should include("alice")
      jaasConfig.get should include("sasl-password")
    }

    it("preserves existing kafkaProperties when secrets are not set") {
      val source = createKafkaSource(kafkaProperties = Map("some.property" -> "some-value"))

      source.effectiveProperties.get("some.property") shouldBe Some("some-value")
    }

    it("typed secrets take precedence over kafkaProperties values") {
      val source = createKafkaSource(
        kafkaProperties = Map("ssl.keystore.password" -> "old-password"),
        sslKeystorePassword = Some(Secret("typed-password")),
      )

      source.effectiveProperties.get("ssl.keystore.password") shouldBe Some("typed-password")
    }
  }

  describe("V1ToV2 conversion for SaslJaasConfig") {

    it("converts V1 PlainLogin to V2 PlainLogin") {
      val v1Config: V1SaslJaasConfig = V1SaslJaasConfig.PlainLogin("alice", Secret("alice-password"))

      val v2Config = V1ToV2(v1Config)

      v2Config shouldBe a[api.PlainLogin]
      val plainLogin = v2Config.asInstanceOf[api.PlainLogin]
      plainLogin.username shouldBe "alice"
      plainLogin.password shouldBe Secret("alice-password")
    }

    it("converts V1 ScramLogin to V2 ScramLogin") {
      val v1Config: V1SaslJaasConfig = V1SaslJaasConfig.ScramLogin("bob", Secret("scram-password"))

      val v2Config = V1ToV2(v1Config)

      v2Config shouldBe a[api.ScramLogin]
      val scramLogin = v2Config.asInstanceOf[api.ScramLogin]
      scramLogin.username shouldBe "bob"
      scramLogin.password shouldBe Secret("scram-password")
    }

    it("converts V1 OAuthBearerLogin to V2 OAuthBearerLogin") {
      val v1Config: V1SaslJaasConfig =
        V1SaslJaasConfig.OAuthBearerLogin(
          "client-id",
          Secret("client-secret"),
          Some("my-scope"),
          Some("https://auth.example.com/token"),
        )

      val v2Config = V1ToV2(v1Config)

      v2Config shouldBe a[api.OAuthBearerLogin]
      val oauthLogin = v2Config.asInstanceOf[api.OAuthBearerLogin]
      oauthLogin.clientId shouldBe "client-id"
      oauthLogin.clientSecret shouldBe Secret("client-secret")
      oauthLogin.scope shouldBe Some("my-scope")
      oauthLogin.tokenEndpointUrl shouldBe Some("https://auth.example.com/token")
    }
  }
}
