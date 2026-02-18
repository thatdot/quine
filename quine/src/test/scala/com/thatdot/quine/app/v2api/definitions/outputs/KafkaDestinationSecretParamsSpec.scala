package com.thatdot.quine.app.v2api.definitions.outputs

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.PlainLogin
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps

class KafkaDestinationSecretParamsSpec extends AnyFunSuite with Matchers {

  test("QuineDestinationSteps.Kafka encodes with sslKeystorePassword redacted") {
    val kafka: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("keystore-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeystorePassword") shouldBe Right("Secret(****)")
  }

  test("QuineDestinationSteps.Kafka encodes with sslTruststorePassword redacted") {
    val kafka: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslTruststorePassword = Some(Secret("truststore-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslTruststorePassword") shouldBe Right("Secret(****)")
  }

  test("QuineDestinationSteps.Kafka encodes with sslKeyPassword redacted") {
    val kafka: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeyPassword = Some(Secret("key-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeyPassword") shouldBe Right("Secret(****)")
  }

  test("QuineDestinationSteps.Kafka encodes saslJaasConfig with password redacted") {
    val kafka: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      saslJaasConfig = Some(PlainLogin("alice", Secret("sasl-password"))),
    )
    val json = kafka.asJson

    val jaasJson = json.hcursor.downField("saslJaasConfig")
    jaasJson.get[String]("username") shouldBe Right("alice")
    jaasJson.get[String]("password") shouldBe Right("Secret(****)")
  }

  test("QuineDestinationSteps.Kafka round-trips with typed Secret params") {
    import Secret.Unsafe._
    val original: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("ks-pass")),
      sslKeyPassword = Some(Secret("key-pass")),
      saslJaasConfig = Some(PlainLogin("alice", Secret("secret"))),
    )

    val json = original.asJson
    val decoded = json.as[QuineDestinationSteps] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode QuineDestinationSteps: ${err.message}")
    }

    decoded shouldBe a[QuineDestinationSteps.Kafka]
    val kafka = decoded.asInstanceOf[QuineDestinationSteps.Kafka]
    kafka.sslKeystorePassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
    kafka.sslKeyPassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
  }

  test("QuineDestinationSteps.Kafka round-trips with None Secret params") {
    val original: QuineDestinationSteps = QuineDestinationSteps.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
    )

    val json = original.asJson
    val decoded = json.as[QuineDestinationSteps] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode QuineDestinationSteps: ${err.message}")
    }

    decoded shouldBe a[QuineDestinationSteps.Kafka]
    val kafka = decoded.asInstanceOf[QuineDestinationSteps.Kafka]
    kafka.sslKeystorePassword shouldBe None
    kafka.sslTruststorePassword shouldBe None
    kafka.sslKeyPassword shouldBe None
    kafka.saslJaasConfig shouldBe None
  }
}
