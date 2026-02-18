package com.thatdot.quine.app.v2api.definitions.ingest2

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.PlainLogin
import com.thatdot.common.security.Secret

class KafkaDlqSecretParamsSpec extends AnyFunSuite with Matchers {

  test("DeadLetterQueueOutput.Kafka encodes with sslKeystorePassword redacted") {
    val kafka: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("keystore-secret")),
      outputFormat = OutputFormat.JSON(),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeystorePassword") shouldBe Right("Secret(****)")
  }

  test("DeadLetterQueueOutput.Kafka encodes with sslTruststorePassword redacted") {
    val kafka: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslTruststorePassword = Some(Secret("truststore-secret")),
      outputFormat = OutputFormat.JSON(),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslTruststorePassword") shouldBe Right("Secret(****)")
  }

  test("DeadLetterQueueOutput.Kafka encodes with sslKeyPassword redacted") {
    val kafka: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeyPassword = Some(Secret("key-secret")),
      outputFormat = OutputFormat.JSON(),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeyPassword") shouldBe Right("Secret(****)")
  }

  test("DeadLetterQueueOutput.Kafka encodes saslJaasConfig with password redacted") {
    val kafka: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      saslJaasConfig = Some(PlainLogin("alice", Secret("sasl-password"))),
      outputFormat = OutputFormat.JSON(),
    )
    val json = kafka.asJson

    val jaasJson = json.hcursor.downField("saslJaasConfig")
    jaasJson.get[String]("username") shouldBe Right("alice")
    jaasJson.get[String]("password") shouldBe Right("Secret(****)")
  }

  test("DeadLetterQueueOutput.Kafka round-trips with typed Secret params") {
    import Secret.Unsafe._
    val original: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("ks-pass")),
      sslKeyPassword = Some(Secret("key-pass")),
      saslJaasConfig = Some(PlainLogin("alice", Secret("secret"))),
      outputFormat = OutputFormat.JSON(),
    )

    val json = original.asJson
    val decoded = json.as[DeadLetterQueueOutput] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode DeadLetterQueueOutput: ${err.message}")
    }

    decoded shouldBe a[DeadLetterQueueOutput.Kafka]
    val kafka = decoded.asInstanceOf[DeadLetterQueueOutput.Kafka]
    kafka.sslKeystorePassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
    kafka.sslKeyPassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
    kafka.saslJaasConfig.map {
      case PlainLogin(username, password) => (username, password.unsafeValue)
      case _ => fail("Expected PlainLogin")
    } shouldBe Some(("alice", "Secret(****)"))
  }

  test("DeadLetterQueueOutput.Kafka round-trips with None Secret params") {
    val original: DeadLetterQueueOutput = DeadLetterQueueOutput.Kafka(
      topic = "test-topic",
      bootstrapServers = "localhost:9092",
      outputFormat = OutputFormat.JSON(),
    )

    val json = original.asJson
    val decoded = json.as[DeadLetterQueueOutput] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode DeadLetterQueueOutput: ${err.message}")
    }

    decoded shouldBe a[DeadLetterQueueOutput.Kafka]
    val kafka = decoded.asInstanceOf[DeadLetterQueueOutput.Kafka]
    kafka.sslKeystorePassword shouldBe None
    kafka.sslTruststorePassword shouldBe None
    kafka.sslKeyPassword shouldBe None
    kafka.saslJaasConfig shouldBe None
  }
}
