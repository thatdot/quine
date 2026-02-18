package com.thatdot.quine.app.v2api.definitions.ingest2

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.PlainLogin
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.KafkaIngest
import com.thatdot.quine.app.v2api.converters.ApiToIngest
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestSource

class KafkaIngestSecretParamsSpec extends AnyFunSuite with Matchers {

  test("IngestSource.Kafka encodes with sslKeystorePassword redacted") {
    val kafka: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = None,
      offsetCommitting = None,
      endingOffset = None,
      sslKeystorePassword = Some(Secret("keystore-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeystorePassword") shouldBe Right("Secret(****)")
  }

  test("IngestSource.Kafka encodes with sslTruststorePassword redacted") {
    val kafka: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = None,
      offsetCommitting = None,
      endingOffset = None,
      sslTruststorePassword = Some(Secret("truststore-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslTruststorePassword") shouldBe Right("Secret(****)")
  }

  test("IngestSource.Kafka encodes with sslKeyPassword redacted") {
    val kafka: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = None,
      offsetCommitting = None,
      endingOffset = None,
      sslKeyPassword = Some(Secret("key-secret")),
    )
    val json = kafka.asJson

    json.hcursor.get[String]("sslKeyPassword") shouldBe Right("Secret(****)")
  }

  test("IngestSource.Kafka encodes saslJaasConfig with password redacted") {
    val kafka: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = None,
      offsetCommitting = None,
      endingOffset = None,
      saslJaasConfig = Some(PlainLogin("alice", Secret("sasl-password"))),
    )
    val json = kafka.asJson

    val jaasJson = json.hcursor.downField("saslJaasConfig")
    jaasJson.get[String]("username") shouldBe Right("alice")
    jaasJson.get[String]("password") shouldBe Right("Secret(****)")
  }

  test("IngestSource.Kafka round-trips with typed Secret params") {
    import Secret.Unsafe._
    val original: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = Some("test-group"),
      offsetCommitting = None,
      endingOffset = None,
      sslKeystorePassword = Some(Secret("ks-pass")),
      sslKeyPassword = Some(Secret("key-pass")),
      saslJaasConfig = Some(PlainLogin("alice", Secret("secret"))),
    )

    val json = original.asJson
    val decoded = json.as[IngestSource] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode IngestSource: ${err.message}")
    }

    decoded shouldBe a[IngestSource.Kafka]
    val kafka = decoded.asInstanceOf[IngestSource.Kafka]
    kafka.sslKeystorePassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
    kafka.sslKeyPassword.map(_.unsafeValue) shouldBe Some("Secret(****)")
    kafka.saslJaasConfig.map {
      case PlainLogin(username, password) => (username, password.unsafeValue)
      case _ => fail("Expected PlainLogin")
    } shouldBe Some(("alice", "Secret(****)"))
  }

  test("IngestSource.Kafka round-trips with None Secret params") {
    val original: IngestSource = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = Some("test-group"),
      offsetCommitting = None,
      endingOffset = None,
    )

    val json = original.asJson
    val decoded = json.as[IngestSource] match {
      case Right(v) => v
      case Left(err) => fail(s"Failed to decode IngestSource: ${err.message}")
    }

    decoded shouldBe a[IngestSource.Kafka]
    val kafka = decoded.asInstanceOf[IngestSource.Kafka]
    kafka.sslKeystorePassword shouldBe None
    kafka.sslTruststorePassword shouldBe None
    kafka.sslKeyPassword shouldBe None
    kafka.saslJaasConfig shouldBe None
  }

  test("ApiToIngest converts Kafka secrets to internal model") {
    val apiKafka = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = Some("test-group"),
      offsetCommitting = None,
      endingOffset = None,
      sslKeystorePassword = Some(Secret("keystore-secret")),
      sslTruststorePassword = Some(Secret("truststore-secret")),
      sslKeyPassword = Some(Secret("key-secret")),
      saslJaasConfig = Some(PlainLogin("alice", Secret("sasl-password"))),
    )

    val internalSource = ApiToIngest(apiKafka)

    internalSource shouldBe a[KafkaIngest]
    val internalKafka = internalSource.asInstanceOf[KafkaIngest]
    internalKafka.sslKeystorePassword shouldBe Some(Secret("keystore-secret"))
    internalKafka.sslTruststorePassword shouldBe Some(Secret("truststore-secret"))
    internalKafka.sslKeyPassword shouldBe Some(Secret("key-secret"))
    internalKafka.saslJaasConfig shouldBe Some(PlainLogin("alice", Secret("sasl-password")))
  }

  test("ApiToIngest converts Kafka with None secrets to internal model with None secrets") {
    val apiKafka = IngestSource.Kafka(
      format = ApiIngest.IngestFormat.StreamingFormat.Json,
      topics = Left(Set("test-topic")),
      bootstrapServers = "localhost:9092",
      groupId = Some("test-group"),
      offsetCommitting = None,
      endingOffset = None,
    )

    val internalSource = ApiToIngest(apiKafka)

    internalSource shouldBe a[KafkaIngest]
    val internalKafka = internalSource.asInstanceOf[KafkaIngest]
    internalKafka.sslKeystorePassword shouldBe None
    internalKafka.sslTruststorePassword shouldBe None
    internalKafka.sslKeyPassword shouldBe None
    internalKafka.saslJaasConfig shouldBe None
  }
}
