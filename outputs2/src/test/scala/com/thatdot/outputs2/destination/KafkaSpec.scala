package com.thatdot.outputs2.destination

import org.apache.pekko.actor.ActorSystem

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.security.Secret
import com.thatdot.outputs2.{PlainLogin, ScramLogin}

class KafkaSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("KafkaSpec")

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  test("effectiveProperties includes sslKeystorePassword when set") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("keystore-secret")),
    )

    kafka.effectiveProperties should contain("ssl.keystore.password" -> "keystore-secret")
  }

  test("effectiveProperties includes sslTruststorePassword when set") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      sslTruststorePassword = Some(Secret("truststore-secret")),
    )

    kafka.effectiveProperties should contain("ssl.truststore.password" -> "truststore-secret")
  }

  test("effectiveProperties includes sslKeyPassword when set") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      sslKeyPassword = Some(Secret("key-secret")),
    )

    kafka.effectiveProperties should contain("ssl.key.password" -> "key-secret")
  }

  test("effectiveProperties includes saslJaasConfig as JAAS string when set") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      saslJaasConfig = Some(PlainLogin("alice", Secret("password123"))),
    )

    val jaasConfig = kafka.effectiveProperties.get("sasl.jaas.config")
    jaasConfig shouldBe defined
    jaasConfig.get should include("PlainLoginModule")
    jaasConfig.get should include("alice")
    jaasConfig.get should include("password123")
  }

  test("effectiveProperties preserves non-conflicting kafkaProperties") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      kafkaProperties = Map(
        "acks" -> "all",
        "batch.size" -> "16384",
      ),
    )

    kafka.effectiveProperties should contain("acks" -> "all")
    kafka.effectiveProperties should contain("batch.size" -> "16384")
  }

  test("typed Secret params override conflicting kafkaProperties") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("typed-keystore-secret")),
      kafkaProperties = Map(
        "ssl.keystore.password" -> "should-be-overridden",
        "acks" -> "all",
      ),
    )

    kafka.effectiveProperties should contain("ssl.keystore.password" -> "typed-keystore-secret")
    kafka.effectiveProperties should contain("acks" -> "all")
    kafka.effectiveProperties should not contain ("ssl.keystore.password" -> "should-be-overridden")
  }

  test("all typed Secret params override their corresponding kafkaProperties") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
      sslKeystorePassword = Some(Secret("typed-ks")),
      sslTruststorePassword = Some(Secret("typed-ts")),
      sslKeyPassword = Some(Secret("typed-key")),
      saslJaasConfig = Some(ScramLogin("bob", Secret("typed-sasl"))),
      kafkaProperties = Map(
        "ssl.keystore.password" -> "old-ks",
        "ssl.truststore.password" -> "old-ts",
        "ssl.key.password" -> "old-key",
        "sasl.jaas.config" -> "old-jaas-config",
      ),
    )

    kafka.effectiveProperties("ssl.keystore.password") shouldBe "typed-ks"
    kafka.effectiveProperties("ssl.truststore.password") shouldBe "typed-ts"
    kafka.effectiveProperties("ssl.key.password") shouldBe "typed-key"
    kafka.effectiveProperties("sasl.jaas.config") should include("ScramLoginModule")
    kafka.effectiveProperties("sasl.jaas.config") should include("typed-sasl")
  }

  test("effectiveProperties is empty when no params are set") {
    val kafka = Kafka(
      topic = "test",
      bootstrapServers = "localhost:9092",
    )

    kafka.effectiveProperties shouldBe empty
  }
}
