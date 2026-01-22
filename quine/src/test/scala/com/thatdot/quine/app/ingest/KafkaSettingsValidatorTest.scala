package com.thatdot.quine.app.ingest

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import org.scalatest.Inspectors.forAll
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit

/** Apply test on ingest endpoint as a validator.
  * Security type can use an extra value.
  */
class KafkaSettingsValidatorTest extends AnyFunSuite {

  test("empty input settings map accepted") {
    assert(KafkaSettingsValidator.validateInput(Map()).isEmpty)
  }
  test("final empty input settings map accepted") {
    assert(KafkaSettingsValidator.validateInput(Map(), assumeConfigIsFinal = true).isEmpty)
  }

  test("Unrecognized input setting disallowed") {
    assert(
      KafkaSettingsValidator.validateInput(Map("Unrecognized.property.name" -> "anything")).get.size == 1,
    )
  }

  test("Conflicting input settings disallowed") {

    //group.id
    assert(
      KafkaSettingsValidator.validateInput(Map("group.id" -> "a"), explicitGroupId = Some("group")).get.size == 1,
    )

    //enable.auto.commit
    assert(
      KafkaSettingsValidator
        .validateInput(
          Map("enable.auto.commit" -> "a"),
          explicitOffsetCommitting = Some(ExplicitCommit(1000, 1000, 1100)),
        )
        .get
        .size == 1,
    )

    //auto.commit.interval.ms
    assert(
      KafkaSettingsValidator
        .validateInput(
          Map("auto.commit.interval.ms" -> "true"),
          explicitOffsetCommitting = Some(ExplicitCommit(1000, 1000, 1100)),
        )
        .get
        .size == 1,
    )

  }
  test("Unsupported input settings disallowed") {
    //value.deserializer
    assert(KafkaSettingsValidator.validateInput(Map("value.deserializer" -> "a")).get.size == 1)

    //bootstrap.servers
    assert(KafkaSettingsValidator.validateInput(Map("bootstrap.servers" -> "a")).get.size == 1)

    //security.protocol
    assert(KafkaSettingsValidator.validateInput(Map("security.protocol" -> "a")).get.size == 1)

    //completely made up
    assert(KafkaSettingsValidator.validateInput(Map("my.super.cool.property" -> "false")).get.size == 1)

  }
  test("Unsupported output settings disallowed") {
    //value.deserializer
    assert(KafkaSettingsValidator.validateProperties(Map("value.deserializer" -> "a")).get.size == 1)

    //bootstrap.servers
    assert(KafkaSettingsValidator.validateProperties(Map("bootstrap.servers" -> "a")).get.size == 1)

    //completely made up
    assert(KafkaSettingsValidator.validateProperties(Map("my.super.cool.property" -> "false")).get.size == 1)

  }
  test("non-member settings disallowed") {
    assert(KafkaSettingsValidator.validateProperties(Map("auto.offset.reset" -> "a")).get.size == 1)
  }
  test("SSL selections allowed") {
    // truststore
    assert(
      KafkaSettingsValidator
        .validateInput(
          Map("ssl.truststore.location" -> "alpha", "ssl.truststore.password" -> "beta"),
        )
        .isEmpty,
    )
    // keystore
    assert(
      KafkaSettingsValidator
        .validateInput(
          Map("ssl.keystore.location" -> "gamma", "ssl.keystore.password" -> "delta"),
        )
        .isEmpty,
    )
    // key
    assert(KafkaSettingsValidator.validateInput(Map("ssl.key.password" -> "epsilon")).isEmpty)
  }
  test("Spooky SASL selections disallowed") {
    // CVE-2023-25194
    val badModuleNoBiscuit = "com.sun.security.auth.module.JndiLoginModule"
    val bannedSettings = Seq(
      "producer.override.sasl.jaas.config" -> badModuleNoBiscuit,
      "consumer.override.sasl.jaas.config" -> badModuleNoBiscuit,
      "admin.override.sasl.jaas.config" -> badModuleNoBiscuit,
      "sasl.jaas.config" -> badModuleNoBiscuit,
    )
    // Each of these settings should be rejected for at least 1 reason
    forAll(bannedSettings) { setting =>
      assert(
        KafkaSettingsValidator.validateInput(Map(setting)).nonEmpty,
      )
      assert(KafkaSettingsValidator.validateProperties(Map(setting)).nonEmpty)
    }
  }

  test("parseBootstrapServers parses single server") {
    val result = KafkaSettingsValidator.parseBootstrapServers("localhost:9092")
    assert(result.map(_.toList) == Right(List(("localhost", 9092))))
  }

  test("parseBootstrapServers parses multiple servers") {
    val result = KafkaSettingsValidator.parseBootstrapServers("server1:9092,server2:9093")
    assert(result.map(_.toList) == Right(List(("server1", 9092), ("server2", 9093))))
  }

  test("parseBootstrapServers handles whitespace") {
    val result = KafkaSettingsValidator.parseBootstrapServers("server1:9092 , server2:9093")
    assert(result.map(_.toList) == Right(List(("server1", 9092), ("server2", 9093))))
  }

  test("parseBootstrapServers rejects missing port") {
    val result = KafkaSettingsValidator.parseBootstrapServers("localhost")
    assert(result.isLeft)
  }

  test("parseBootstrapServers rejects invalid port") {
    val result = KafkaSettingsValidator.parseBootstrapServers("localhost:notaport")
    assert(result.isLeft)
  }

  test("parseBootstrapServers rejects port out of range") {
    val result = KafkaSettingsValidator.parseBootstrapServers("localhost:99999")
    assert(result.isLeft)
  }

  test("parseBootstrapServers rejects empty string") {
    val result = KafkaSettingsValidator.parseBootstrapServers("")
    assert(result.isLeft)
  }

  test("checkBootstrapConnectivity returns error for unreachable server") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    // Port 19999 should not be listening on localhost
    val result = Await.result(
      KafkaSettingsValidator.checkBootstrapConnectivity("localhost:19999", timeout = 1.second),
      5.seconds,
    )
    assert(result.isDefined, "Expected error for unreachable server")
    assert(result.get.head.contains("localhost:19999"), "Error message should mention the server")
  }

  test("checkBootstrapConnectivity respects timeout and does not hang") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    // Use a non-routable IP address to ensure connection attempt times out
    val startTime = System.currentTimeMillis()
    val result = Await.result(
      KafkaSettingsValidator.checkBootstrapConnectivity("10.255.255.1:9092", timeout = 1.second),
      5.seconds,
    )
    val elapsed = System.currentTimeMillis() - startTime
    assert(result.isDefined, "Expected error for unreachable server")
    // Should complete within roughly the timeout period, not hang indefinitely
    assert(elapsed < 3000L, s"Expected completion within ~1-2 seconds, but took ${elapsed}ms")
  }

  test("checkBootstrapConnectivity returns errors only when all servers fail") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    // Both ports are unreachable, so we expect errors for both
    val result = Await.result(
      KafkaSettingsValidator.checkBootstrapConnectivity("localhost:19999,localhost:19998", timeout = 1.second),
      5.seconds,
    )
    assert(result.isDefined, "Expected error when all servers are unreachable")
  }

  test("parseBootstrapServers combines multiple errors") {
    val result = KafkaSettingsValidator.parseBootstrapServers("invalid,also-invalid")
    assert(result.isLeft)
    // Should contain errors for both servers
    val errors = result.left.getOrElse(cats.data.NonEmptyList.one("")).toList
    assert(errors.exists(_.contains("invalid")), "Should mention first invalid server")
    assert(errors.exists(_.contains("also-invalid")), "Should mention second invalid server")
  }

  test("checkBootstrapConnectivity returns parse error for malformed input") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val result = Await.result(
      KafkaSettingsValidator.checkBootstrapConnectivity("not-a-valid-server", timeout = 1.second),
      5.seconds,
    )
    assert(result.isDefined, "Expected error for malformed bootstrap server")
    assert(result.get.head.contains("host:port"), "Error should mention expected format")
  }

  test("validatePropertiesWithConnectivity returns property validation errors without checking connectivity") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    // bootstrap.servers in properties is disallowed - should fail property validation
    val result = Await.result(
      KafkaSettingsValidator.validatePropertiesWithConnectivity(
        Map("bootstrap.servers" -> "localhost:9092"),
        "localhost:9092",
        timeout = 1.second,
      ),
      5.seconds,
    )
    assert(result.isDefined, "Expected property validation error")
    assert(result.get.head.contains("bootstrap.servers"), "Error should mention the disallowed property")
  }

}
