package com.thatdot.quine.app.ingest

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

}
