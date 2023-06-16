package com.thatdot.quine.app.ingest

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit

/** Apply test on ingest endpoint as a validator.
  * Security type can use an extra value.
  */
class KafkaSettingsValidatorTest extends AnyFunSuite {

  test("Underlying Kafka ConfigDef is accessible") {
    KafkaSettingsValidator.underlyingValidator.isSuccess
  }
  test("empty settings map accepted") {
    assert(KafkaSettingsValidator(Map()).validate().isEmpty)
  }
  test("final empty settings map accepted") {
    assert(KafkaSettingsValidator(Map()).validate(assumeConfigIsFinal = true).isEmpty)
  }

  test("Unrecognized setting disallowed") {
    assert(
      KafkaSettingsValidator(Map("Unrecognized.property.name" -> "anything")).validate().get.size == 1
    )
  }

  test("Conflicting settings disallowed") {

    //group.id
    assert(
      KafkaSettingsValidator(Map("group.id" -> "a"), explicitGroupId = Some("group"))
        .validate()
        .get
        .size == 1
    )

    //enable.auto.commit
    assert(
      KafkaSettingsValidator(
        Map("enable.auto.commit" -> "a"),
        explicitOffsetCommitting = Some(ExplicitCommit(1000, 1000, 1100))
      ).validate(false).get.size == 1
    )

    //auto.commit.interval.ms
    assert(
      KafkaSettingsValidator(
        Map("auto.commit.interval.ms" -> "true"),
        explicitOffsetCommitting = Some(ExplicitCommit(1000, 1000, 1100))
      ).validate(false).get.size == 1
    )

  }
  test("Unsupported settings disallowed") {
    //value.deserializer
    assert(KafkaSettingsValidator(Map("value.deserializer" -> "a")).validate(false).get.size == 1)

    //bootstrap.servers
    assert(KafkaSettingsValidator(Map("bootstrap.servers" -> "a")).validate(false).get.size == 1)

    //security.protocol
    assert(KafkaSettingsValidator(Map("security.protocol" -> "a")).validate(false).get.size == 1)

    //completely made up
    assert(KafkaSettingsValidator(Map("my.super.cool.property" -> "false")).validate(false).get.size == 1)

  }
  test("non-member settings disallowed") {
    assert(KafkaSettingsValidator(Map("auto.offset.reset" -> "a")).validate(false).get.size == 1)
  }
  test("SSL selections allowed") {
    // truststore
    assert(
      KafkaSettingsValidator(
        Map("ssl.truststore.location" -> "alpha", "ssl.truststore.password" -> "beta")
      ).validate(false).isEmpty
    )
    // keystore
    assert(
      KafkaSettingsValidator(
        Map("ssl.keystore.location" -> "gamma", "ssl.keystore.password" -> "delta")
      ).validate(false).isEmpty
    )
    // key
    assert(KafkaSettingsValidator(Map("ssl.key.password" -> "epsilon")).validate(false).isEmpty)
  }

}
