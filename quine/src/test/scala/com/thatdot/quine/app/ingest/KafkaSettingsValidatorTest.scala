package com.thatdot.quine.app.ingest

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaIngest, KafkaSecurityProtocol}

/** Apply test on ingest endpoint as a validator.
  * Security type can use an extra value.
  */
class KafkaSettingsValidatorTest extends AnyFunSuite {

  private def testSettings(props: Map[String, String] = Map.empty) = KafkaIngest(
    topics = Left(Set("A")),
    parallelism = 1,
    bootstrapServers = "localhost:9092",
    groupId = Some("group"),
    securityProtocol = KafkaSecurityProtocol.PlainText,
    offsetCommitting = None,
    autoOffsetReset = KafkaAutoOffsetReset.Latest,
    kafkaProperties = props,
    endingOffset = Some(1L),
    maximumPerSecond = Some(1),
    recordDecoders = Seq.empty
  )

  test("base setting accepted") {
    assert(KafkaSettingsValidator(testSettings()).validate.isEmpty)
  }
  test("Unrecognized setting disallowed") {
    assert(KafkaSettingsValidator(testSettings(Map("Unrecognized.property.name" -> "anything"))).validate.get.size == 1)
  }

  test("Conflicting settings disallowed") {

    //group.id
    assert(
      KafkaSettingsValidator(testSettings(Map("group.id" -> "a")).copy(groupId = Some("group"))).validate.get.size == 1
    )

    //enable.auto.commit
    assert(
      KafkaSettingsValidator(
        testSettings(Map("enable.auto.commit" -> "a")).copy(offsetCommitting = Some(ExplicitCommit(1000, 1000, 1100)))
      ).validate.get.size == 1
    )

    //auto.commit.interval.ms
    assert(
      KafkaSettingsValidator(
        testSettings(Map("auto.commit.interval.ms" -> "a")).copy(offsetCommitting =
          Some(ExplicitCommit(1000, 1000, 1100))
        )
      ).validate.get.size == 1
    )

  }
  test("Unsupported settings disallowed") {
    //value.deserializer
    assert(KafkaSettingsValidator(testSettings(Map("value.deserializer" -> "a"))).validate.get.size == 1)

    //bootstrap.servers
    assert(KafkaSettingsValidator(testSettings(Map("bootstrap.servers" -> "a"))).validate.get.size == 1)

    //security.protocol
    assert(KafkaSettingsValidator(testSettings(Map("security.protocol" -> "a"))).validate.get.size == 1)

  }
  test("non-member settings disallowed") {
    assert(KafkaSettingsValidator(testSettings(Map("auto.offset.reset" -> "a")).copy()).validate.get.size == 1)
  }

}
