package com.thatdot.quine.app.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should
import pureconfig.error.{ConfigReaderException, ConvertFailure, UnknownKey, UserValidationFailed}
import pureconfig.{ConfigSource, ConfigWriter}

class QuineConfigTest extends AnyFunSuite with should.Matchers {

  def readConfig(config: String): QuineConfig =
    ConfigSource.string(config).loadOrThrow[QuineConfig]

  def writeConfig(config: QuineConfig): String =
    ConfigWriter[QuineConfig].to(config).render()

  test("Empty config") {
    val empty1 = readConfig("quine {}")
    val roundtripped1 = readConfig(writeConfig(empty1))
    roundtripped1 shouldEqual empty1

    val empty3 = readConfig("")
    val roundtripped3 = readConfig(writeConfig(empty3))
    roundtripped3 shouldEqual empty3
  }

  test("Unknown settings in `quine` cause errors") {
    val dumpConfig = readConfig("quine { dump-config = yes }")
    val roundtripped = readConfig(writeConfig(dumpConfig))
    roundtripped shouldEqual dumpConfig

    val error = intercept[ConfigReaderException[QuineConfig]](
      readConfig("quine { dumpConfig = yes }"),
    )
    val failure = error.failures.head
    assert(failure.isInstanceOf[ConvertFailure])
    val convertFailure = failure.asInstanceOf[ConvertFailure]
    assert(convertFailure.reason === UnknownKey("dumpConfig"))
    assert(convertFailure.path === "quine.dumpConfig")
  }

  test("Annotated default config parses and matches the empty config") {
    val configStream = getClass.getResourceAsStream("/documented_config.conf")
    val annotated = readConfig(scala.io.Source.fromInputStream(configStream).mkString)
    val defaultConf = readConfig("")
    val roundtripped = readConfig(writeConfig(annotated))
    roundtripped shouldEqual annotated
    defaultConf shouldEqual annotated
  }

  test("Annotated default config for Cassandra parses and matches the empty config") {
    val configStream = getClass.getResourceAsStream("/documented_cassandra_config.conf")
    val annotated = readConfig(scala.io.Source.fromInputStream(configStream).mkString)
    val defaultConf = QuineConfig(store = PersistenceAgentType.Cassandra())
    val roundtripped = readConfig(writeConfig(annotated))
    roundtripped shouldEqual annotated
    defaultConf shouldEqual annotated
  }

  private def refusal(config: String): String = {
    val error = intercept[ConfigReaderException[QuineConfig]](readConfig(config))
    val failure = error.failures.head.asInstanceOf[ConvertFailure]
    failure.path shouldEqual "quine.persistence"
    failure.reason match {
      case UserValidationFailed(reason) => reason
      case other => fail(s"expected a validation failure, got $other")
    }
  }

  test("snapshot-after-events is refused without a journal and with a singleton snapshot") {
    refusal("quine { persistence { journal-enabled = false, snapshot-after-events = 2 } }") should include(
      "requires journal-enabled = true",
    )
    refusal("quine { persistence { snapshot-singleton = true, snapshot-after-events = 2 } }") should include(
      "must be 0 with snapshot-singleton = true",
    )
    readConfig(
      "quine { persistence { journal-enabled = true, snapshot-after-events = 2 } }",
    ).persistence.snapshotAfterEvents shouldEqual 2
    readConfig("quine { persistence { snapshot-singleton = true } }").persistence.snapshotAfterEvents shouldEqual 0
    readConfig(
      "quine { persistence { journal-enabled = false, snapshot-after-events = 0 } }",
    ).persistence.snapshotAfterEvents shouldEqual 0
  }

  test("snapshot-after-events is refused when negative") {
    refusal("quine { persistence { snapshot-after-events = -1 } }") should include("must not be negative")
    // Refused for being negative, not for the journal it would not have needed.
    refusal("quine { persistence { journal-enabled = false, snapshot-after-events = -1 } }") should include(
      "must not be negative",
    )
  }

  test("snapshot-after-events is refused under a schedule that never snapshots on sleep") {
    refusal("quine { persistence { snapshot-schedule = on-node-update, snapshot-after-events = 2 } }") should include(
      "unless snapshot-schedule = on-node-sleep",
    )
    refusal("quine { persistence { snapshot-schedule = never, snapshot-after-events = 2 } }") should include(
      "unless snapshot-schedule = on-node-sleep",
    )
    readConfig(
      "quine { persistence { snapshot-schedule = on-node-update } }",
    ).persistence.snapshotAfterEvents shouldEqual 0
  }
}
