package com.thatdot.connect

import org.scalatest.funsuite.AnyFunSuite
import pureconfig._
import pureconfig.error._

class ConfigTest extends AnyFunSuite {
  import Config._

  def readConfig(config: String): ConnectConfig =
    ConfigSource.string(config).loadOrThrow[ConnectConfig](implicitly, connectReader)

  def writeConfig(config: ConnectConfig): String =
    connectWriter.to(config).render()

  test("Empty config") {
    val empty1 = readConfig("thatdot.connect {}")
    val roundtripped1 = readConfig(writeConfig(empty1))
    assert(empty1 === roundtripped1)

    val empty2 = readConfig("thatdot {}")
    val roundtripped2 = readConfig(writeConfig(empty2))
    assert(empty2 === roundtripped2)

    val empty3 = readConfig("")
    val roundtripped3 = readConfig(writeConfig(empty3))
    assert(empty3 === roundtripped3)
  }

  test("Unknown settings in `thatdot.connect` cause errors") {
    val dumpConfig = readConfig("thatdot.connect { dump-config = yes }")
    val roundtripped = readConfig(writeConfig(dumpConfig))
    assert(dumpConfig === roundtripped)

    val error = intercept[ConfigReaderException[ConnectConfig]](
      readConfig("thatdot.connect { dumpConfig = yes }")
    )
    val failure = error.failures.head
    assert(failure.isInstanceOf[ConvertFailure])
    val convertFailure = failure.asInstanceOf[ConvertFailure]
    assert(convertFailure.reason === UnknownKey("dumpConfig"))
    assert(convertFailure.path === "thatdot.connect.dumpConfig")
  }

  test("Unknown settings in `thatdot` do not cause errors") {
    val unknown = readConfig("thatdot { other-setting = 234s }")
    val roundtripped = readConfig(writeConfig(unknown))
    assert(unknown === roundtripped)
  }

  test("Annotated default config parses and matches the empty config") {
    val configStream = getClass.getResourceAsStream("/documented_config.conf")
    val annotated = readConfig(scala.io.Source.fromInputStream(configStream).mkString)
    val defaultConf = readConfig("")
    val roundtripped = readConfig(writeConfig(annotated))
    assert(annotated === roundtripped)
    assert(annotated === defaultConf)
  }
}
