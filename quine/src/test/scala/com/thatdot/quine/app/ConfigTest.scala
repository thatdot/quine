package com.thatdot.quine.app

import org.scalatest.funsuite.AnyFunSuite
import pureconfig._
import pureconfig.error._

class ConfigTest extends AnyFunSuite {
  import Config._

  def readConfig(config: String): QuineConfig =
    ConfigSource.string(config).loadOrThrow[QuineConfig](implicitly, quineReader)

  def writeConfig(config: QuineConfig): String =
    quineWriter.to(config).render()

  test("Empty config") {
    val empty1 = readConfig("quine {}")
    val roundtripped1 = readConfig(writeConfig(empty1))
    assert(empty1 === roundtripped1)

    val empty3 = readConfig("")
    val roundtripped3 = readConfig(writeConfig(empty3))
    assert(empty3 === roundtripped3)
  }

  test("Unknown settings in `quine` cause errors") {
    val dumpConfig = readConfig("quine { dump-config = yes }")
    val roundtripped = readConfig(writeConfig(dumpConfig))
    assert(dumpConfig === roundtripped)

    val error = intercept[ConfigReaderException[QuineConfig]](
      readConfig("quine { dumpConfig = yes }")
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
    assert(annotated === roundtripped)
    assert(annotated === defaultConf)
  }
}
