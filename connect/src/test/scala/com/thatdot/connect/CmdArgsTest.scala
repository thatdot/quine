package com.thatdot.connect

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

class CmdArgsTest extends AnyFunSuite with EitherValues {
  test("empty") {
    val cmdArgs = CmdArgs(Array.empty[String])
    assert(cmdArgs.value == CmdArgs())
  }

  test("version") {
    val cmdArgs = CmdArgs(Array("-v"))
    assert(cmdArgs.value.printVersion)
  }

  test("help") {
    val cmdArgs = CmdArgs(Array("--help"))
    assert(cmdArgs.left.value == """Quine universal program
                                 |Usage: quine [options]
                                 |
                                 |  -w, --web-service        start connect web service
                                 |  -p, --port <value>       web service port (default is 8080)
                                 |  -r, --recipe file or URL
                                 |                           follow the specified recipe
                                 |  -x, --recipe-value key=value
                                 |                           recipe parameter substitution
                                 |  --force-config           disable recipe configuration defaults
                                 |  --no-delete              disable deleting data file when process exits
                                 |  -h, --help
                                 |  -v, --version            print Quine program version""".stripMargin)
  }

  test("webservice") {
    val cmdArgs = CmdArgs(Array("-w", "-p", "8991"))
    assert(new CmdArgs(webservice = true, port = Some(8991), recipe = None) == cmdArgs.value)
  }

  test("recipe") {
    val cmdArgs = CmdArgs(Array("-r", "http://example.com", "-x", "a=b", "-x", "c=d"))
    assert(
      new CmdArgs(
        webservice = false,
        recipe = Some("http://example.com"),
        recipeValues = Map("a" -> "b", "c" -> "d")
      ) == cmdArgs.value
    )
  }
}
