package com.thatdot.quine.app

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

  // Different platforms can render the boundary whitespace differently (eg tabs vs spaces), so check only the content
  def contentOf(multiline: String): List[String] = multiline.split('\n').map(_.trim).toList

  test("help") {
    val cmdArgs = CmdArgs(Array("--help"))
    assert(
      contentOf(cmdArgs.left.value) ===
        contentOf("""Quine universal program
                    |Usage: quine [options]
                    |
                    |  -W, --disable-web-service
                    |                           disable Quine web service
                    |  -p, --port <value>       web service port (default is 8080)
                    |  -r, --recipe name, file, or URL
                    |                           follow the specified recipe
                    |  -x, --recipe-value key=value
                    |                           recipe parameter substitution
                    |  --force-config           disable recipe configuration defaults
                    |  --no-delete              disable deleting data file when process exits
                    |  -h, --help
                    |  -v, --version            print Quine program version""".stripMargin)
    )
  }

  test("port") {
    val cmdArgs = CmdArgs(Array("-p", "8991"))
    assert(new CmdArgs(disableWebservice = false, port = Some(8991), recipe = None) == cmdArgs.value)
  }

  test("disable webservice") {
    val cmdArgs = CmdArgs(Array("-W"))
    assert(new CmdArgs(disableWebservice = true, port = None, recipe = None) == cmdArgs.value)
  }

  test("disable webservice with port") {
    val cmdArgs = CmdArgs(Array("-W", "-p", "1234"))
    assert(
      contentOf("""Error: use only one: --disable-web-service, or --port
                  |Try --help for more information.""".stripMargin) == contentOf(cmdArgs.left.value)
    )
  }

  test("recipe") {
    val cmdArgs = CmdArgs(Array("-r", "http://example.com", "-x", "a=b", "-x", "c=d"))
    assert(
      new CmdArgs(
        disableWebservice = false,
        recipe = Some("http://example.com"),
        recipeValues = Map("a" -> "b", "c" -> "d")
      ) == cmdArgs.value
    )
  }
}
