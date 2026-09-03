package com.thatdot.quine.docs

import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import io.circe.{Json, Printer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GenerateCapabilitiesTest extends AnyFunSuite with Matchers {

  test("main is a static method on the object, so `runMain` can find it") {
    // `main` is inherited from CapabilityGenerator; this checks the static forwarder Scala emits for it.
    val main = Class.forName("com.thatdot.quine.docs.GenerateCapabilities").getMethod("main", classOf[Array[String]])
    Modifier.isStatic(main.getModifiers) shouldBe true
  }

  test("Quine OSS declares persistors only") {
    GenerateCapabilities.fields.map(_._1) shouldBe Vector("persistors")
  }

  test("Quine OSS ships every persistor except ClickHouse") {
    val ids = GenerateCapabilities.fields.head._2.asArray.get.flatMap(_.hcursor.get[String]("id").toOption)
    ids shouldBe Vector("cassandra", "empty", "in-memory", "keyspaces", "map-db", "rocks-db")
  }

  test("main writes exactly the document `fields` describes") {
    val out = Files.createTempDirectory("generate-capabilities-test").resolve("capabilities.json")
    GenerateCapabilities.main(Array(out.toString))
    new String(Files.readAllBytes(out), StandardCharsets.UTF_8) shouldBe
    Printer.spaces2.print(Json.obj(GenerateCapabilities.fields: _*))
  }
}
