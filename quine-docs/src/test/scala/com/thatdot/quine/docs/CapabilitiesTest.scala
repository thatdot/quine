package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import io.circe.{Json, Printer}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Deliberately unsealed, so [[Capabilities.sealedSubclassNames]] must refuse it. */
trait NotSealedFixture

class CapabilitiesTest extends AnyFunSuite with Matchers {

  private val PersistenceAgentTypeClass = "com.thatdot.quine.app.config.PersistenceAgentType"
  private val AllPersistors = Set("Cassandra", "ClickHouse", "Empty", "InMemory", "Keyspaces", "MapDb", "RocksDb")

  private def field(json: Json, name: String): Vector[String] =
    json.asArray.get.flatMap(_.hcursor.get[String](name).toOption)

  test("sealedSubclassNames enumerates every PersistenceAgentType") {
    Capabilities.sealedSubclassNames(PersistenceAgentTypeClass) shouldBe AllPersistors
  }

  test("sealedSubclassNames refuses an unsealed type rather than reporting nothing") {
    val e = the[IllegalArgumentException] thrownBy
      Capabilities.sealedSubclassNames("com.thatdot.quine.docs.NotSealedFixture")
    e.getMessage should include("not sealed")
  }

  test("sealedSubclassNames fails on a class that does not exist") {
    a[ScalaReflectionException] should be thrownBy
    Capabilities.sealedSubclassNames("com.thatdot.quine.docs.NoSuchClass")
  }

  test("persistor ids are the pureconfig `type` discriminators, sorted by class name") {
    val json = Capabilities.persistors(AllPersistors, Set.empty)
    field(json, "id") shouldBe Vector(
      "cassandra",
      "click-house",
      "empty",
      "in-memory",
      "keyspaces",
      "map-db",
      "rocks-db",
    )
    field(json, "className") shouldBe AllPersistors.toVector.sorted
  }

  test("persistors declared not shipped are left out") {
    val json = Capabilities.persistors(AllPersistors - "ClickHouse", Set("ClickHouse"))
    field(json, "id") should not contain "click-house"
    field(json, "id") should have size 6
  }

  test("a persistor listed as both shipped and not shipped is rejected") {
    val e = the[IllegalArgumentException] thrownBy Capabilities.persistors(AllPersistors, Set("ClickHouse"))
    e.getMessage should include("both shipped and not shipped: ClickHouse")
  }

  test("a persistor missing from the declaration is rejected") {
    val e = the[IllegalArgumentException] thrownBy Capabilities.persistors(AllPersistors - "ClickHouse", Set.empty)
    e.getMessage should include("Undeclared: ClickHouse")
  }

  test("a declared name that is not a persistor is rejected") {
    val e = the[IllegalArgumentException] thrownBy Capabilities.persistors(AllPersistors + "Postgres", Set.empty)
    e.getMessage should include("Declared but not a persistor: Postgres")
  }

  test("write creates parent directories and prints the fields in order") {
    val out = Files.createTempDirectory("capabilities-test").resolve("nested").resolve("capabilities.json")
    val fields = Vector("first" -> Json.fromInt(1), "second" -> Json.arr())
    Capabilities.write(out, fields)
    new String(Files.readAllBytes(out), StandardCharsets.UTF_8) shouldBe Printer.spaces2.print(Json.obj(fields: _*))
  }
}
