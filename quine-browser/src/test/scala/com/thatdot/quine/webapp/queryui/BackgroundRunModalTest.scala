package com.thatdot.quine.webapp.queryui

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.openapi.OpenApiParser

/** The run-in-background dialog builds its form from the live `BackgroundQueryDef` schema, so
  * what's worth pinning is that the schema it depends on has the shape the dialog assumes:
  * `query` strippable, `destinations` a required discriminated union array.
  *
  * The spec fragment below is trimmed verbatim from a running server's
  * `GET /api/v2/openapi.json`.
  */
class BackgroundRunModalTest extends AnyFunSuite with Matchers with OptionValues {

  private val specJson =
    """{
      |  "openapi": "3.1.0",
      |  "info": {"title": "t", "version": "1"},
      |  "paths": {},
      |  "components": {"schemas": {
      |    "BackgroundQueryDef": {
      |      "title": "BackgroundQueryDef",
      |      "type": "object",
      |      "required": ["query", "destinations"],
      |      "properties": {
      |        "query": {"type": "string"},
      |        "destinations": {"type": "array", "minItems": 1,
      |          "items": {"$ref": "#/components/schemas/QuineDestinationSteps"}},
      |        "name": {"type": "string"},
      |        "parameters": {"type": "object"},
      |        "statusExpiry": {"type": "string"}
      |      }
      |    },
      |    "QuineDestinationSteps": {
      |      "title": "QuineDestinationSteps",
      |      "oneOf": [
      |        {"$ref": "#/components/schemas/Drop"},
      |        {"$ref": "#/components/schemas/Kafka"}
      |      ],
      |      "discriminator": {"propertyName": "type",
      |        "mapping": {"Drop": "#/components/schemas/Drop", "Kafka": "#/components/schemas/Kafka"}}
      |    },
      |    "Drop": {"title": "Drop", "type": "object", "properties": {"type": {"type": "string"}}},
      |    "Kafka": {"title": "Kafka", "type": "object", "required": ["topic"],
      |      "properties": {"topic": {"type": "string"}, "type": {"type": "string"}}}
      |  }}
      |}""".stripMargin

  private val spec = OpenApiParser.parse(specJson).toOption.value

  test("the schema the dialog renders is present and carries the destination union") {
    val node = spec.schemas.get("BackgroundQueryDef").value
    node.properties.value.keys should contain allOf ("query", "destinations", "name", "statusExpiry")

    val destinations = OpenApiParser.resolveNode(node.properties.value("destinations"), spec.schemas)
    val items = OpenApiParser.resolveNode(destinations.items.value, spec.schemas)
    // A discriminated union is what `SchemaFormRenderer` needs to offer a destination picker
    // rather than a bare text field.
    items.discriminator.value.propertyName shouldBe "type"
    items.discriminator.value.mapping.value.keys should contain allOf ("Drop", "Kafka")
    items.oneOf.value should have size 2
  }

  test("stripping `query` leaves the fields the dialog does render") {
    // The dialog removes `query` because it comes from the editor buffer and is merged back at
    // submit; asking for it twice would let the two disagree.
    val node = spec.schemas.get("BackgroundQueryDef").value
    val stripped = node.copy(properties = node.properties.map(_.removed("query")))

    stripped.properties.value.keys should not contain "query"
    stripped.properties.value.keys should contain("destinations")
  }

  private def bodyOf(formState: String, query: String = "MATCH (n) RETURN n"): Json =
    BackgroundRunModal.buildBody(parse(formState).toOption.value, query)

  test("the submitted body merges the buffer's query over the form state") {
    // The server decodes this type strictly, so the merged object must carry only schema fields.
    val body = bodyOf("""{"destinations":[{"type":"Kafka","topic":"t"}],"name":"nightly"}""")

    body.hcursor.downField("query").as[String] shouldBe Right("MATCH (n) RETURN n")
    body.hcursor.downField("name").as[String] shouldBe Right("nightly")
    body.hcursor.keys.map(_.toSet).value shouldBe Set("destinations", "name", "query")
  }

  test("a chosen destination is left exactly as configured") {
    val body = bodyOf("""{"destinations":[{"type":"Kafka","topic":"results"}]}""")
    body.noSpaces should include("""[{"type":"Kafka","topic":"results"}]""")
  }

  test("several chosen destinations all survive") {
    val body = bodyOf("""{"destinations":[{"type":"Drop"},{"type":"StandardOut"}]}""")
    body.hcursor.downField("destinations").as[List[Json]].toOption.value should have size 2
  }

  test("an unset destination defaults to Drop rather than blocking the run") {
    // From a query bar the common intent is "run this and show me the results", which is what
    // Drop gives: the tap relay sees every row before the destinations do.
    bodyOf("""{"name":"nightly"}""").noSpaces should include("""[{"type":"Drop"}]""")
  }

  test("an empty or null destination list defaults the same way") {
    // Both shapes the array renderer can leave behind when the user adds nothing.
    bodyOf("""{"destinations":[]}""").noSpaces should include("""[{"type":"Drop"}]""")
    bodyOf("""{"destinations":null}""").noSpaces should include("""[{"type":"Drop"}]""")
  }

  test("defaulting replaces the destination list wholesale, never merging into it") {
    // `deepMerge` recurses into objects only, so an empty array is replaced rather than
    // element-merged — otherwise the default could fuse with a half-filled entry.
    val body = bodyOf("""{"destinations":[]}""")
    body.hcursor.downField("destinations").as[List[Json]].toOption.value shouldBe
    List(Json.obj("type" -> Json.fromString("Drop")))
  }
}
