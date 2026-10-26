package com.thatdot.quine.app.llm

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.funsuite.AnyFunSuite

class JsonSchemaValidatorTest extends AnyFunSuite {

  private def json(text: String): Json = parse(text).fold(throw _, identity)
  private def valid(schema: String, instance: String): Boolean =
    JsonSchemaValidator.isValid(json(schema), json(instance))

  test("boolean schemas") {
    assert(valid("true", "42"))
    assert(!valid("false", "42"))
  }

  test("type, as a string or a list") {
    assert(valid("""{"type":"string"}""", "\"s\""))
    assert(!valid("""{"type":"string"}""", "1"))
    assert(valid("""{"type":["string","null"]}""", "null"))
    assert(valid("""{"type":"integer"}""", "3"))
    assert(!valid("""{"type":"integer"}""", "3.5"))
    assert(valid("""{"type":"number"}""", "3.5"))
    assert(valid("""{"type":"boolean"}""", "true"))
    assert(valid("""{"type":"array"}""", "[]"))
    assert(valid("""{"type":"object"}""", "{}"))
  }

  test("enum and const") {
    assert(valid("""{"enum":["a","b"]}""", "\"a\""))
    assert(!valid("""{"enum":["a","b"]}""", "\"c\""))
    assert(valid("""{"const":7}""", "7"))
    assert(!valid("""{"const":7}""", "8"))
  }

  test("properties, required, and additionalProperties false") {
    val schema = """{"type":"object","required":["a"],"additionalProperties":false,
                   | "properties":{"a":{"type":"string"},"b":{"type":"integer"}}}""".stripMargin
    assert(valid(schema, """{"a":"x"}"""))
    assert(valid(schema, """{"a":"x","b":2}"""))
    assert(!valid(schema, """{"b":2}"""), "missing required")
    assert(!valid(schema, """{"a":1}"""), "wrong property type")
    assert(!valid(schema, """{"a":"x","c":true}"""), "additional property")
  }

  test("additionalProperties false with nonced keys") {
    val schema = PromptFencing.suffixSchemaKeys(
      json("""{"type":"object","required":["sentiment"],"additionalProperties":false,
             | "properties":{"sentiment":{"enum":["positive","negative"]}}}""".stripMargin),
      "__n1",
    )
    assert(JsonSchemaValidator.isValid(schema, json("""{"sentiment__n1":"positive"}""")))
    assert(!JsonSchemaValidator.isValid(schema, json("""{"sentiment":"positive"}""")), "un-suffixed key is additional")
  }

  test("items, minItems, maxItems") {
    val schema = """{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":2}"""
    assert(valid(schema, """["a"]"""))
    assert(!valid(schema, """[]"""))
    assert(!valid(schema, """["a","b","c"]"""))
    assert(!valid(schema, """[1]"""))
  }

  test("minimum and maximum") {
    assert(valid("""{"minimum":0,"maximum":1}""", "0.5"))
    assert(!valid("""{"minimum":0,"maximum":1}""", "1.5"))
    assert(!valid("""{"minimum":0}""", "-1"))
  }

  test("minLength, maxLength, pattern") {
    assert(valid("""{"minLength":2,"maxLength":3}""", "\"ab\""))
    assert(!valid("""{"minLength":2}""", "\"a\""))
    assert(!valid("""{"maxLength":1}""", "\"ab\""))
    assert(valid("""{"pattern":"^[a-z]+$"}""", "\"abc\""))
    assert(!valid("""{"pattern":"^[a-z]+$"}""", "\"ABC\""))
    assert(valid("""{"pattern":"[unclosed"}""", "\"x\""), "an invalid regex is no constraint")
  }

  test("anyOf, allOf, oneOf, not") {
    assert(valid("""{"anyOf":[{"type":"string"},{"type":"null"}]}""", "null"))
    assert(!valid("""{"anyOf":[{"type":"string"},{"type":"null"}]}""", "1"))
    assert(valid("""{"allOf":[{"type":"integer"},{"minimum":5}]}""", "6"))
    assert(!valid("""{"allOf":[{"type":"integer"},{"minimum":5}]}""", "4"))
    assert(valid("""{"oneOf":[{"type":"integer"},{"minimum":5}]}""", "4"))
    assert(!valid("""{"oneOf":[{"type":"integer"},{"minimum":5}]}""", "6"), "matches both")
    assert(valid("""{"not":{"type":"string"}}""", "1"))
    assert(!valid("""{"not":{"type":"string"}}""", "\"s\""))
  }

  test("validate reports the path of each failure") {
    val errors = JsonSchemaValidator.validate(
      json("""{"type":"object","properties":{"a":{"type":"array","items":{"type":"integer"}}}}"""),
      json("""{"a":[1,"x"]}"""),
    )
    assert(errors == List("$.a[1]: expected type integer"))
  }

  private def checked(schema: String): List[String] = JsonSchemaValidator.checkSchema(json(schema))

  test("checkSchema accepts every schema validate enforces, with the annotations") {
    assert(checked("true") == Nil)
    assert(checked("false") == Nil)
    val full = """{"type":"object","required":["a"],"additionalProperties":false,
                 | "title":"t","description":"d","default":{},"examples":[{}],
                 | "properties":{"a":{"anyOf":[{"enum":["x"]},{"const":3},
                 |   {"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":2},
                 |   {"type":["string","null"],"minLength":1,"maxLength":9,"pattern":"^[a-z]+$"},
                 |   {"type":"number","minimum":0,"maximum":1},
                 |   {"allOf":[{"type":"integer"}],"oneOf":[{"minimum":5}],"not":{"const":9}}]}}}""".stripMargin
    assert(checked(full) == Nil)
  }

  test("checkSchema rejects an unknown keyword, with the path") {
    assert(
      checked("""{"type":"object","properties":{"a":{"minimumm":1}}}""") ==
        List("""$.properties.a: unknown schema keyword "minimumm""""),
    )
  }

  test("checkSchema rejects a type value that is not a JSON Schema type name") {
    assert(
      checked("""{"properties":{"stars":{"type":"integegegeger"}}}""") ==
        List("""$.properties.stars.type: "integegegeger" is not a JSON Schema type"""),
    )
    assert(checked("""{"type":1}""") == List("$.type: must be a type name or a list of type names"))
    assert(checked("""{"type":["string",1]}""") == List("$.type: must be a type name or a list of type names"))
    assert(checked("""{"type":[]}""") == List("$.type: must name at least one type"))
  }

  test("checkSchema rejects a keyword value of the wrong shape") {
    assert(checked("""{"enum":"a"}""") == List("$.enum: must be an array"))
    assert(checked("""{"required":"a"}""") == List("$.required: must be an array of strings"))
    assert(checked("""{"required":["a",1]}""") == List("$.required: must be an array of strings"))
    assert(checked("""{"properties":[]}""") == List("$.properties: must be an object"))
    assert(checked("""{"minimum":"0"}""") == List("$.minimum: must be a number"))
    assert(
      checked("""{"minLength":1.5}""") ==
        List("$.minLength: must be an integer between -2147483648 and 2147483647"),
    )
    assert(
      checked("""{"maxItems":10000000000}""") ==
        List("$.maxItems: must be an integer between -2147483648 and 2147483647"),
      "an integral value over Int range cannot be compared by validate, so it is rejected too",
    )
    assert(checked("""{"anyOf":{}}""") == List("$.anyOf: must be a list of schemas"))
    assert(checked("""{"not":1}""") == List("$.not: a schema must be an object or a boolean"))
  }

  test("checkSchema rejects the shapes validate silently skips") {
    assert(checked("""{"pattern":"[unclosed"}""") == List("$.pattern: is not a valid regular expression"))
    assert(checked("""{"pattern":1}""") == List("$.pattern: must be a string"))
    assert(
      checked("""{"additionalProperties":{"type":"string"}}""") ==
        List("$.additionalProperties: must be true or false, a schema here is not enforced"),
    )
    assert(
      checked("""{"items":[{"type":"string"}]}""") ==
        List("$.items: must be a single schema, not a list of schemas by position"),
    )
  }

  test("checkSchema reports every fault and recurses into subschemas") {
    assert(
      checked("""{"type":"objct","oneOf":[{"enum":1},{"items":{"typ":"string"}}]}""") == List(
        """$.type: "objct" is not a JSON Schema type""",
        "$.oneOf[0].enum: must be an array",
        """$.oneOf[1].items: unknown schema keyword "typ"""",
      ),
    )
  }
}
