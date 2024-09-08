package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherFunctions extends CypherHarness("cypher-function-tests") {

  describe("`bytes` function") {
    // upper case
    testExpression("""bytes("CEDEC0DE")""", Expr.Bytes(Array(0xCE, 0xDE, 0xC0, 0xDE).map(_.toByte)))
    // lower case
    testExpression("""bytes("cafec0de")""", Expr.Bytes(Array(0xCA, 0xFE, 0xC0, 0xDE).map(_.toByte)))
    // mixed case
    testExpression("""bytes("feEdb33f")""", Expr.Bytes(Array(0xFE, 0xED, 0xB3, 0x3F).map(_.toByte)))
    // length unlikely to be an int
    testExpression("""bytes("000000")""", Expr.Bytes(Array(0x00, 0x00, 0x00).map(_.toByte)))
    // single byte
    testExpression("""bytes("02")""", Expr.Bytes(Array(0x02).map(_.toByte)))
    // right padded 0s
    testExpression("""bytes("c0ffee00")""", Expr.Bytes(Array(0xC0, 0xFF, 0xEE, 0x00).map(_.toByte)))
    // left padded 0s
    testExpression("""bytes("0000c0De")""", Expr.Bytes(Array(0x00, 0x00, 0xC0, 0xDE).map(_.toByte)))
    // left and right padded 0s
    testExpression("""bytes("00FACE00")""", Expr.Bytes(Array(0x00, 0xFA, 0xCE, 0x00).map(_.toByte)))
  }

  describe("`toJson` function") {
    testExpression("toJson(100.000)", Expr.Str("100.0"))
    testExpression("toJson(100)", Expr.Str("100"))
    testExpression(
      "toJson([n, r, m])",
      Expr.Str(
        List(
          """{"id":"0","labels":[],"properties":{"foo":"bar"}}""",
          """{"start":"0","end":"1","name":"relation","properties":{}}""",
          """{"id":"1","labels":[],"properties":{}}""",
        ).mkString("[", ",", "]"),
      ),
      queryPreamble = """CREATE (n{foo: "bar"})-[r:relation]->(m) RETURN """,
      expectedIsIdempotent = false,
      expectedIsReadOnly = false,
    )
    // TODO depends on bytes tests
    // testExpression("""toJson(bytes("c0de"))""", Expr.Bytes(Array(0xc0, 0xde).map(_.toByte)))
  }

  describe("`parseJson` function") {
    testExpression("""parseJson("42")""", Expr.Integer(42))
    testExpression("""parseJson("-42")""", Expr.Integer(-42))
    testExpression("""parseJson("42.0")""", Expr.Integer(42))
    testExpression("""parseJson("42.5")""", Expr.Floating(42.5))
    testExpression("""parseJson("null")""", Expr.Null)
    testExpression(
      """parseJson("{\"hello\": \"world\", \"x\": -128.4, \"b\": false, \"nest\": {\"birds\": [1, 4], \"type\": \"robin\"}}")""",
      Expr.Map(
        Map(
          "hello" -> Expr.Str("world"),
          "x" -> Expr.Floating(-128.4),
          "b" -> Expr.False,
          "nest" -> Expr.Map(
            Map(
              "birds" -> Expr.List(
                Vector(
                  Expr.Integer(1),
                  Expr.Integer(4),
                ),
              ),
              "type" -> Expr.Str("robin"),
            ),
          ),
        ),
      ),
    )
  }

  describe("`map.fromPairs` function") {
    testExpression(
      "map.fromPairs([])",
      Expr.Map(Map.empty),
    )
    testExpression(
      "map.fromPairs([['a', 1],['b',2]])",
      Expr.Map(Map("a" -> Expr.Integer(1L), "b" -> Expr.Integer(2L))),
    )
  }

  describe("`map.removeKey` function") {
    testExpression(
      "map.removeKey({ foo: 'bar', baz: 123 }, 'foo')",
      Expr.Map(Map("baz" -> Expr.Integer(123L))),
    )

    testExpression(
      "map.removeKey({ foo: 'bar', baz: 123 }, 'qux')",
      Expr.Map(Map("foo" -> Expr.Str("bar"), "baz" -> Expr.Integer(123L))),
    )
  }

  describe("`coll.max` function") {
    testExpression("coll.max([])", Expr.Null)
    testExpression("coll.max([3.14])", Expr.Floating(3.14))
    testExpression("coll.max([3.14, 3, 4])", Expr.Integer(4L))
    testExpression("coll.max(3.14, 3, 4)", Expr.Integer(4L))
    testExpression("coll.max([3.14, 2.9, 'not a number'])", Expr.Floating(3.14))
    testExpression("coll.max([3.14, 10.1, 2, 2.9])", Expr.Floating(10.1))
    testExpression("coll.max(3.14, 10.1, 2, 2.9)", Expr.Floating(10.1))
    testQuery(
      "UNWIND [3.14, 10.1, 2, 2.9] AS x RETURN max(x)",
      expectedColumns = Vector("max(x)"),
      expectedRows = Seq(
        Vector(Expr.Floating(10.1)),
      ),
      expectedCannotFail = true,
      expectedIsIdempotent = true,
    )
  }

  describe("`coll.min` function") {
    testExpression("coll.min([])", Expr.Null)
    testExpression("coll.min([3.14])", Expr.Floating(3.14))
    testExpression("coll.min([3.14, 3, 4])", Expr.Integer(3L))
    testExpression("coll.min(3.14, 3, 4)", Expr.Integer(3L))
    testExpression("coll.min([3.14, 2.9, 'not a number'])", Expr.Str("not a number"))
    testExpression("coll.min([3.14, 10.1, 2, 2.9])", Expr.Integer(2L))
    testExpression("coll.min(3.14, 10.1, 2, 2.9)", Expr.Integer(2L))
    testQuery(
      "UNWIND [3.14, 10.1, 2, 2.9] AS x RETURN min(x)",
      expectedColumns = Vector("min(x)"),
      expectedRows = Seq(
        Vector(Expr.Integer(2L)),
      ),
      expectedCannotFail = true,
      expectedIsIdempotent = true,
    )
  }

  describe("`toInteger` function") {
    testExpression("toInteger(123)", Expr.Integer(123L))

    testExpression("toInteger(123.0)", Expr.Integer(123L))
    testExpression("toInteger(123.3)", Expr.Integer(123L))
    testExpression("toInteger(123.7)", Expr.Integer(123L))
    testExpression("toInteger(-123.3)", Expr.Integer(-123L))
    testExpression("toInteger(-123.7)", Expr.Integer(-123L))

    testExpression("toInteger('123')", Expr.Integer(123L))
    testExpression("toInteger('123.0')", Expr.Integer(123L))
    testExpression("toInteger('123.3')", Expr.Integer(123L))
    testExpression("toInteger('123.7')", Expr.Integer(123L))
    testExpression("toInteger('-123.3')", Expr.Integer(-123L))
    testExpression("toInteger('-123.7')", Expr.Integer(-123L))

    testExpression("toInteger('0x11')", Expr.Integer(0x11L))
    testExpression("toInteger('0xf')", Expr.Integer(0xFL))
    testExpression("toInteger('0xc0FfEe')", Expr.Integer(0xC0FFEEL))
    testExpression("toInteger('-0x12')", Expr.Integer(-0x12L))
    testExpression("toInteger('-0xca11ab1e')", Expr.Integer(-0xCA11AB1EL))
    testExpression("toInteger('-0x0')", Expr.Integer(0L))

    // Cypher hex literal equivalence
    testExpression("toInteger('-0x12') = -0x12", Expr.True)
    testExpression("toInteger('0xf00') = 0xf00", Expr.True)

    testExpression("toInteger('9223372036854775806.2')", Expr.Integer(9223372036854775806L))

    testExpression("toInteger('bogus')", Expr.Null)
    testExpression("toInteger(' 123 ')", Expr.Null)
  }

  describe("`toFloat` function") {
    testExpression("toFloat(123)", Expr.Floating(123.0))

    testExpression("toFloat(123.0)", Expr.Floating(123.0))
    testExpression("toFloat(123.3)", Expr.Floating(123.3))
    testExpression("toFloat(123.7)", Expr.Floating(123.7))
    testExpression("toFloat(-123.3)", Expr.Floating(-123.3))
    testExpression("toFloat(-123.7)", Expr.Floating(-123.7))

    testExpression("toFloat('123')", Expr.Floating(123.0))
    testExpression("toFloat('123.0')", Expr.Floating(123.0))
    testExpression("toFloat('123.3')", Expr.Floating(123.3))
    testExpression("toFloat('123.7')", Expr.Floating(123.7))
    testExpression("toFloat('-123.3')", Expr.Floating(-123.3))
    testExpression("toFloat('-123.7')", Expr.Floating(-123.7))

    testExpression("toFloat('9223372036854775806.2')", Expr.Floating(9223372036854776000.0))

    testExpression("toFloat('bogus')", Expr.Null)
    testExpression("toFloat(' 123 ')", Expr.Floating(123.0)) // yes, I know this doesn't match `toInteger`
  }

  describe("`text.utf8Decode` function") {
    testExpression(
      "text.utf8Decode(bytes('6162206364'))",
      Expr.Str("ab cd"),
    )
    testExpression(
      "text.utf8Decode(bytes('5765204469646E2774205374617274207468652046697265'))",
      Expr.Str("We Didn't Start the Fire"),
    )
    testExpression(
      "text.utf8Decode(bytes('F09F8C88'))",
      Expr.Str("\uD83C\uDF08"), // 🌈
    )
    testExpression(
      "text.utf8Decode(bytes('E4BDA0E5A5BDE4B896E7958C'))",
      Expr.Str("你好世界"),
    )
  }

  describe("`text.utf8Encode` function") {
    testExpression(
      """text.utf8Encode("ab cd")""",
      Expr.Bytes(Array(0x61, 0x62, 0x20, 0x63, 0x64).map(_.toByte)),
    )
    testExpression(
      """text.utf8Encode("We Didn't Start the Fire")""",
      Expr.Bytes(
        Array(0x57, 0x65, 0x20, 0x44, 0x69, 0x64, 0x6E, 0x27, 0x74, 0x20, 0x53, 0x74, 0x61, 0x72, 0x74, 0x20, 0x74,
          0x68, 0x65, 0x20, 0x46, 0x69, 0x72, 0x65).map(_.toByte),
      ),
    )
    testExpression(
      """text.utf8Encode("🌈")""", // \uD83C\uDF08
      Expr.Bytes(Array(0xF0, 0x9F, 0x8C, 0x88).map(_.toByte)),
    )
    testExpression(
      """text.utf8Encode("你好世界")""",
      Expr.Bytes(Array(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD, 0xE4, 0xB8, 0x96, 0xE7, 0x95, 0x8C).map(_.toByte)),
    )
  }
  describe("getHost function") {
    // [[CypherHarness]] uses a non-namespaced ID provider, so all IDs will be assigned position index "None" aka null
    testExpression("getHost(idFrom(-1))", Expr.Null, expectedIsIdempotent = false)
  }
}
