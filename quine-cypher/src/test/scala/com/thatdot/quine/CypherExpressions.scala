package com.thatdot.quine.compiler.cypher

import org.scalactic.source.Position

import com.thatdot.quine.graph.cypher.{CypherException, Expr}

class CypherExpressions extends CypherHarness("cypher-expression-tests") {

  /** Check that a given boolean operator has the expected output for all inputs
    *
    * @param componentToTest extract the operator from the truth table row
    * @param buildExpression construct the boolean cypher expression
    * @param pos source position of the call to `testBooleanOperator`
    */
  private def testBooleanOperator(
    componentToTest: TruthTableRow => Expr.Bool,
    buildExpression: (String, String) => String
  )(implicit
    pos: Position
  ): Unit = {
    val printBool: Expr.Bool => String = {
      case Expr.False => "false"
      case Expr.Null => "null"
      case Expr.True => "true"
    }

    val exprsSeen = scala.collection.mutable.Set.empty[String]
    for {
      row <- booleanOperators
      expr = buildExpression(printBool(row.lhs), printBool(row.rhs))
      if exprsSeen.add(expr)
    } testExpression(
      buildExpression("x", "y"),
      componentToTest(row),
      queryPreamble = s"WITH ${printBool(row.lhs)} AS x, ${printBool(row.rhs)} AS y RETURN "
    )(pos)
  }

  /** Given two inputs, what are the expected outputs for all boolean operators */
  private case class TruthTableRow(
    lhs: Expr.Bool,
    rhs: Expr.Bool,
    and: Expr.Bool,
    or: Expr.Bool,
    xor: Expr.Bool,
    not: Expr.Bool
  )

  // https://neo4j.com/docs/cypher-manual/current/syntax/operators/#query-operators-boolean
  private val booleanOperators: Vector[TruthTableRow] = Vector(
    TruthTableRow(Expr.False, Expr.False, Expr.False, Expr.False, Expr.False, Expr.True),
    TruthTableRow(Expr.False, Expr.Null, Expr.False, Expr.Null, Expr.Null, Expr.True),
    TruthTableRow(Expr.False, Expr.True, Expr.False, Expr.True, Expr.True, Expr.True),
    TruthTableRow(Expr.True, Expr.False, Expr.False, Expr.True, Expr.True, Expr.False),
    TruthTableRow(Expr.True, Expr.Null, Expr.Null, Expr.True, Expr.Null, Expr.False),
    TruthTableRow(Expr.True, Expr.True, Expr.True, Expr.True, Expr.False, Expr.False),
    TruthTableRow(Expr.Null, Expr.False, Expr.False, Expr.Null, Expr.Null, Expr.Null),
    TruthTableRow(Expr.Null, Expr.Null, Expr.Null, Expr.Null, Expr.Null, Expr.Null),
    TruthTableRow(Expr.Null, Expr.True, Expr.Null, Expr.True, Expr.Null, Expr.Null)
  )

  describe("Neo4j bugs") {
    testExpression(
      "+null",
      Expr.Null,
      expectedCannotFail = true
    )
  }

  describe("AND operator") {
    testBooleanOperator(_.and, (lhs, rhs) => s"$lhs AND $rhs")
  }

  describe("OR operator") {
    testBooleanOperator(_.or, (lhs, rhs) => s"$lhs OR $rhs")
  }

  describe("XOR operator") {
    testBooleanOperator(_.xor, (lhs, rhs) => s"$lhs XOR $rhs")
  }

  describe("NOT operator") {
    testBooleanOperator(_.not, (lhs, rhs) => s"NOT $lhs")
  }

  describe("`abs` function") {
    testExpression("abs(1.3)", Expr.Floating(1.3))
    testExpression("abs(-4.3)", Expr.Floating(4.3))
    testExpression("abs(-4)", Expr.Integer(4L))
  }

  describe("`sign` function") {
    testExpression("sign(1.3)", Expr.Integer(1L))
    testExpression("sign(-4.3)", Expr.Integer(-1L))
    testExpression("sign(-4)", Expr.Integer(-1L))
    testExpression("sign(-0.0)", Expr.Integer(0L))
    testExpression("sign(0)", Expr.Integer(0L))
  }

  describe("`toLower` function") {
    testExpression("toLower(\"hello\")", Expr.Str("hello"))
    testExpression("toLower(\"HELLO\")", Expr.Str("hello"))
    testExpression("toLower(\"Hello\")", Expr.Str("hello"))
  }

  describe("`toUpper` function") {
    testExpression("toUpper(\"hello\")", Expr.Str("HELLO"))
    testExpression("toUpper(\"HELLO\")", Expr.Str("HELLO"))
    testExpression("toUpper(\"Hello\")", Expr.Str("HELLO"))
  }

  describe("`pi` function") {
    testExpression("pi()", Expr.Floating(Math.PI))
  }

  describe("`e` function") {
    testExpression("e()", Expr.Floating(Math.E))
  }

  describe("`toString` function") {
    testExpression("toString('hello')", Expr.Str("hello"))
    testExpression("toString(123)", Expr.Str("123"))
    testExpression("toString(12.3)", Expr.Str("12.3"))
    testExpression("toString(true)", Expr.Str("true"))
  }

  describe("`head` function") {
    testExpression("head([1,2,3])", Expr.Integer(1L))
    testExpression("head([])", Expr.Null)
  }

  describe("`last` function") {
    testExpression("last([1,2,3])", Expr.Integer(3L))
    testExpression("last([])", Expr.Null)
  }

  describe("`tail` function") {
    testExpression("tail([1,2,3])", Expr.List(Vector(Expr.Integer(2L), Expr.Integer(3L))))
    testExpression("tail([])", Expr.List(Vector.empty))
  }

  describe("`size` function") {
    testExpression("size([1,2,3])", Expr.Integer(3L))
    testExpression("size([])", Expr.Integer(0L))
    testExpression("size(\"hello\")", Expr.Integer(5L))
    testExpression("size(\"\")", Expr.Integer(0L))
  }

  describe("`range` function") {
    testExpression(
      "range(1, 10)",
      Expr.List((1 to 10).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression(
      "range(1, 10, 2)",
      Expr.List((1 to 10 by 2).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testExpression(
      "range(1, 10, 3)",
      Expr.List((1 to 10 by 3).map(i => Expr.Integer(i.toLong)).toVector)
    )
  }

  describe("`[]` operator for lists") {

    testExpression("x[4]", Expr.Null, expectedIsIdempotent = true, queryPreamble = "with [1,2,3] as x return ")

    testExpression("x[1]", Expr.Integer(2L), expectedIsIdempotent = true, queryPreamble = "with [1,2,3] as x return ")

    // Python style last element
    testExpression("x[-1]", Expr.Integer(3L), expectedIsIdempotent = true, queryPreamble = "with [1,2,3] as x return ")

    // Negative out of bounds
    testExpression("x[-4]", Expr.Null, expectedIsIdempotent = true, queryPreamble = "with [1,2,3] as x return ")
  }

  describe("splitting strings") {
    // Substring based
    testExpression(
      "split('123.456.789.012', '.')",
      Expr.List(Expr.Str("123"), Expr.Str("456"), Expr.Str("789"), Expr.Str("012"))
    )

    // Regex based
    testExpression(
      "text.split('123.456,789==012', '[.,]|==')",
      Expr.List(Expr.Str("123"), Expr.Str("456"), Expr.Str("789"), Expr.Str("012"))
    )
    testExpression(
      "text.split('123,456,789', ',', 2)",
      Expr.List(Expr.Str("123"), Expr.Str("456,789"))
    )
  }

  describe("regex") {
    testExpression(
      """text.regexFirstMatch('a,b', '(\\w),(\\w)')""",
      Expr.List(Expr.Str("a,b"), Expr.Str("a"), Expr.Str("b"))
    )

    val apacheLogExample =
      """209.85.238.199 - - [18/May/2015:11:05:59 +0000] "GET /?flav=atom HTTP/1.1" 200 32352 "-" "Feedfetcher-Google; (+http://www.google.com/feedfetcher.html; 16 subscribers; feed-id=3389821348893992437)""""
    val apacheLogRegex =
      """(\\S+)\\s+\\S+\\s+(\\S+)\\s+\\[(.+)\\]\\s+"(.*)"\\s+([0-9]+)\\s+(\\S+)\\s+"(.*)"\\s+"(.*)"\\s*\\Z"""
    testExpression(
      s"text.regexFirstMatch('$apacheLogExample', '$apacheLogRegex')",
      Expr.List(
        Expr.Str(apacheLogExample),
        Expr.Str("209.85.238.199"),
        Expr.Str("-"),
        Expr.Str("18/May/2015:11:05:59 +0000"),
        Expr.Str("GET /?flav=atom HTTP/1.1"),
        Expr.Str("200"),
        Expr.Str("32352"),
        Expr.Str("-"),
        Expr.Str(
          "Feedfetcher-Google; (+http://www.google.com/feedfetcher.html; 16 subscribers; feed-id=3389821348893992437)"
        )
      )
    )

    val pocExampleText = "abc <link xxx1>yyy1</link> def <link xxx2>yyy2</link>"
    val pocExampleRegex = """<link (\\w+)>(\\w+)</link>"""
    testExpression(
      s"text.regexFirstMatch('$pocExampleText', '$pocExampleRegex')",
      Expr.List(Vector(Expr.Str("<link xxx1>yyy1</link>"), Expr.Str("xxx1"), Expr.Str("yyy1")))
    )

    // no match
    testExpression(
      s"text.regexFirstMatch('foo', 'bar')",
      Expr.List()
    )
  }

  describe("map projections") {

    testQuery(
      "with { foo: 1, bar: 'hi' } as m return m { .age, baz: m.foo + 1 }",
      expectedColumns = Vector("m"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            Map(
              "age" -> Expr.Null,
              "baz" -> Expr.Integer(2L)
            )
          )
        )
      )
    )

    testQuery(
      "with { foo: 1, bar: 'hi' } as m, 1.2 as quz return m { .age, baz: m.foo + 1, quz, .* }",
      expectedColumns = Vector("m"),
      expectedRows = Seq(
        Vector(
          Expr.Map(
            Map(
              "age" -> Expr.Null,
              "foo" -> Expr.Integer(1L),
              "bar" -> Expr.Str("hi"),
              "baz" -> Expr.Integer(2L),
              "quz" -> Expr.Floating(1.2)
            )
          )
        )
      )
    )

    testQuery(
      "with NULL as m return m { .age, baz: 987, .* }",
      expectedColumns = Vector("m"),
      expectedRows = Seq(Vector(Expr.Null))
    )
  }

  describe("CASE") {

    testQuery(
      "WITH 3 as x, 2 as y RETURN (CASE ((x + 1) - y >= 0) WHEN true THEN y ELSE x END) as z",
      expectedColumns = Vector("z"),
      expectedRows = Seq(Vector(Expr.Integer(2L)))
    )

    testQuery(
      "WITH 3 as x, 2 as y RETURN (CASE WHEN ((x + 1) - y >= 0) THEN y ELSE x END) as z",
      expectedColumns = Vector("z"),
      expectedRows = Seq(Vector(Expr.Integer(2L)))
    )

    testQuery(
      "WITH 3 as x, null as y RETURN (CASE WHEN ((x + 1) - y >= 0) THEN y ELSE x END) as z",
      expectedColumns = Vector("z"),
      expectedRows = Seq(Vector(Expr.Integer(3L)))
    )

    testQuery(
      "WITH null as x, 7 as y RETURN (CASE WHEN ((x + 1) - y >= 0) THEN y ELSE x END) as z",
      expectedColumns = Vector("z"),
      expectedRows = Seq(Vector(Expr.Null))
    )

    testQuery(
      "WITH 3 as x, 2 as y RETURN (CASE x*2+y*2 WHEN x+y THEN 'one' WHEN 2*(x+y) THEN 'two' ELSE 'three' END) as z",
      expectedColumns = Vector("z"),
      expectedRows = Seq(Vector(Expr.Str("two")))
    )

    testQuery(
      "RETURN CASE 2.0 WHEN 2 THEN 'equal' ELSE 'not-equal' END AS answer",
      expectedColumns = Vector("answer"),
      expectedRows = Seq(Vector(Expr.Str("equal"))),
      expectedCannotFail = true
    )

    testQuery(
      "RETURN CASE toInteger(NULL) WHEN NULL THEN 'equal' ELSE 'not-equal' END AS answer",
      expectedColumns = Vector("answer"),
      expectedRows = Seq(Vector(Expr.Str("equal")))
    )
  }

  /* TODO: add functions that test error messages:
   *
   * 9223372036854775804 + 1      // 9223372036854775805
   * with null as x return x.foo  // null
   * +"hi"                        // type error
   * with [1,2,3] as x return x[9223372036854775807]
   */

  describe("Errors") {
    assertQueryExecutionFailure(
      "UNWIND [1] AS x RETURN 9223372036854775807 + x",
      CypherException.Arithmetic(
        wrapping = "long overflow",
        operands = Seq(Expr.Integer(9223372036854775807L), Expr.Integer(1L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [1] AS x RETURN -9223372036854775808 - x",
      CypherException.Arithmetic(
        wrapping = "long overflow",
        operands = Seq(Expr.Integer(-9223372036854775808L), Expr.Integer(1L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [-9223372036854775808] AS x RETURN -x",
      CypherException.Arithmetic(
        wrapping = "long overflow",
        operands = Seq(Expr.Integer(0L), Expr.Integer(-9223372036854775808L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [0] AS x RETURN 500 / x",
      CypherException.Arithmetic(
        wrapping = "/ by zero",
        operands = Seq(Expr.Integer(500L), Expr.Integer(0L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [0] AS x RETURN 500 % x",
      CypherException.Arithmetic(
        wrapping = "/ by zero",
        operands = Seq(Expr.Integer(500L), Expr.Integer(0L))
      )
    )

    assertQueryExecutionFailure(
      "UNWIND [922337203685] AS x RETURN x * 45938759384",
      CypherException.Arithmetic(
        wrapping = "long overflow",
        operands = Seq(Expr.Integer(922337203685L), Expr.Integer(45938759384L))
      )
    )
  }
}
