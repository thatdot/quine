package com.thatdot.quine.compiler.cypher

import org.scalactic.source.Position

import com.thatdot.quine.graph.cypher.{CypherException, Expr, SourceText}

/** Test suite for behaviors written in Cypher, but not necessarily for the Cypher interpreter itself.
  * Tests in this suite assume that basic statement composition and execution is correct, and focus on the behavior of
  * specific functions or clauses, particularly in edge cases. For more foundational behavior of the Cypher interpreter
  * itself, look to other instances of [[CypherHarness]]
  */
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

  describe("ceil and floor") {
    testExpression("ceil(1.0)", Expr.Floating(1L))
    testExpression("ceil(1.1)", Expr.Floating(2L))
    testExpression("ceil(1.9)", Expr.Floating(2L))
    testExpression("ceil(200.5)", Expr.Floating(201L))
    testExpression("ceil(-1.0)", Expr.Floating(-1L))
    testExpression("ceil(-1.1)", Expr.Floating(-1L))
    testExpression("ceil(-1.5)", Expr.Floating(-1L))

    testExpression("floor(1.0)", Expr.Floating(1L))
    testExpression("floor(1.1)", Expr.Floating(1L))
    testExpression("floor(1.9)", Expr.Floating(1L))
    testExpression("floor(200.5)", Expr.Floating(200L))
    testExpression("floor(-1.0)", Expr.Floating(-1L))
    testExpression("floor(-1.1)", Expr.Floating(-2L))
    testExpression("floor(-1.5)", Expr.Floating(-2L))
  }

  describe("rounding functions") {
    testExpression("round(1.0)", Expr.Floating(1L))
    testExpression("round(1.1)", Expr.Floating(1L))
    testExpression("round(1.9)", Expr.Floating(2L))
    testExpression("round(200.5)", Expr.Floating(201L))
    testExpression("round(-1.0)", Expr.Floating(-1L))
    testExpression("round(-1.1)", Expr.Floating(-1L))
    testExpression("round(-1.5)", Expr.Floating(-2L))

    testExpression("round(9)", Expr.Floating(9L))
    testExpression("ceil(-9)", Expr.Floating(-9L))
    testExpression("floor(102)", Expr.Floating(102L))

    // Rounding UP
    testExpression("round(5.5, 0, 'UP')", Expr.Floating(6d))
    testExpression("round(2.5, 0, 'UP')", Expr.Floating(3d))
    testExpression("round(1.6, 0, 'UP')", Expr.Floating(2d))
    testExpression("round(1.1, 0, 'UP')", Expr.Floating(2d))
    testExpression("round(1.0, 0, 'UP')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'UP')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'UP')", Expr.Floating(-2d))
    testExpression("round(-1.6, 0, 'UP')", Expr.Floating(-2d))
    testExpression("round(-2.5, 0, 'UP')", Expr.Floating(-3d))
    testExpression("round(-5.5, 0, 'UP')", Expr.Floating(-6d))

    // Rounding DOWN
    testExpression("round(5.5, 0, 'DOWN')", Expr.Floating(5d))
    testExpression("round(2.5, 0, 'DOWN')", Expr.Floating(2d))
    testExpression("round(1.6, 0, 'DOWN')", Expr.Floating(1d))
    testExpression("round(1.1, 0, 'DOWN')", Expr.Floating(1d))
    testExpression("round(1.0, 0, 'DOWN')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'DOWN')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'DOWN')", Expr.Floating(-1d))
    testExpression("round(-1.6, 0, 'DOWN')", Expr.Floating(-1d))
    testExpression("round(-2.5, 0, 'DOWN')", Expr.Floating(-2d))
    testExpression("round(-5.5, 0, 'DOWN')", Expr.Floating(-5d))

    // Rounding CEILING
    testExpression("round(5.5, 0, 'CEILING')", Expr.Floating(6d))
    testExpression("round(2.5, 0, 'CEILING')", Expr.Floating(3d))
    testExpression("round(1.6, 0, 'CEILING')", Expr.Floating(2d))
    testExpression("round(1.1, 0, 'CEILING')", Expr.Floating(2d))
    testExpression("round(1.0, 0, 'CEILING')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'CEILING')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'CEILING')", Expr.Floating(-1d))
    testExpression("round(-1.6, 0, 'CEILING')", Expr.Floating(-1d))
    testExpression("round(-2.5, 0, 'CEILING')", Expr.Floating(-2d))
    testExpression("round(-5.5, 0, 'CEILING')", Expr.Floating(-5d))

    // Rounding FLOOR
    testExpression("round(5.5, 0, 'FLOOR')", Expr.Floating(5d))
    testExpression("round(2.5, 0, 'FLOOR')", Expr.Floating(2d))
    testExpression("round(1.6, 0, 'FLOOR')", Expr.Floating(1d))
    testExpression("round(1.1, 0, 'FLOOR')", Expr.Floating(1d))
    testExpression("round(1.0, 0, 'FLOOR')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'FLOOR')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'FLOOR')", Expr.Floating(-2d))
    testExpression("round(-1.6, 0, 'FLOOR')", Expr.Floating(-2d))
    testExpression("round(-2.5, 0, 'FLOOR')", Expr.Floating(-3d))
    testExpression("round(-5.5, 0, 'FLOOR')", Expr.Floating(-6d))

    // Rounding HALF_UP
    testExpression("round(5.5, 0, 'HALF_UP')", Expr.Floating(6d))
    testExpression("round(2.5, 0, 'HALF_UP')", Expr.Floating(3d))
    testExpression("round(1.6, 0, 'HALF_UP')", Expr.Floating(2d))
    testExpression("round(1.1, 0, 'HALF_UP')", Expr.Floating(1d))
    testExpression("round(1.0, 0, 'HALF_UP')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'HALF_UP')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'HALF_UP')", Expr.Floating(-1d))
    testExpression("round(-1.6, 0, 'HALF_UP')", Expr.Floating(-2d))
    testExpression("round(-2.5, 0, 'HALF_UP')", Expr.Floating(-3d))
    testExpression("round(-5.5, 0, 'HALF_UP')", Expr.Floating(-6d))

    // Rounding HALF_DOWN
    testExpression("round(5.5, 0, 'HALF_DOWN')", Expr.Floating(5d))
    testExpression("round(2.5, 0, 'HALF_DOWN')", Expr.Floating(2d))
    testExpression("round(1.6, 0, 'HALF_DOWN')", Expr.Floating(2d))
    testExpression("round(1.1, 0, 'HALF_DOWN')", Expr.Floating(1d))
    testExpression("round(1.0, 0, 'HALF_DOWN')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'HALF_DOWN')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'HALF_DOWN')", Expr.Floating(-1d))
    testExpression("round(-1.6, 0, 'HALF_DOWN')", Expr.Floating(-2d))
    testExpression("round(-2.5, 0, 'HALF_DOWN')", Expr.Floating(-2d))
    testExpression("round(-5.5, 0, 'HALF_DOWN')", Expr.Floating(-5d))

    // Rounding HALF_EVEN
    testExpression("round(5.5, 0, 'HALF_EVEN')", Expr.Floating(6d))
    testExpression("round(2.5, 0, 'HALF_EVEN')", Expr.Floating(2d))
    testExpression("round(1.6, 0, 'HALF_EVEN')", Expr.Floating(2d))
    testExpression("round(1.1, 0, 'HALF_EVEN')", Expr.Floating(1d))
    testExpression("round(1.0, 0, 'HALF_EVEN')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'HALF_EVEN')", Expr.Floating(-1d))
    testExpression("round(-1.1, 0, 'HALF_EVEN')", Expr.Floating(-1d))
    testExpression("round(-1.6, 0, 'HALF_EVEN')", Expr.Floating(-2d))
    testExpression("round(-2.5, 0, 'HALF_EVEN')", Expr.Floating(-2d))
    testExpression("round(-5.5, 0, 'HALF_EVEN')", Expr.Floating(-6d))

    // Rounding UNNECESSARY
    val roundingException = new ArithmeticException("Rounding necessary")
    assertQueryExecutionFailure("RETURN round(5.5, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(2.5, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(1.6, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(1.1, 0, 'UNNECESSARY')", roundingException)
    testExpression("round(1.0, 0, 'UNNECESSARY')", Expr.Floating(1d))
    testExpression("round(-1.0, 0, 'UNNECESSARY')", Expr.Floating(-1d))
    assertQueryExecutionFailure("RETURN round(-1.1, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(-1.6, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(-2.5, 0, 'UNNECESSARY')", roundingException)
    assertQueryExecutionFailure("RETURN round(-5.5, 0, 'UNNECESSARY')", roundingException)

    // Test rounding with precision and Scala BigNumber rounding.
    // cf.: https://stackoverflow.com/questions/42396509/roundingmode-half-up-difference-in-scala-and-java
    testExpression("round(8409.3555, 3)", Expr.Floating(8409.356d)) // Java BigNumber would round to `8409.355`
    testExpression("round(8409.3555, -3)", Expr.Floating(8000d))
    testExpression("round(8409.3555, 0)", Expr.Floating(8409d))
    testExpression("round(8509.3555, -3)", Expr.Floating(9000d))
    testExpression("round(8509.3555, -3, 'DOWN')", Expr.Floating(8000d))
    testExpression("round(8499.3555, -3, 'HALF_UP')", Expr.Floating(8000d))
    testExpression("round(8499.3555, -3, 'UP')", Expr.Floating(9000d))

    // Test equivalence of default parameters for different signatures.
    testExpression("round(8409.3555, 0) = round(8409.3555)", Expr.Bool(true))
    testExpression("round(8409.3555, 0, 'HALF_UP') = round(8409.3555)", Expr.Bool(true))
  }

  describe("`pi` function") {
    testExpression("pi()", Expr.Floating(Math.PI))
  }
  describe("`radians` function") {
    testExpression("radians(180) = pi()", Expr.True)
    testExpression("radians(360) = 2*pi()", Expr.True)
    testExpression("radians(-180 + 0.0001) + pi() < 0.001", Expr.True)
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

    //Make sure we throw the correct error when passing an invalid regest to regexFirstMatch
    assertQueryExecutionFailure(
      "RETURN text.regexFirstMatch('hello', '(')",
      CypherException.ConstraintViolation("Unclosed group near index 1\n(", None)
    )

  }

  describe("url decoding") {
    // RFC3986
    testExpression("""text.urldecode("foo", false)""", Expr.Str("foo"))
    testExpression("""text.urldecode("%2F%20%5e", false)""", Expr.Str("/ ^"))
    testExpression("""text.urldecode("hello%2C%20world", false)""", Expr.Str("hello, world"))
    testExpression("""text.urldecode("%68%65%6C%6C%6F, %77%6F%72%6C%64", false)""", Expr.Str("hello, world"))
    testExpression("""text.urldecode("+", false)""", Expr.Str("+"))
    testExpression("""text.urldecode("%25", false)""", Expr.Str("%"))
    testExpression("""text.urldecode("%%", false)""", Expr.Null) // malformed under RFC3986
    // x-www-form-urlencoded
    testExpression("""text.urldecode("foo")""", Expr.Str("foo"))
    testExpression("""text.urldecode("%2F%20%5e")""", Expr.Str("/ ^")) // %20 still works
    testExpression("""text.urldecode("hello%2C+world")""", Expr.Str("hello, world")) // but + can be used too
    testExpression("""text.urldecode("%68%65%6C%6C%6F, %77%6F%72%6C%64")""", Expr.Str("hello, world"))
    testExpression("""text.urldecode("+")""", Expr.Str(" "))
    testExpression("""text.urldecode("%25")""", Expr.Str("%"))
    testExpression("""text.urldecode("%%")""", Expr.Null) // malformed under x-www-form-urlencoded
  }

  describe("url encoding") {
    // RFC3986 + "{}
    testExpression("""text.urlencode("hello, world")""", Expr.Str("hello%2C%20world"))
    testExpression(
      """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle')""",
      Expr.Str("MATCH%20%28n%29%20WHERE%20strId%28n%29%20%3D%20%2212345678%2F54321%22%20RETURN%20n.foo%20AS%20fiddle")
    )
    testExpression("""text.urlencode("%")""", Expr.Str("%25"))
    testExpression(
      """text.urlencode('MATCH(missEvents:missEvents) WHERE id(missEvents)="d75db269-41cb-3439-8810-085a8fe85c2e" MATCH (event {cache_class:"MISS"})-[:TARGETED]->(server) RETURN server, event LIMIT 10')""",
      Expr.Str(
        """MATCH%28missEvents%3AmissEvents%29%20WHERE%20id%28missEvents%29%3D%22d75db269-41cb-3439-8810-085a8fe85c2e%22%20MATCH%20%28event%20%7Bcache_class%3A%22MISS%22%7D%29-%5B%3ATARGETED%5D-%3E%28server%29%20RETURN%20server%2C%20event%20LIMIT%2010"""
      )
    )

    // RFC3986
    testExpression(
      """text.urlencode("MATCH (n) WHERE strId(n) = '12345678/54321' RETURN n.foo AS fiddle")""",
      Expr.Str("MATCH%20%28n%29%20WHERE%20strId%28n%29%20%3D%20%2712345678%2F54321%27%20RETURN%20n.foo%20AS%20fiddle")
    )
    testExpression(
      """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', '')""",
      Expr.Str("""MATCH%20%28n%29%20WHERE%20strId%28n%29%20%3D%20"12345678%2F54321"%20RETURN%20n.foo%20AS%20fiddle""")
    )
    testExpression(
      """text.urlencode('MATCH(missEvents:missEvents) WHERE id(missEvents)="d75db269-41cb-3439-8810-085a8fe85c2e" MATCH (event {cache_class:"MISS"})-[:TARGETED]->(server) RETURN server, event LIMIT 10', '')""",
      Expr.Str(
        """MATCH%28missEvents%3AmissEvents%29%20WHERE%20id%28missEvents%29%3D"d75db269-41cb-3439-8810-085a8fe85c2e"%20MATCH%20%28event%20{cache_class%3A"MISS"}%29-%5B%3ATARGETED%5D->%28server%29%20RETURN%20server%2C%20event%20LIMIT%2010"""
      )
    )

    // x-www-form-urlencoded + "{}
    testExpression("""text.urlencode("hello, world", true)""", Expr.Str("hello%2C+world"))
    testExpression("""text.urlencode("%", true)""", Expr.Str("%25"))
    testExpression(
      """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', true)""",
      Expr.Str("MATCH+%28n%29+WHERE+strId%28n%29+%3D+%2212345678%2F54321%22+RETURN+n.foo+AS+fiddle")
    )

    // x-www-form-urlencoded
    testExpression(
      """text.urlencode("MATCH (n) WHERE strId(n) = '12345678/54321' RETURN n.foo AS fiddle", true)""",
      Expr.Str("MATCH+%28n%29+WHERE+strId%28n%29+%3D+%2712345678%2F54321%27+RETURN+n.foo+AS+fiddle")
    )
    testExpression(
      """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', true, '')""",
      Expr.Str("""MATCH+%28n%29+WHERE+strId%28n%29+%3D+"12345678%2F54321"+RETURN+n.foo+AS+fiddle""")
    )
  }

  describe("runtime type checking") {
    testExpression("meta.type(1)", Expr.Str("INTEGER"))
    testExpression("meta.type(1.0)", Expr.Str("FLOAT"))
    testExpression("meta.type('bazinga')", Expr.Str("STRING"))
    testExpression("meta.type([1, 2, 3])", Expr.Str("LIST OF ANY"))
    // meta.type edge case: Note that the "calling a function with NULL" rule skips the function entirely, whenever
    // cypher is clever enough to pick up on it
    testExpression("meta.type(null)", Expr.Null)
  }

  describe("simple assertion-based runtime type casting") {
    testExpression("castOrThrow.integer(1)", Expr.Integer(1))
    testExpression("castOrThrow.integer(n)", Expr.Integer(1), queryPreamble = "UNWIND [1] AS n RETURN ")
    testQuery(
      "UNWIND [1, 2, 3] AS n RETURN castOrThrow.integer(n) AS cast",
      Vector("cast"),
      Vector(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3))
      )
    )
  }

  describe("simple null-on-failure runtime type casting") {
    testExpression("castOrNull.integer(1)", Expr.Integer(1))
    testExpression("castOrNull.integer(2.0)", Expr.Null)
    testExpression("castOrNull.integer(n)", Expr.Integer(1), queryPreamble = "UNWIND [1] AS n RETURN ")
    testQuery(
      "UNWIND [1, 2, 3] AS n RETURN castOrThrow.integer(n) AS cast",
      Vector("cast"),
      Vector(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3))
      )
    )
    testQuery(
      "UNWIND [1, 2, 'tortoise', 8675309] AS n RETURN castOrNull.integer(n) AS cast",
      Vector("cast"),
      Vector(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Null),
        Vector(Expr.Integer(8675309))
      )
    )
  }

  describe("runtime casts to circumvent cypher limitations") {
    val testJson =
      """{
        |  "hello": "world",
        |  "arr": [1, 2, 3],
        |  "sub": {
        |    "object": {},
        |    "bool": true
        |  }
        |}""".stripMargin.replace('\n', ' ').replace(" ", "")
    val testMap = Expr.Map(
      "hello" -> Expr.Str("world"),
      "arr" -> Expr.List(Expr.Integer(1), Expr.Integer(2), Expr.Integer(3)),
      "sub" -> Expr.Map(
        "object" -> Expr.Map.empty,
        "bool" -> Expr.True
      )
    )

    // verification that the test case is coherent
    testExpression(s"parseJson('$testJson')", testMap)

    // verification that castOrThrow.map performs basic functionality
    testQuery(
      s"WITH parseJson('$testJson') AS json RETURN castOrThrow.map(json) AS j",
      expectedColumns = Vector("j"),
      Vector(
        Vector(
          testMap
        )
      )
    )

    // This is the first real test: using the parsed value directly with UNWIND is possible
    val failedUnwind = s"WITH parseJson('$testJson') AS json UNWIND keys(json) AS key RETURN key"
    assertStaticQueryFailure(
      failedUnwind,
      CypherException.Compile(
        "Type mismatch: expected Map, Node or Relationship but was Any",
        Some(
          com.thatdot.quine.graph.cypher.Position(1, 103, 102, SourceText(failedUnwind))
        )
      )
    )
    // But with castOrThrow, all is well:
    testQuery(
      s"WITH parseJson('$testJson') AS json UNWIND keys(castOrThrow.map(json)) AS key RETURN key",
      Vector("key"),
      Vector(
        Vector(Expr.Str("hello")),
        Vector(Expr.Str("arr")),
        Vector(Expr.Str("sub"))
      ),
      ordered = false
    )
  }

  describe("regression test type inference bug from thatdot/quine#9") {
    testQuery(
      """
        |// Setup query
        |MATCH (n) WHERE id(n) = idFrom(-2439) SET n = {
        |  tags: {
        |    foo: "bar",
        |    fizz: "buzz"
        |  }
        |}
        |WITH n
        |UNWIND keys(castOrThrow.map(n.tags)) AS key
        |RETURN key
        |""".stripMargin,
      Vector("key"),
      Vector(
        Vector(Expr.Str("foo")),
        Vector(Expr.Str("fizz"))
      ),
      expectedIsReadOnly = false,
      expectedIsIdempotent = true,
      ordered = false
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

    // cast failure
    assertQueryExecutionFailure(
      "RETURN castOrThrow.integer(2.0)",
      CypherException.Runtime(
        s"Cast failed: Cypher execution engine is unable to determine that Floating(2.0) is a valid INTEGER"
      )
    )
  }
}
