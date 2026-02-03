package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class ExpressionVisitorTests extends munit.FunSuite {

  def parseExpression(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_Expression()

    ExpressionVisitor.visitOC_Expression(tree)
  }

  test("clusterPosition()") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 16),
      name = Symbol("clusterPosition"),
      args = Nil,
      ty = None,
    )

    val actual = parseExpression("clusterPosition()")

    assertEquals(actual, expected)
  }

  test("+null") {
    val expected = Expression.UnaryOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Plus,
      exp = Expression.AtomicLiteral(
        source = Source.TextSource(start = 1, end = 4),
        value = Value.Null,
        ty = None,
      ),
      ty = None,
    )

    val actual = parseExpression("+null")

    assertEquals(actual, expected)
  }

  test("abs(1.3)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 7),
      name = Symbol("abs"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 4, end = 6),
          value = Value.Real(1.3),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("abs(1.3)")

    assertEquals(actual, expected)
  }

  test("abs(-4.3)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 8),
      name = Symbol("abs"),
      args = List(
        Expression.UnaryOp(
          source = Source.TextSource(start = 4, end = 7),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 5, end = 7),
            value = Value.Real(4.3),
            ty = None,
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("abs(-4.3)")

    assertEquals(actual, expected)
  }

  test("abs(-4)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 6),
      name = Symbol("abs"),
      args = List(
        Expression.UnaryOp(
          source = Source.TextSource(start = 4, end = 5),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 5, end = 5),
            value = Value.Integer(4),
            ty = None,
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("abs(-4)")

    assertEquals(actual, expected)
  }

  test("sign(1.3)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 8),
      name = Symbol("sign"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 5, end = 7),
          value = Value.Real(1.3),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("sign(1.3)")

    assertEquals(actual, expected)
  }

  test("sign(-4.3)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 9),
      name = Symbol("sign"),
      args = List(
        Expression.UnaryOp(
          source = Source.TextSource(start = 5, end = 8),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 6, end = 8),
            value = Value.Real(4.3),
            ty = None,
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("sign(-4.3)")

    assertEquals(actual, expected)
  }

  test("sign(-4)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 7),
      name = Symbol("sign"),
      args = List(
        Expression.UnaryOp(
          source = Source.TextSource(start = 5, end = 6),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 6, end = 6),
            value = Value.Integer(4),
            ty = None,
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("sign(-4)")

    assertEquals(actual, expected)
  }

  test("sign(-0.0)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 9),
      name = Symbol("sign"),
      args = List(
        Expression.UnaryOp(
          source = Source.TextSource(start = 5, end = 8),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 6, end = 8),
            value = Value.Real(0.0),
            ty = None,
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("sign(-0.0)")

    assertEquals(actual, expected)
  }

  test("sign(0)") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 6),
      name = Symbol("sign"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 5, end = 5),
          value = Value.Integer(0),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("sign(0)")

    assertEquals(actual, expected)
  }

  test("toLower(\"hello\")") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 15),
      name = Symbol("toLower"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 8, end = 14),
          value = Value.Text("hello"),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("toLower(\"hello\")")

    assertEquals(actual, expected)
  }

  test("toLower(\"HELLO\")") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 15),
      name = Symbol("toLower"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 8, end = 14),
          value = Value.Text("HELLO"),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("toLower(\"HELLO\")")

    assertEquals(actual, expected)
  }

  //  "toLower(\"Hello\")"
  //  "toUpper(\"hello\")"
  //  "toUpper(\"HELLO\")"
  //  "toUpper(\"Hello\")"

  test("pi()") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 3),
      name = Symbol("pi"),
      args = Nil,
      ty = None,
    )

    val actual = parseExpression("pi()")

    assertEquals(actual, expected)
  }

//  "e()"
//  "toString('hello')"
//  "toString(123)"
//  "toString(12.3)"
//  "toString(true)"

  test("head([1,2,3])") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 12),
      name = Symbol("head"),
      args = List(
        Expression.ListLiteral(
          source = Source.TextSource(start = 5, end = 11),
          value = List(
            Expression.AtomicLiteral(
              source = Source.TextSource(start = 6, end = 6),
              value = Value.Integer(1),
              ty = None,
            ),
            Expression.AtomicLiteral(
              source = Source.TextSource(start = 8, end = 8),
              value = Value.Integer(2),
              ty = None,
            ),
            Expression.AtomicLiteral(
              source = Source.TextSource(start = 10, end = 10),
              value = Value.Integer(3),
              ty = None,
            ),
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("head([1,2,3])")

    assertEquals(actual, expected)
  }
//  "head([])"
//  "last([1,2,3])"
//  "last([])"
//  "tail([1,2,3])"
//  "tail([])"
//  "size([1,2,3])"
//  "size([])"
//  "size(\"hello\")"
//  "size(\"\")"
//  "range(1, 10)"
//  "range(1, 10, 2)"
//  "range(1, 10, 3)"

  test("x[4]") {
    val actual = parseExpression("x[4]")
    val expected = Expression.IndexIntoArray(
      source = Source.TextSource(start = 1, end = 3),
      of = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("x"))),
        ty = None,
      ),
      index = Expression.AtomicLiteral(
        source = Source.TextSource(start = 2, end = 2),
        value = Value.Integer(n = 4),
        ty = None,
      ),
      ty = None,
    )

    assertEquals(actual, expected)
  }
//  "x[1]"
//  "x[-1]"
//  "x[-4]"
//  "split('123.456.789.012', '.')"
//  "text.split('123.456,789==012', '[.,]|==')"
//  "text.split('123,456,789', ',', 2)"
//  """text.regexFirstMatch('a,b', '(\\w),(\\w)')"""
//  s"text.regexFirstMatch('$apacheLogExample', '$apacheLogRegex')"
//  s"text.regexFirstMatch('$pocExampleText', '$pocExampleRegex')"
//  s"text.regexFirstMatch('foo', 'bar')"
//  """text.urldecode("foo", false)"""
//  """text.urldecode("%2F%20%5e", false)"""
//  """text.urldecode("hello%2C%20world", false)"""
//  """text.urldecode("%68%65%6C%6C%6F, %77%6F%72%6C%64", false)"""
//  """text.urldecode("+", false)"""
//  """text.urldecode("%25", false)"""
//  """text.urldecode("%%", false)"""
//  """text.urldecode("foo")"""
//  """text.urldecode("%2F%20%5e")"""
//  """text.urldecode("hello%2C+world")"""
//  """text.urldecode("%68%65%6C%6C%6F, %77%6F%72%6C%64")"""
//  """text.urldecode("+")"""
//  """text.urldecode("%25")"""
//  """text.urldecode("%%")"""
//  """text.urlencode("hello, world")"""
//  """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle')"""
//  """text.urlencode("%")"""
//  """text.urlencode('MATCH(missEvents:missEvents) WHERE id(missEvents)="d75db269-41cb-3439-8810-085a8fe85c2e" MATCH (event {cache_class:"MISS"})-[:TARGETED]->(server) RETURN server, event LIMIT 10')"""
//  """text.urlencode("MATCH (n) WHERE strId(n) = '12345678/54321' RETURN n.foo AS fiddle")"""
//  """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', '')"""
//  """text.urlencode('MATCH(missEvents:missEvents) WHERE id(missEvents)="d75db269-41cb-3439-8810-085a8fe85c2e" MATCH (event {cache_class:"MISS"})-[:TARGETED]->(server) RETURN server, event LIMIT 10', '')"""
//  """text.urlencode("hello, world", true)"""
//  """text.urlencode("%", true)"""
//  """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', true)"""
//  """text.urlencode("MATCH (n) WHERE strId(n) = '12345678/54321' RETURN n.foo AS fiddle", true)"""
//  """text.urlencode('MATCH (n) WHERE strId(n) = "12345678/54321" RETURN n.foo AS fiddle', true, '')"""
//  "meta.type(1)"
//  "meta.type(1.0)"
//  "meta.type('bazinga')"
//  "meta.type([1, 2, 3])"
//  "meta.type(null)"
//  "castOrThrow.integer(1)"
//  "castOrThrow.integer(n)"
//  "castOrNull.integer(1)"
//  "castOrNull.integer(2.0)"
//  "castOrNull.integer(n)"
//  "\"hello world\" STARTS WITH \"hell\""
//  "\"hello world\" STARTS WITH \"llo\""
//  "\"hello world\" STARTS WITH \"world\""
//  "\"hello world\" STARTS WITH NULL"
//  "NULL STARTS WITH \"hell\""
//  "\"hello world\" CONTAINS \"hell\""
//  "\"hello world\" CONTAINS \"llo\""
//  "\"hello world\" CONTAINS \"world\""
//  "\"hello world\" CONTAINS NULL"
//  "NULL CONTAINS \"hell\""
//  "\"hello world\" ENDS WITH \"hell\""
//  "\"hello world\" ENDS WITH \"llo\""
//  "\"hello world\" ENDS WITH \"world\""
//  "\"hello world\" ENDS WITH NULL"
//  "NULL ENDS WITH \"hell\""
//  "\"hello world\" =~ \"he[lo]{1,8} w.*\""
//  "\"hello world\" =~ \"he[lo]{1,2} w.*\""
//  "\"hello world\" =~ \"llo\""
//  "\"hello world\" =~ NULL"
//  "NULL =~ \"hell\""

  test("[0, 1, 2, 2 + 1, 4, 5, 6, 7, 8, 9]") {
    val actual = parseExpression("[0, 1, 2, 2 + 1, 4, 5, 6, 7, 8, 9]")

    val expected = Expression.ListLiteral(
      source = Source.TextSource(start = 0, end = 33),
      value = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(1, 1),
          value = Value.Integer(0),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(4, 4),
          value = Value.Integer(1),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(7, 7),
          value = Value.Integer(2),
          ty = None,
        ),
        Expression.BinOp(
          source = Source.TextSource(10, 14),
          op = Operator.Plus,
          lhs = Expression.AtomicLiteral(
            source = Source.TextSource(10, 10),
            value = Value.Integer(2),
            ty = None,
          ),
          rhs = Expression.AtomicLiteral(
            source = Source.TextSource(14, 14),
            value = Value.Integer(1),
            ty = None,
          ),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(17, 17),
          value = Value.Integer(4),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(20, 20),
          value = Value.Integer(5),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(23, 23),
          value = Value.Integer(6),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(26, 26),
          value = Value.Integer(7),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(29, 29),
          value = Value.Integer(8),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(32, 32),
          value = Value.Integer(9),
          ty = None,
        ),
      ),
      ty = None,
    )

    assertEquals(actual, expected)
  }

//  "[]"
//  "range(0, 10)[3]"
//  "range(0, 10)[-3]"
//  "range(0, 10)[0..3]"
//  "range(0, 10)[0..-5]"
//  "range(0, 10)[-5..]"
//  "range(0, 10)[..4]"
//  "range(0, 10)[15]"
//  "range(0, 10)[5..15]"
//  "size(range(0, 10)[0..3])"
//  "[x IN range(0,10) WHERE x % 2 = 0 | x^3]"
//  "[x IN range(0,10) WHERE x % 2 = 0]"
//  "[x IN range(0,10) | x^3]"
//  "[x in range(0,10) WHERE x > 3]"
//  "[x in range(0,10) | x ^ 2]"
//  "any(x IN [1,2,3,4,5] WHERE x > 2)"
//  "any(x IN [true,null,false,false] WHERE x)"
//  "any(x IN [null,null,false,false] WHERE x)"
//  "any(x IN [false,false,false,false] WHERE x)"
//  "all(x IN [1,2,3,4,5] WHERE x > 2)"
//  "all(x IN [true,null,true,false] WHERE x)"
//  "all(x IN [true,null,true,null] WHERE x)"
//  "all(x IN [true,true,true,true] WHERE x)"
//  "none(x IN [1,2,3,4,5] WHERE x > 2)"
//  "none(x IN [true,null,true,false] WHERE x)"
//  "none(x IN [false,null,false,null] WHERE x)"
//  "none(x IN [false,false,false,false] WHERE x)"
//  "single(x IN [1,2,3,4,5] WHERE x > 2)"
//  "single(x IN [1,2,3,4,5,null] WHERE x > 2)"
//  "single(x IN [1,2,3,4,5] WHERE x > 9)"
//  "single(x IN [true,null,null,false] WHERE x)"
//  "single(x IN [null,null,null,false] WHERE x)"
//  "single(x IN [1,2,3,4,5] WHERE x = 2)"
//  "reduce(acc = 1, x IN [1,3,6,9] | acc * x)"
//  "localdatetime({ year: 2019 })"
//  "localdatetime({ year: 1995, month: 4, day: 24 })"
//  "datetime({ epochSeconds: 1607532063, timezone: 'UTC' }).ordinalDay"
//  "date({ year: 1995, month: 4, day: 24 })"
//  "time({ hour: 10, minute: 4, second: 24, nanosecond: 110, offsetSeconds: -25200})"
//  "localtime({ hour: 10, minute: 4, second: 24, nanosecond: 110 })"
//  "duration({ days: 24 })"
//  "datetime('2020-12-09T13:15:41.914-05:00[America/Montreal]')"
//  "localdatetime('2020-12-09T13:15:41.914')"
//  "duration('PT20.345S')"
//  s"localdatetime({ year: 1995, month: 4, day: 24 }).$name"
//  "datetime({ year: 1995, month: 4, day: 24, timezone: 'Asia/Hong_Kong' }).epochSeconds"
//  "datetime({ epochSeconds: 798652800 }) = datetime({ epochSeconds: 798652800 })"
//  "localdatetime({ year: 2001, month: 11 }) < localdatetime({ year: 2000, month: 10, day: 2 })"

  test("(datetime({ year: 2001 }) + duration({ days: 13, hours: 1 })).day") {
    val actual = parseExpression(
      "(datetime({ year: 2001 }) + duration({ days: 13, hours: 1 })).day",
    )

    val expected = Expression.FieldAccess(
      source = Source.TextSource(61, 64),
      of = Expression.BinOp(
        source = Source.TextSource(1, 59),
        op = Operator.Plus,
        lhs = Expression.Apply(
          source = Source.TextSource(1, 24),
          name = Symbol("datetime"),
          args = List(
            Expression.MapLiteral(
              source = Source.TextSource(10, 23),
              value = Map(
                Symbol("year") -> Expression.AtomicLiteral(
                  Source.TextSource(18, 21),
                  Value.Integer(2001),
                  None,
                ),
              ),
              ty = None,
            ),
          ),
          ty = None,
        ),
        Expression.Apply(
          source = Source.TextSource(28, 59),
          name = Symbol("duration"),
          args = List(
            Expression.MapLiteral(
              source = Source.TextSource(37, 58),
              value = Map(
                Symbol("days") -> Expression.AtomicLiteral(
                  source = Source.TextSource(45, 46),
                  value = Value.Integer(13),
                  ty = None,
                ),
                Symbol("hours") -> Expression.AtomicLiteral(
                  source = Source.TextSource(56, 56),
                  value = Value.Integer(1),
                  ty = None,
                ),
              ),
              ty = None,
            ),
          ),
          ty = None,
        ),
        ty = None,
      ),
      fieldName = Symbol("day"),
      ty = None,
    )

    assertEquals(actual, expected)
  }

//  "(duration({ days: 13, hours: 1 }) + datetime({ year: 2001 })).hour"
//  "(datetime({ year: 2001 }) - duration({ days: 13, hours: 1 })).dayOfQuarter"
//  "duration({ minutes: 361 }) + duration({ days: 14 })"
//  "duration({ minutes: 361 }) - duration({ days: 14 })"
//  "temporal.format(datetime('Mon, 1 Apr 2019 11:05:30 GMT', 'E, d MMM yyyy HH:mm:ss z'), 'MMM dd uu')"
//  "temporal.format(localdatetime('Apr 1, 11 oclock in \\'19', 'MMM d, HH \\'oclock in \\'\\'\\'yy'), 'MMM dd uu')"
//  "1 IN null"
//  "null IN null"
//  "null IN []"
//  "null IN [1,2,3,4]"
//  "null IN [1,null,2]"
//  "2 IN [1,2,3,4]"
//  "6 IN [1,2,3,4]"
//  "2 IN [1,null,2,3,4]"
//  "6 IN [1,null,2,3,4]"
//  "[1,2] IN [[1,null,3]]"
//  "[1,2] IN [[1,null]]"
//  "[1,2] IN [[1,2]]"
//  "1 = 2.0"
//  "1 = 1.0"
//  "[1] = [1.0]"
//  "sqrt(-1) = sqrt(-1)"
//  "1.0/0.0 = 1.0/0.0"
//  "1.0/0.0 = -1.0/0.0"

  test("null + {}") {
    val actual = parseExpression("null + {}")

    val expected = Expression.BinOp(
      source = Source.TextSource(0, 8),
      op = Operator.Plus,
      lhs = Expression.AtomicLiteral(
        source = Source.TextSource(0, 3),
        value = Value.Null,
        ty = None,
      ),
      rhs = Expression.MapLiteral(
        source = Source.TextSource(7, 8),
        value = Map(),
        ty = None,
      ),
      ty = None,
    )

    assertEquals(actual, expected)
  }

//  "x IS NOT NULL"
//  "x IS NULL"
//  "x IS NOT NULL"
//  "x IS NULL"
//  "0.0/0.0 = 0.0/0.0"
//  "0.0/0.0 <> 0.0/0.0"
//  "0.0/0.0 = nan"
//  "0.0/0.0 <> nan"
//  "nan = nan"
//  "nan <> nan"
//  "NOT (n <> n)"
//  "n = n"
//  "1.0/0.0 > 0.0/0.0"
//  "1.0/0.0 < 0.0/0.0"
//  "-1.0/0.0 < 0.0/0.0"
//  "1.0/0.0 = 0.0/0.0"
//  "1.0/0.0 <> 0.0/0.0"
//  "n = n"
//  "n <> n"

  test("'hi' < 'hello'") {
    val actual = parseExpression("'hi' < 'hello'")

    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 13),
      op = Operator.LessThan,
      lhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 0, end = 3),
        value = Value.Text("hi"),
        ty = None,
      ),
      rhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 7, end = 13),
        value = Value.Text("hello"),
        ty = None,
      ),
      ty = None,
    )

    assertEquals(actual, expected)
  }

//  "'ha' < 'hello'"
//  "'he' < 'hello'"
//  "'hellooooo' < 'hello'"
//  """bytes("CEDEC0DE")"""
//  """bytes("cafec0de")"""
//  """bytes("feEdb33f")"""
//  """bytes("000000")"""
//  """bytes("02")"""
//  """bytes("c0ffee00")"""
//  """bytes("0000c0De")"""
//  """bytes("00FACE00")"""
//  "toJson(100.000)"
//  "toJson(100)"
//  "toJson([n, r, m])"
//  """toJson(bytes("c0de"))"""
//  """parseJson("42")"""
//  """parseJson("-42")"""
//  """parseJson("42.0")"""
//  """parseJson("42.5")"""
//  """parseJson("null")"""
//  """parseJson("{\"hello\": \"world\", \"x\": -128.4, \"b\": false, \"nest\": {\"birds\": [1, 4], \"type\": \"robin\"}}")"""
//  "map.fromPairs([])"
//  "map.fromPairs([['a', 1],['b',2]])"
//  "map.removeKey({ foo: 'bar', baz: 123 }, 'foo')"
//  "map.removeKey({ foo: 'bar', baz: 123 }, 'qux')"
//  "coll.max([])"
//  "coll.max([3.14])"
//  "coll.max([3.14, 3, 4])"
//  "coll.max(3.14, 3, 4)"
//  "coll.max([3.14, 2.9, 'not a number'])"
//  "coll.max([3.14, 10.1, 2, 2.9])"
//  "coll.max(3.14, 10.1, 2, 2.9)"
//  "coll.min([])"
//  "coll.min([3.14])"
//  "coll.min([3.14, 3, 4])"
//  "coll.min(3.14, 3, 4)"
//  "coll.min([3.14, 2.9, 'not a number'])"
//  "coll.min([3.14, 10.1, 2, 2.9])"
//  "coll.min(3.14, 10.1, 2, 2.9)"
//  "toInteger(123)"
//  "toInteger(123.0)"
//  "toInteger(123.3)"
//  "toInteger(123.7)"
//  "toInteger(-123.3)"
//  "toInteger(-123.7)"
//  "toInteger('123')"
//  "toInteger('123.0')"
//  "toInteger('123.3')"
//  "toInteger('123.7')"
//  "toInteger('-123.3')"
//  "toInteger('-123.7')"
//  "toInteger('0x11')"
//  "toInteger('0xf')"
//  "toInteger('0xc0FfEe')"
//  "toInteger('-0x12')"
//  "toInteger('-0xca11ab1e')"
//  "toInteger('-0x0')"
//  "toInteger('-0x12') = -0x12"
//  "toInteger('0xf00') = 0xf00"
//  "toInteger('9223372036854775806.2')"
//  "toInteger('bogus')"
//  "toInteger(' 123 ')"
//  "toFloat(123)"
//  "toFloat(123.0)"
//  "toFloat(123.3)"
//  "toFloat(123.7)"
//  "toFloat(-123.3)"
//  "toFloat(-123.7)"
//  "toFloat('123')"
//  "toFloat('123.0')"
//  "toFloat('123.3')"
//  "toFloat('123.7')"
//  "toFloat('-123.3')"
//  "toFloat('-123.7')"
//  "toFloat('9223372036854775806.2')"
//  "toFloat('bogus')"
//  "toFloat(' 123 ')"
//  "text.utf8Decode(bytes('6162206364'))"
//  "text.utf8Decode(bytes('5765204469646E2774205374617274207468652046697265'))"
//  "text.utf8Decode(bytes('F09F8C88'))"
//  "text.utf8Decode(bytes('E4BDA0E5A5BDE4B896E7958C'))"
//  """text.utf8Encode("ab cd")"""
//  """text.utf8Encode("We Didn't Start the Fire")"""

  test("""text.utf8Encode("你好世界")""") {
    val expected = Expression.Apply(
      source = Source
        .TextSource(start = 0, end = 22),
      name = Symbol("text.utf8Encode"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 16, end = 21),
          value = Value.Text("你好世界"),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("""text.utf8Encode("你好世界")""")

    assertEquals(actual, expected)
  }

  test("getHost(idFrom(-1))") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 18),
      name = Symbol("getHost"),
      args = List(
        Expression.SynthesizeId(
          source = Source.TextSource(start = 8, end = 17),
          from = List(
            Expression.UnaryOp(
              source = Source.TextSource(start = 15, end = 16),
              op = Operator.Minus,
              exp = Expression.AtomicLiteral(
                source = Source.TextSource(start = 16, end = 16),
                value = Value.Integer(1),
                ty = None,
              ),
              ty = None,
            ),
          ),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseExpression("getHost(idFrom(-1))")

    assertEquals(actual, expected)
  }

  test("[1,2] + [3]") {
    val actual = parseExpression("[1,2] + [3]")

    val expected = Expression.BinOp(
      source = Source.TextSource(0, 10),
      op = Operator.Plus,
      lhs = Expression.ListLiteral(
        source = Source.TextSource(0, 4),
        value = List(
          Expression
            .AtomicLiteral(Source.TextSource(1, 1), Value.Integer(1), None),
          Expression
            .AtomicLiteral(Source.TextSource(3, 3), Value.Integer(2), None),
        ),
        ty = None,
      ),
      rhs = Expression.ListLiteral(
        source = Source.TextSource(8, 10),
        value = List(
          Expression
            .AtomicLiteral(Source.TextSource(9, 9), Value.Integer(3), None),
        ),
        ty = None,
      ),
      ty = None,
    )

    assertEquals(actual, expected)
  }
}
