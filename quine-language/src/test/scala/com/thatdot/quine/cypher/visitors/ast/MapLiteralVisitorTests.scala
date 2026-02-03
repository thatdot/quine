package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.MapLiteralVisitor
import com.thatdot.quine.language.ast.{Expression, Operator, Source, Value}

class MapLiteralVisitorTests extends munit.FunSuite {
  def parseMapLiteral(source: String): Expression.MapLiteral = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_MapLiteral()

    MapLiteralVisitor.visitOC_MapLiteral(tree)
  }

  test("empty map literal") {
    val expected = Expression.MapLiteral(
      source = Source.TextSource(start = 0, end = 1),
      value = Map(),
      ty = None,
    )

    val actual = parseMapLiteral("{}")

    assertEquals(actual, expected)
  }

  test("simple props") {
    val expected: Expression.MapLiteral = Expression.MapLiteral(
      source = Source.TextSource(start = 0, end = 48),
      value = Map(
        Symbol("e") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 40, end = 47),
          value = Value.Text("single"),
          ty = None,
        ),
        Symbol("a") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 4, end = 10),
          value = Value.Text("hello"),
          ty = None,
        ),
        Symbol("b") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 16, end = 16),
          value = Value.Integer(3),
          ty = None,
        ),
        Symbol("c") -> Expression.UnaryOp(
          source = Source.TextSource(start = 22, end = 25),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 23, end = 25),
            value = Value.Real(4.1),
            ty = None,
          ),
          ty = None,
        ),
        Symbol("d") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 31, end = 34),
          value = Value.True,
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseMapLiteral("""{a: "hello", b: 3, c: -4.1, d: true, e: 'single'}""")

    assertEquals(actual, expected)
  }

  test("duplicate keys") {
    val expected = Expression.MapLiteral(
      source = Source.TextSource(start = 0, end = 26),
      value = Map(
        Symbol("a") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 20, end = 25),
          value = Value.Text("test"),
          ty = None,
        ),
        Symbol("b") -> Expression.AtomicLiteral(
          source = Source.TextSource(start = 10, end = 14),
          value = Value.False,
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseMapLiteral("""{a: 3, b: false, a: "test"}""")

    assertEquals(actual, expected)
  }
}
