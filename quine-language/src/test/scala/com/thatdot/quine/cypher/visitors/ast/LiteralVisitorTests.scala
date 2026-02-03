package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.LiteralVisitor
import com.thatdot.quine.language.ast.{Expression, Source, Value}

class LiteralVisitorTests extends munit.FunSuite {
  def parseLiteral(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_Literal()

    LiteralVisitor.visitOC_Literal(tree)
  }

  test("123") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Integer(123),
      ty = None,
    )

    val actual = parseLiteral("123")

    assertEquals(actual, expected)
  }

  test("1.23") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Real(1.23),
      ty = None,
    )

    val actual = parseLiteral("1.23")

    assertEquals(actual, expected)
  }

  test("\"hi\"") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Text("hi"),
      ty = None,
    )

    val actual = parseLiteral("\"hi\"")

    assertEquals(actual, expected)
  }

  test("[]") {
    val expected = Expression.ListLiteral(
      source = Source.TextSource(start = 0, end = 1),
      value = Nil,
      ty = None,
    )

    val actual = parseLiteral("[]]")

    assertEquals(actual, expected)
  }

  test("[1,2,3]") {
    val expected = Expression.ListLiteral(
      source = Source.TextSource(start = 0, end = 6),
      value = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 1, end = 1),
          value = Value.Integer(1),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 3, end = 3),
          value = Value.Integer(2),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 5, end = 5),
          value = Value.Integer(3),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseLiteral("[1,2,3]")

    assertEquals(actual, expected)
  }

  test("null") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Null,
      ty = None,
    )

    val actual = parseLiteral("null")

    assertEquals(actual, expected)
  }

  test("nULl") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Null,
      ty = None,
    )

    val actual = parseLiteral("nULl")

    assertEquals(actual, expected)
  }

  test("true") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.True,
      ty = None,
    )

    val actual = parseLiteral("true")

    assertEquals(actual, expected)
  }

  test("false") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 4),
      value = Value.False,
      ty = None,
    )

    val actual = parseLiteral("false")

    assertEquals(actual, expected)
  }
}
