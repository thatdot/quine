package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ComparisonVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source}

class ComparisonVisitorTests extends munit.FunSuite {
  def parseComparison(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_ComparisonExpression()

    ComparisonVisitor.visitOC_ComparisonExpression(tree)
  }

  test("a = b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Equals,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 4, end = 4),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a = b")

    assertEquals(actual, expected)
  }

  test("a <> b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 5),
      op = Operator.NotEquals,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 5, end = 5),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a <> b")

    assertEquals(actual, expected)
  }

  test("a < b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.LessThan,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 4, end = 4),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a < b")

    assertEquals(actual, expected)
  }

  test("a > b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.GreaterThan,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 4, end = 4),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a > b")

    assertEquals(actual, expected)
  }

  test("a <= b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 5),
      op = Operator.LessThanEqual,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 5, end = 5),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a <= b")

    assertEquals(actual, expected)
  }

  test("a >= b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 5),
      op = Operator.GreaterThanEqual,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 5, end = 5),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseComparison("a >= b")

    assertEquals(actual, expected)
  }
}
