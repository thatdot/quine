package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.PartialComparisonVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source}

class PartialComparisonVisitorTests extends munit.FunSuite {
  def parsePartialComparison(source: String): (Operator, Expression) = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_PartialComparisonExpression()

    PartialComparisonVisitor.visitOC_PartialComparisonExpression(tree)
  }

  test("= a") {
    val expected =
      Operator.Equals ->
      Expression.Ident(
        source = Source.TextSource(start = 2, end = 2),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison("= a")

    assertEquals(actual, expected)
  }

  test("<> a") {
    val expected =
      Operator.NotEquals ->
      Expression.Ident(
        source = Source.TextSource(start = 3, end = 3),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison("<> a")

    assertEquals(actual, expected)
  }

  test("< a") {
    val expected =
      Operator.LessThan ->
      Expression.Ident(
        source = Source.TextSource(start = 2, end = 2),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison("< a")

    assertEquals(actual, expected)
  }

  test("<= a") {
    val expected =
      Operator.LessThanEqual ->
      Expression.Ident(
        source = Source.TextSource(start = 3, end = 3),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison("<= a")

    assertEquals(actual, expected)
  }

  test("> a") {
    val expected =
      Operator.GreaterThan ->
      Expression.Ident(
        source = Source.TextSource(start = 2, end = 2),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison("> a")

    assertEquals(actual, expected)
  }

  test(">= a") {
    val expected =
      Operator.GreaterThanEqual ->
      Expression.Ident(
        source = Source.TextSource(start = 3, end = 3),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      )

    val actual = parsePartialComparison(">= a")

    assertEquals(actual, expected)
  }
}
