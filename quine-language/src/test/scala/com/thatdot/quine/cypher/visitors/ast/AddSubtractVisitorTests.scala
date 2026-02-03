package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.AddSubtractVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class AddSubtractVisitorTests extends munit.FunSuite {
  def parseAddSubtract(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_AddOrSubtractExpression()

    AddSubtractVisitor.visitOC_AddOrSubtractExpression(tree)
  }

  test("\"bob\"") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 4),
      value = Value.Text("bob"),
      ty = None,
    )

    val actual = parseAddSubtract("\"bob\"")

    assertEquals(actual, expected)
  }

  test("1 + 2") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Plus,
      lhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 0, end = 0),
        value = Value.Integer(1),
        ty = None,
      ),
      rhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 4, end = 4),
        value = Value.Integer(2),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseAddSubtract("1 + 2")

    assertEquals(actual, expected)
  }

  test("n + m") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Plus,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("n"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 4, end = 4),
        identifier = Left(CypherIdentifier(Symbol("m"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseAddSubtract("n + m")

    assertEquals(actual, expected)
  }

  test("3 - 2 + 1") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 8),
      op = Operator.Minus,
      lhs = Expression.BinOp(
        source = Source.TextSource(start = 0, end = 8),
        op = Operator.Plus,
        lhs = Expression.AtomicLiteral(
          source = Source.TextSource(start = 0, end = 0),
          value = Value.Integer(3),
          ty = None,
        ),
        rhs = Expression.AtomicLiteral(
          source = Source.TextSource(start = 4, end = 4),
          value = Value.Integer(2),
          ty = None,
        ),
        ty = None,
      ),
      rhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 8, end = 8),
        value = Value.Integer(1),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseAddSubtract("3 - 2 + 1")

    assertEquals(actual, expected)
  }

  test("n - (m + n)") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 10),
      op = Operator.Minus,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("n"))),
        ty = None,
      ),
      rhs = Expression.BinOp(
        source = Source.TextSource(start = 5, end = 9),
        op = Operator.Plus,
        lhs = Expression.Ident(
          source = Source.TextSource(start = 5, end = 5),
          identifier = Left(CypherIdentifier(Symbol("m"))),
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 9, end = 9),
          identifier = Left(CypherIdentifier(Symbol("n"))),
          ty = None,
        ),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseAddSubtract("n - (m + n)")

    assertEquals(actual, expected)
  }
}
