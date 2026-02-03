package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.MultiplyDivideModuloVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class MultiplyDivideModuloVisitorTests extends munit.FunSuite {
  def parseMultiplyDivideModulo(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_MultiplyDivideModuloExpression()

    MultiplyDivideModuloVisitor.visitOC_MultiplyDivideModuloExpression(tree)
  }

  test("\"bob\"") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 4),
      value = Value.Text("bob"),
      ty = None,
    )

    val actual = parseMultiplyDivideModulo("\"bob\"")

    assertEquals(actual, expected)
  }

  test("1 * 2") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Asterisk,
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

    val actual = parseMultiplyDivideModulo("1 * 2")

    assertEquals(actual, expected)
  }

  test("1 % 2") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Percent,
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

    val actual = parseMultiplyDivideModulo("1 % 2")

    assertEquals(actual, expected)
  }

  test("3 / 1.5") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 6),
      op = Operator.Slash,
      lhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 0, end = 0),
        value = Value.Integer(3),
        ty = None,
      ),
      rhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 4, end = 6),
        value = Value.Real(1.5),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseMultiplyDivideModulo("3 / 1.5")

    assertEquals(actual, expected)
  }

  test("a * (b / c)") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 10),
      op = Operator.Asterisk,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.BinOp(
        source = Source.TextSource(start = 5, end = 9),
        op = Operator.Slash,
        lhs = Expression.Ident(
          source = Source.TextSource(start = 5, end = 5),
          identifier = Left(CypherIdentifier(Symbol("b"))),
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 9, end = 9),
          identifier = Left(CypherIdentifier(Symbol("c"))),
          ty = None,
        ),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseMultiplyDivideModulo("a * (b / c)")

    assertEquals(actual, expected)
  }
}
