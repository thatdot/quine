package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.OrExpressionVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class OrExpressionVisitorTests extends munit.FunSuite {
  def parseOr(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_OrExpression()

    OrExpressionVisitor.visitOC_OrExpression(tree)
  }

  test("123") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Integer(123),
      ty = None,
    )

    val actual = parseOr("123")

    assertEquals(actual, expected)
  }

  test("a OR b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 5),
      op = Operator.Or,
      lhs = Expression.BinOp(
        source = Source.TextSource(start = 0, end = 5),
        op = Operator.Or,
        lhs = Expression.AtomicLiteral(
          source = Source.NoSource,
          value = Value.False,
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 5, end = 5),
          identifier = Left(CypherIdentifier(Symbol("b"))),
          ty = None,
        ),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseOr("a OR b")

    assertEquals(actual, expected)
  }

  test("a OR b OR c") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 10),
      op = Operator.Or,
      lhs = Expression.BinOp(
        source = Source.TextSource(start = 0, end = 10),
        op = Operator.Or,
        lhs = Expression.BinOp(
          source = Source.TextSource(start = 0, end = 10),
          op = Operator.Or,
          lhs = Expression.AtomicLiteral(
            source = Source.NoSource,
            value = Value.False,
            ty = None,
          ),
          rhs = Expression.Ident(
            source = Source.TextSource(start = 10, end = 10),
            identifier = Left(CypherIdentifier(Symbol("c"))),
            ty = None,
          ),
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 5, end = 5),
          identifier = Left(CypherIdentifier(Symbol("b"))),
          ty = None,
        ),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseOr("a OR b OR c")

    assertEquals(actual, expected)
  }
}
