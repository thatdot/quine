package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.XorVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class XorVisitorTests extends munit.FunSuite {
  def parseXor(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_XorExpression()

    XorVisitor.visitOC_XorExpression(tree)
  }

  test("$bleh") {
    val expected = Expression.Parameter(
      source = Source.TextSource(start = 0, end = 4),
      name = Symbol("$bleh"),
      ty = None,
    )

    val actual = parseXor("$bleh")

    assertEquals(actual, expected)
  }

  test("a XOR b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 6),
      op = Operator.Xor,
      lhs = Expression.BinOp(
        source = Source.TextSource(start = 0, end = 6),
        op = Operator.Xor,
        lhs = Expression.AtomicLiteral(
          source = Source.NoSource,
          value = Value.False,
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 6, end = 6),
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

    val actual = parseXor("a XOR b")

    assertEquals(actual, expected)
  }

  test("a XOR b XOR c") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 0, end = 12),
      op = Operator.Xor,
      lhs = Expression.BinOp(
        source = Source.TextSource(start = 0, end = 12),
        op = Operator.Xor,
        lhs = Expression.BinOp(
          source = Source.TextSource(start = 0, end = 12),
          op = Operator.Xor,
          lhs = Expression.AtomicLiteral(
            source = Source.NoSource,
            value = Value.False,
            ty = None,
          ),
          rhs = Expression.Ident(
            source = Source.TextSource(start = 12, end = 12),
            identifier = Left(CypherIdentifier(Symbol("c"))),
            ty = None,
          ),
          ty = None,
        ),
        rhs = Expression.Ident(
          source = Source.TextSource(start = 6, end = 6),
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

    val actual = parseXor("a XOR b XOR c")

    assertEquals(actual, expected)
  }
}
