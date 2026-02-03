package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.PowerOfVisitor
import com.thatdot.quine.language.ast.{Expression, Operator, Source, Value}

class PowerOfVisitorTests extends munit.FunSuite {
  def parsePowerOf(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_PowerOfExpression()

    PowerOfVisitor.visitOC_PowerOfExpression(tree)
  }

  test("$bleh") {
    val expected = Expression.Parameter(
      source = Source.TextSource(start = 0, end = 4),
      name = Symbol("$bleh"),
      ty = None,
    )

    val actual = parsePowerOf("$bleh")

    assertEquals(actual, expected)
  }

  test("2^3") {
    val expected = Expression.BinOp(
      source = Source.NoSource,
      op = Operator.Carat,
      lhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 0, end = 0),
        value = Value.Integer(2),
        ty = None,
      ),
      rhs = Expression.AtomicLiteral(
        source = Source.TextSource(start = 2, end = 2),
        value = Value.Integer(3),
        ty = None,
      ),
      ty = None,
    )

    val actual = parsePowerOf("2^3")

    assertEquals(actual, expected)
  }
}
