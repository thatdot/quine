package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.NonArithmeticOperatorVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Source}

class NonArithmeticOperatorVisitorTests extends munit.FunSuite {
  def parseNonArithmeticOperator(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_NonArithmeticOperatorExpression()

    NonArithmeticOperatorVisitor.visitOC_NonArithmeticOperatorExpression(tree)
  }

  test("a.b") {
    val actual = parseNonArithmeticOperator("a.b")

    val expected = Expression.FieldAccess(
      source = Source.TextSource(start = 1, end = 2),
      of = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      fieldName = Symbol("b"),
      ty = None,
    )

    assertEquals(actual, expected)
  }
}
