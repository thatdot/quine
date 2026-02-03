package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.NumberVisitor
import com.thatdot.quine.language.ast.{Expression, Source, Value}

class NumberVisitorTests extends munit.FunSuite {
  def parseNumber(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_NumberLiteral()

    NumberVisitor.visitOC_NumberLiteral(tree)
  }

  test("123") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Integer(123),
      ty = None,
    )

    val actual = parseNumber("123")

    assertEquals(actual, expected)
  }

  test("1.23") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Real(1.23),
      ty = None,
    )

    val actual = parseNumber("1.23")

    assertEquals(actual, expected)
  }
}
