package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.DoubleVisitor
import com.thatdot.quine.language.ast.{Expression, Source, Value}

class DoubleVisitorTests extends munit.FunSuite {
  def parseDouble(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_DoubleLiteral()

    DoubleVisitor.visitOC_DoubleLiteral(tree)
  }

  test("1.0") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Real(1.0),
      ty = None,
    )

    val actual = parseDouble("1.0")

    assertEquals(actual, expected)
  }
}
