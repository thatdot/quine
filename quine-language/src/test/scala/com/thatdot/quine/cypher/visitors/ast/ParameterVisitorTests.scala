package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ParameterVisitor
import com.thatdot.quine.language.ast.{Expression, Source}

class ParameterVisitorTests extends munit.FunSuite {
  def parseParameter(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_Parameter()

    ParameterVisitor.visitOC_Parameter(tree)
  }

  test("$parameter") {
    val expected = Expression.Parameter(
      source = Source.TextSource(start = 0, end = 9),
      name = Symbol("$parameter"),
      ty = None,
    )

    val actual = parseParameter("$parameter")

    assertEquals(actual, expected)
  }
}
