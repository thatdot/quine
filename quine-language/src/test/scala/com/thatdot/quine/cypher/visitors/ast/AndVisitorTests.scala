package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.AndVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source}

class AndVisitorTests extends munit.FunSuite {
  def parseAnd(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_AndExpression()

    AndVisitor.visitOC_AndExpression(tree)
  }

  test("a AND b") {
    val expected = Expression.BinOp(
      source = Source.TextSource(start = 6, end = 6),
      op = Operator.And,
      lhs = Expression.Ident(
        source = Source.TextSource(start = 0, end = 0),
        identifier = Left(CypherIdentifier(Symbol("a"))),
        ty = None,
      ),
      rhs = Expression.Ident(
        source = Source.TextSource(start = 6, end = 6),
        identifier = Left(CypherIdentifier(Symbol("b"))),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseAnd("a AND b")

    assertEquals(actual, expected)
  }
}
