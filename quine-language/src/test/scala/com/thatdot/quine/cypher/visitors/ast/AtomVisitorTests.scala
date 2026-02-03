package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.AtomVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class AtomVisitorTests extends munit.FunSuite {
  def parseAtom(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_Atom()

    AtomVisitor.visitOC_Atom(tree)
  }

  test("null") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Null,
      ty = None,
    )

    val actual = parseAtom("null")

    assertEquals(actual, expected)
  }

  test("123") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Integer(123),
      ty = None,
    )

    val actual = parseAtom("123")

    assertEquals(actual, expected)
  }

  test("(99)") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 1, end = 2),
      value = Value.Integer(99),
      ty = None,
    )

    val actual = parseAtom("(99)")

    assertEquals(actual, expected)
  }

  test("someFunction(33,-17,\"hello\")") {
    val expected = Expression.Apply(
      source = Source.TextSource(start = 0, end = 27),
      name = Symbol("someFunction"),
      args = List(
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 13, end = 14),
          value = Value.Integer(33),
          ty = None,
        ),
        Expression.UnaryOp(
          source = Source.TextSource(start = 16, end = 18),
          op = Operator.Minus,
          exp = Expression.AtomicLiteral(
            source = Source.TextSource(start = 17, end = 18),
            value = Value.Integer(17),
            ty = None,
          ),
          ty = None,
        ),
        Expression.AtomicLiteral(
          source = Source.TextSource(start = 20, end = 26),
          value = Value.Text("hello"),
          ty = None,
        ),
      ),
      ty = None,
    )

    val actual = parseAtom("someFunction(33,-17,\"hello\")")

    assertEquals(actual, expected)
  }

  test("whatAmI") {
    val expected = Expression.Ident(
      source = Source.TextSource(start = 0, end = 6),
      identifier = Left(CypherIdentifier(Symbol("whatAmI"))),
      ty = None,
    )

    val actual = parseAtom("whatAmI")

    assertEquals(actual, expected)
  }

  test("$whatAmI") {
    val expected = Expression.Parameter(
      source = Source.TextSource(start = 0, end = 7),
      name = Symbol("$whatAmI"),
      ty = None,
    )

    val actual = parseAtom("$whatAmI")

    assertEquals(actual, expected)
  }
}
