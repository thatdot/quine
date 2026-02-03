package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.UnaryAddSubtractVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, Source, Value}

class UnaryAddSubtractVisitorTests extends munit.FunSuite {
  def parseUnaryAddSubtract(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_UnaryAddOrSubtractExpression()

    UnaryAddSubtractVisitor.visitOC_UnaryAddOrSubtractExpression(tree)
  }

  test("+null") {
    val expected = Expression.UnaryOp(
      source = Source.TextSource(start = 0, end = 4),
      op = Operator.Plus,
      exp = Expression.AtomicLiteral(
        source = Source.TextSource(start = 1, end = 4),
        value = Value.Null,
        ty = None,
      ),
      ty = None,
    )

    val actual = parseUnaryAddSubtract("+null")

    assertEquals(actual, expected)
  }

  test("-123") {
    val expected = Expression.UnaryOp(
      source = Source.TextSource(start = 0, end = 3),
      op = Operator.Minus,
      exp = Expression.AtomicLiteral(
        source = Source.TextSource(start = 1, end = 3),
        value = Value.Integer(123),
        ty = None,
      ),
      ty = None,
    )

    val actual = parseUnaryAddSubtract("-123")

    assertEquals(actual, expected)
  }

  test("--a is not valid Cypher syntax") {
    // The Cypher grammar does not support double negation as `--a`.
    // The parser treats the second `-` as extraneous input.
    // To negate a negation in Cypher, use parentheses: -(-a)
    val actual = parseUnaryAddSubtract("--a")

    // The parser produces a single UnaryOp with Minus, treating only `-a` as the expression
    // (the first `-` is reported as extraneous but parsing continues with error recovery)
    actual match {
      case Expression.UnaryOp(_, Operator.Minus, Expression.Ident(_, Left(CypherIdentifier(name)), _), _) =>
        assertEquals(name, Symbol("a"), "Should parse as -a (with first - as extraneous)")
      case other =>
        fail(s"Expected UnaryOp(Minus, Ident(a)), got $other")
    }
  }

  test("-(-a) nested negation with parentheses") {
    // Proper way to express double negation in Cypher
    val input = CharStreams.fromString("-(-a)")
    val lexer = new CypherLexer(input)
    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)
    val tree = parser.oC_UnaryAddOrSubtractExpression()
    val actual = UnaryAddSubtractVisitor.visitOC_UnaryAddOrSubtractExpression(tree)

    actual match {
      case Expression.UnaryOp(_, Operator.Minus, inner: Expression.UnaryOp, _) =>
        assertEquals(inner.op, Operator.Minus, "Inner operator should be Minus")
        inner.exp match {
          case Expression.Ident(_, Left(CypherIdentifier(name)), _) =>
            assertEquals(name, Symbol("a"), "Innermost expression should be identifier 'a'")
          case other =>
            fail(s"Expected Ident for innermost expression, got $other")
        }
      case other =>
        fail(s"Expected nested UnaryOp(Minus, UnaryOp(Minus, Ident)), got $other")
    }
  }
}
