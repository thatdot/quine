package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.IntegerVisitor
import com.thatdot.quine.language.ast.{Expression, Source, Value}

class IntegerVisitorTests extends munit.FunSuite {
  def parseInt(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_IntegerLiteral()

    IntegerVisitor.visitOC_IntegerLiteral(tree)
  }

  test("9876543210") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 9),
      value = Value.Integer(9876543210L),
      ty = None,
    )

    val actual = parseInt("9876543210")

    assertEquals(actual, expected)
  }

  test("0") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 0),
      value = Value.Integer(0L),
      ty = None,
    )

    val actual = parseInt("0")

    assertEquals(actual, expected)
  }

  test("hex integer 0xFF") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Integer(255L),
      ty = None,
    )

    val actual = parseInt("0xFF")

    assertEquals(actual, expected)
  }

  test("hex integer 0x1A") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Integer(26L),
      ty = None,
    )

    val actual = parseInt("0x1A")

    assertEquals(actual, expected)
  }

  test("hex integer lowercase 0xabcdef") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 7),
      value = Value.Integer(11259375L),
      ty = None,
    )

    val actual = parseInt("0xabcdef")

    assertEquals(actual, expected)
  }

  test("octal integer 0o17") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 3),
      value = Value.Integer(15L), // 1*8 + 7 = 15
      ty = None,
    )

    val actual = parseInt("0o17")

    assertEquals(actual, expected)
  }

  test("octal integer 0o777") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 4),
      value = Value.Integer(511L), // 7*64 + 7*8 + 7 = 511
      ty = None,
    )

    val actual = parseInt("0o777")

    assertEquals(actual, expected)
  }

  test("octal integer 0o0") {
    val expected = Expression.AtomicLiteral(
      source = Source.TextSource(start = 0, end = 2),
      value = Value.Integer(0L),
      ty = None,
    )

    val actual = parseInt("0o0")

    assertEquals(actual, expected)
  }
}
