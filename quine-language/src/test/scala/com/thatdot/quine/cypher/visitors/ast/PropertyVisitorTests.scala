package com.thatdot.quine.cypher.visitors.ast

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.PropertyVisitor
import com.thatdot.quine.language.ast.Expression.{FieldAccess, Ident}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression}

class PropertyVisitorTests extends munit.FunSuite {
  def parseProperty(source: String): Expression = {
    val input = CharStreams.fromString(source)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_PropertyExpression()

    PropertyVisitor.visitOC_PropertyExpression(tree)
  }

  test("simple property") {
    val actual = parseProperty("foo.bar")

    // The AST visitor produces CypherIdentifiers (unresolved) - symbol analysis resolves them to QuineIdentifiers
    actual match {
      case FieldAccess(_, of: Ident, fieldName, _) =>
        assertEquals(fieldName, Symbol("bar"))
        of.identifier match {
          case Left(CypherIdentifier(name)) => assertEquals(name, Symbol("foo"))
          case Right(_) => fail("Expected CypherIdentifier (Left), got QuineIdentifier (Right)")
        }
      case _ => fail(s"Expected FieldAccess(Ident), got $actual")
    }
  }

  test("property of property") {
    val actual = parseProperty("foo.bar.baz")

    // The AST visitor produces CypherIdentifiers (unresolved) - symbol analysis resolves them to QuineIdentifiers
    actual match {
      case FieldAccess(_, inner: FieldAccess, outerField, _) =>
        assertEquals(outerField, Symbol("baz"))
        assertEquals(inner.fieldName, Symbol("bar"))
        inner.of match {
          case Ident(_, Left(CypherIdentifier(name)), _) => assertEquals(name, Symbol("foo"))
          case Ident(_, Right(_), _) => fail("Expected CypherIdentifier (Left), got QuineIdentifier (Right)")
          case other => fail(s"Expected Ident, got $other")
        }
      case _ => fail(s"Expected FieldAccess(FieldAccess(Ident)), got $actual")
    }
  }
}
