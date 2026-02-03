package com.thatdot.quine.language.semantics

import scala.jdk.CollectionConverters._

import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}
import com.thatdot.quine.language.server.ContextAwareLanguageService

class SemanticAnalysisTests extends munit.FunSuite {
  def analyzeQuery(queryText: String): List[SemanticToken] = {
    val cals = new ContextAwareLanguageService
    cals.semanticAnalysis(queryText).asScala.toList
  }

  test("simple query") {
    val actual = analyzeQuery("MATCH (bob) RETURN bob")
    val expected = List(
      SemanticToken(
        line = 1,
        charOnLine = 0,
        length = 5,
        semanticType = SemanticType.MatchKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 7,
        length = 3,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 12,
        length = 6,
        semanticType = SemanticType.ReturnKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 19,
        length = 3,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
    )

    assertEquals(actual, expected)
  }

  test("simple query with edges") {
    val actual = analyzeQuery("MATCH (s:Source)-[:edge]->(d:Dest) RETURN s.x + d.x")
    val expected = List(
      SemanticToken(
        line = 1,
        charOnLine = 0,
        length = 5,
        semanticType = SemanticType.MatchKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 7,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 9,
        length = 6,
        semanticType = SemanticType.NodeLabel,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 25,
        length = 1,
        semanticType = SemanticType.Edge,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 27,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 29,
        length = 4,
        semanticType = SemanticType.NodeLabel,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 35,
        length = 6,
        semanticType = SemanticType.ReturnKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 42,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 46,
        length = 1,
        semanticType = SemanticType.AdditionOperator,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 48,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
    )

    assertEquals(actual, expected)
  }

  test("multi-line query") {
    val tq1 =
      """MATCH (l) WHERE id(l) = $that.data.id
        |MATCH (v) WHERE id(v) = idFrom('verb', l.verb)
        |SET v.type = 'verb',
        |    v.verb = l.verb
        |CREATE (l)-[:verb]->(v)
        |""".stripMargin

    val actual = analyzeQuery(tq1)
    val expected = List(
      SemanticToken(
        line = 1,
        charOnLine = 0,
        length = 5,
        semanticType = SemanticType.MatchKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 7,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 10,
        length = 5,
        semanticType = SemanticType.WhereKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 16,
        length = 2,
        semanticType = SemanticType.FunctionApplication,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 19,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 25,
        length = 4,
        semanticType = SemanticType.Parameter,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 0,
        length = 5,
        semanticType = SemanticType.MatchKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 7,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 10,
        length = 5,
        semanticType = SemanticType.WhereKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 16,
        length = 2,
        semanticType = SemanticType.FunctionApplication,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 19,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 24,
        length = 6,
        semanticType = SemanticType.FunctionApplication,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 31,
        length = 6,
        semanticType = SemanticType.StringLiteral,
        modifiers = 0,
      ),
      SemanticToken(
        line = 2,
        charOnLine = 39,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 3,
        charOnLine = 4,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 3,
        charOnLine = 6,
        length = 4,
        semanticType = SemanticType.Property,
        modifiers = 0,
      ),
      SemanticToken(
        line = 3,
        charOnLine = 13,
        length = 6,
        semanticType = SemanticType.StringLiteral,
        modifiers = 0,
      ),
      SemanticToken(
        line = 4,
        charOnLine = 4,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 4,
        charOnLine = 6,
        length = 4,
        semanticType = SemanticType.Property,
        modifiers = 0,
      ),
      SemanticToken(
        line = 4,
        charOnLine = 13,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 5,
        charOnLine = 0,
        length = 6,
        semanticType = SemanticType.CreateKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 5,
        charOnLine = 8,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 5,
        charOnLine = 19,
        length = 1,
        semanticType = SemanticType.Edge,
        modifiers = 0,
      ),
      SemanticToken(
        line = 5,
        charOnLine = 21,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
    )

    assertEquals(actual, expected)
  }

  test("DISTINCT keyword") {
    val tq2 =
      """MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
        |      (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
        |WHERE e1.type = "WRITE"
        |      AND e2.type = "READ"
        |      AND e3.type = "DELETE"
        |      AND e4.type = "SEND"
        |RETURN DISTINCT id(f) as fileId
        |""".stripMargin

    val actual = analyzeQuery(tq2)
    val expected = List(
      SemanticToken(1, 0, 5, SemanticType.MatchKeyword, 0),
      SemanticToken(1, 7, 2, SemanticType.Variable, 0),
      SemanticToken(1, 20, 1, SemanticType.Edge, 0),
      SemanticToken(1, 22, 1, SemanticType.Variable, 0),
      SemanticToken(1, 24, 1, SemanticType.Edge, 0),
      SemanticToken(1, 36, 2, SemanticType.Variable, 0),
      SemanticToken(2, 7, 1, SemanticType.Variable, 0),
      SemanticToken(2, 9, 1, SemanticType.Edge, 0),
      SemanticToken(2, 21, 2, SemanticType.Variable, 0),
      SemanticToken(2, 24, 1, SemanticType.Edge, 0),
      SemanticToken(2, 36, 2, SemanticType.Variable, 0),
      SemanticToken(2, 49, 1, SemanticType.Edge, 0),
      SemanticToken(2, 51, 2, SemanticType.Variable, 0),
      SemanticToken(3, 0, 5, SemanticType.WhereKeyword, 0),
      SemanticToken(3, 6, 2, SemanticType.Variable, 0),
      SemanticToken(3, 16, 7, SemanticType.StringLiteral, 0),
      SemanticToken(4, 6, 3, SemanticType.AndKeyword, 0),
      SemanticToken(4, 10, 2, SemanticType.Variable, 0),
      SemanticToken(4, 20, 6, SemanticType.StringLiteral, 0),
      SemanticToken(5, 6, 3, SemanticType.AndKeyword, 0),
      SemanticToken(5, 10, 2, SemanticType.Variable, 0),
      SemanticToken(5, 20, 8, SemanticType.StringLiteral, 0),
      SemanticToken(6, 6, 3, SemanticType.AndKeyword, 0),
      SemanticToken(6, 10, 2, SemanticType.Variable, 0),
      SemanticToken(6, 20, 6, SemanticType.StringLiteral, 0),
      SemanticToken(7, 0, 6, SemanticType.ReturnKeyword, 0),
      SemanticToken(7, 16, 2, SemanticType.FunctionApplication, 0),
      SemanticToken(7, 19, 1, SemanticType.Variable, 0),
      SemanticToken(7, 22, 2, SemanticType.AsKeyword, 0),
      SemanticToken(7, 25, 6, SemanticType.Variable, 0),
    )

    assertEquals(actual, expected)
  }

  test("MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = 1") {
    val expected: List[SemanticToken] = List(
      SemanticToken(
        line = 1,
        charOnLine = 0,
        length = 5,
        semanticType = SemanticType.MatchKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 7,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 10,
        length = 5,
        semanticType = SemanticType.WhereKeyword,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 16,
        length = 2,
        semanticType = SemanticType.FunctionApplication,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 19,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 24,
        length = 6,
        semanticType = SemanticType.FunctionApplication,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 31,
        length = 5,
        semanticType = SemanticType.StringLiteral,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 42,
        length = 1,
        semanticType = SemanticType.Variable,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 44,
        length = 4,
        semanticType = SemanticType.Property,
        modifiers = 0,
      ),
      SemanticToken(
        line = 1,
        charOnLine = 51,
        length = 1,
        semanticType = SemanticType.IntLiteral,
        modifiers = 0,
      ),
    )

    val actual = analyzeQuery("MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = 1")

    assertEquals(actual, expected)
  }
}
