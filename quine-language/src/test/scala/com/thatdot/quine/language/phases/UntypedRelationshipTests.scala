package com.thatdot.quine.language.phases

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, MaterializationPhase, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.diagnostic.Diagnostic.{ParseError, TypeCheckError}
import com.thatdot.quine.language.semantic.SemanticType
import com.thatdot.quine.language.server.{ContextAwareLanguageService, QueryKind}

class UntypedRelationshipTests extends munit.FunSuite {
  import com.thatdot.quine.language.phases.UpgradeModule._
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

  private val cals = new ContextAwareLanguageService()

  private val pipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase() andThen MaterializationPhase

  private def parse(query: String): (TypeCheckingState, Option[Query]) =
    pipeline.process(query).value.run(LexerState(Nil)).value

  private def errors(diagnostics: List[Diagnostic]): List[Diagnostic] =
    diagnostics.filter {
      case _: ParseError => true
      case _: TypeCheckError => true
      case _ => false
    }

  private def assertParses(query: String): Unit = {
    val (state, maybeQuery) = parse(query)
    assert(maybeQuery.isDefined, s"should produce an AST for: $query")
    assertEquals(errors(state.diagnostics), Nil, s"should have no errors for: $query")
  }

  test("untyped relationship with empty brackets: -[]->") {
    assertParses("MATCH (n)-[]->(m) RETURN n")
  }

  test("anonymous relationship without brackets: -->") {
    assertParses("MATCH (n)-->(m) RETURN n")
  }

  test("named but untyped relationship: -[r]->") {
    assertParses("MATCH (n)-[r]->(m) RETURN n")
  }

  test("typed relationship still works: -[:T]->") {
    assertParses("MATCH (n)-[:T]->(m) RETURN n")
  }

  // The three LSP-facing entry points must survive every relationship shape: an arrow-only `-->`
  // has no relationship detail, and an undirected `--`/`-[r]-` is unsupported by Quine but must
  // degrade to a clean diagnostic rather than throwing out of the analysis request.

  private val directedArrowOnly = "MATCH (n)-->(m) RETURN n"
  private val undirectedQueries = List("MATCH (n)--(m) RETURN n", "MATCH (n)-[r]-(m) RETURN r")

  private def edgeTokens(query: String): List[SemanticType] =
    cals.semanticAnalysis(query).asScala.toList.map(_.semanticType).filter(_ == SemanticType.Edge)

  test("arrow-only relationship: semantic analysis does not throw and highlights the edge") {
    assert(edgeTokens(directedArrowOnly).nonEmpty, "arrow-only relationship should emit an Edge token")
  }

  test("arrow-only relationship: parses clean and classifies as a node query") {
    assertEquals(cals.parseErrors(directedArrowOnly).asScala.toList, Nil)
    assertEquals(cals.queryKind(directedArrowOnly), QueryKind.NODE)
  }

  test("undirected relationship: semantic analysis does not throw and highlights the edge") {
    undirectedQueries.foreach { query =>
      assert(edgeTokens(query).nonEmpty, s"undirected relationship should emit an Edge token: $query")
    }
  }

  test("undirected relationship: reported as unsupported, does not throw, classifies as unknown") {
    undirectedQueries.foreach { query =>
      val diagnostics = cals.parseErrors(query).asScala.toList
      assert(
        diagnostics.exists(_.message.toLowerCase.contains("undirected")),
        s"undirected relationship should report an 'undirected' diagnostic: $query ($diagnostics)",
      )
      assertEquals(cals.queryKind(query), QueryKind.UNKNOWN, s"undirected relationship should be unknown: $query")
    }
  }
}
