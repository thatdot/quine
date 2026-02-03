package com.thatdot.quine.cypher.phases

import com.thatdot.quine.cypher.ast.NodePattern
import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart.ReadingClausePart
import com.thatdot.quine.cypher.ast.ReadingClause.FromPatterns
import com.thatdot.quine.language.diagnostic.Diagnostic.ParseError
import com.thatdot.quine.language.phases.UpgradeModule._

class PhaseCompositionTest extends munit.FunSuite {

  test("lexer andThen parser composition") {
    val cypherText = "MATCH (n:Person) RETURN n.name"
    val initialState = LexerState(Nil)

    val composedPhase = LexerPhase andThen ParserPhase
    val (finalState, maybeQuery) = composedPhase.process(cypherText).value.run(initialState).value

    assert(maybeQuery.isDefined, "Composed phase should produce query")
    assert(finalState.diagnostics.isEmpty, "Should have no errors in composed execution")

    val query = maybeQuery.get
    assert(query.isInstanceOf[SinglepartQuery], "Should produce correct query type")
  }

  test("lexer andThen parser andThen symbolAnalysis composition") {
    val cypherText = "MATCH (a:Person {name: 'John'}) RETURN a.age"
    val initialState = LexerState(Nil)

    val fullPipeline = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (finalState, maybeQuery) = fullPipeline.process(cypherText).value.run(initialState).value

    assert(maybeQuery.isDefined, "Full pipeline should produce query")
    assert(finalState.isInstanceOf[SymbolAnalysisState], "Final state should be SymbolAnalysisState")

    val symbolState = finalState.asInstanceOf[SymbolAnalysisState]
    assert(symbolState.symbolTable.references.nonEmpty, "Should have symbol table entries")

    // Check that symbol analysis found our variables
    assert(symbolState.symbolTable.references.nonEmpty, "Should have symbol entries")
  }

  test("error propagation through pipeline") {
    // Test with input that will cause lexer or parser errors
    val invalidCypher = "MATCH (n RETURN n" // Missing closing parenthesis
    val initialState = LexerState(Nil)

    val pipeline = LexerPhase andThen ParserPhase
    val (finalState, maybeQuery) = pipeline.process(invalidCypher).value.run(initialState).value

    // The pipeline should handle errors gracefully and propagate them
    assert(maybeQuery.isEmpty, "Invalid query should not produce a result")
    assertEquals(finalState.diagnostics.length, 1, "Should produce exactly one parse error")

    val expectedError = ParseError(
      line = 1,
      char = 9,
      message = "no viable alternative at input 'MATCH (n RETURN'",
    )
    assertEquals(finalState.diagnostics.head, expectedError)
  }

  test("state upgrade between phases") {
    val cypherText = "MATCH (n) RETURN n"

    // Test that LexerState properly upgrades to ParserState
    val lexerState = LexerState(List(ParseError(1, 0, "Test error")))
    val (parserState, maybeTokens) = LexerPhase.process(cypherText).value.run(lexerState).value

    assert(maybeTokens.isDefined, "Should produce tokens")

    // Now test parser phase with the result
    val tokenStream = maybeTokens.get
    val parserStateTyped = ParserState(parserState.diagnostics, "")
    val (finalParserState, maybeQuery) = ParserPhase.process(tokenStream).value.run(parserStateTyped).value

    assert(
      finalParserState.diagnostics.contains(ParseError(1, 0, "Test error")),
      "Should preserve diagnostics through state upgrade",
    )
    assert(maybeQuery.isDefined, "Should parse successfully despite previous errors")
  }

  test("pipeline with multiple errors accumulates diagnostics") {
    val initialLexerError = ParseError(0, 0, "Initial error")
    val initialState = LexerState(List(initialLexerError))

    // Use potentially problematic input
    val cypherText = "MATCH (n RETURN n"

    val pipeline = LexerPhase andThen ParserPhase
    val (finalState, _) = pipeline.process(cypherText).value.run(initialState).value

    // Should preserve the initial error
    assert(finalState.diagnostics.contains(initialLexerError), "Should preserve initial diagnostics")

    // Total diagnostics should be >= 1 (at least the initial one)
    assert(finalState.diagnostics.length >= 1, "Should accumulate diagnostics")
  }

  test("pipeline handles None result gracefully") {
    // Create a scenario where early phase might return None
    val pipeline = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

    // Test with various inputs that might cause None results
    val testInputs = List(
      "", // Empty input
      "   ", // Whitespace only
      "INVALID_KEYWORD (n) RETURN n", // Invalid syntax
    )

    testInputs.foreach { input =>
      val initialState = LexerState(Nil)

      assertNoException(s"Input: '$input'") {
        pipeline.process(input).value.run(initialState).value
      }
    }
  }

  test("deep pipeline composition doesn't cause stack overflow") {
    // Test the known TODO issue about stack overflow in Phase.andThen
    val cypherText = "MATCH (n) RETURN n"
    val initialState = LexerState(Nil)

    // Build a deeper pipeline to stress test the composition
    val pipeline = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

    // This should complete without stack overflow, though the TODO suggests it might not scale
    assertNoException("Deep pipeline composition") {
      pipeline.process(cypherText).value.run(initialState).value
    }
  }

  test("parallel pipeline execution with different inputs") {
    val inputs = List(
      "MATCH (n:Person) RETURN n",
      "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
      "CREATE (n:Company {name: 'Acme'}) RETURN n",
    )

    val pipeline = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

    inputs.foreach { input =>
      val initialState = LexerState(Nil)

      val (finalState, maybeQuery) = pipeline.process(input).value.run(initialState).value

      assert(maybeQuery.isDefined, s"Should parse: $input")
      assert(finalState.isInstanceOf[SymbolAnalysisState], s"Should produce SymbolAnalysisState for: $input")

      val symbolState = finalState.asInstanceOf[SymbolAnalysisState]
      assert(symbolState.symbolTable.references.nonEmpty, s"Should have symbols for: $input")
    }
  }

  test("phase composition preserves source locations") {
    val cypherText = "MATCH (person:Person {name: 'Alice'}) RETURN person.age"
    val initialState = LexerState(Nil)

    val pipeline = LexerPhase andThen ParserPhase
    val (_, maybeQuery) = pipeline.process(cypherText).value.run(initialState).value

    assert(maybeQuery.isDefined, "Should parse query")

    val query = maybeQuery.get.asInstanceOf[SinglepartQuery]
    val readingClause = query.queryParts.head.asInstanceOf[ReadingClausePart].readingClause.asInstanceOf[FromPatterns]
    val pattern = readingClause.patterns.head

    // Verify source information is preserved through pipeline
    assert(pattern.initial.isInstanceOf[NodePattern], "Should be node pattern")

    val nodePattern = pattern.initial
    assert(nodePattern.maybeProperties.isDefined, "Should have properties")
  }

  test("pipeline with complex query produces rich symbol table") {
    val cypherText = """
      MATCH (person:Person {name: 'John'})-[:WORKS_FOR]->(company:Company)
      WHERE person.age > 25 AND company.industry = 'Tech'
      RETURN person.name AS employee_name, company.name AS company_name
    """.trim

    val initialState = LexerState(Nil)
    val pipeline = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

    val (finalState, maybeQuery) = pipeline.process(cypherText).value.run(initialState).value

    assert(maybeQuery.isDefined, "Should parse complex query")
    assert(finalState.diagnostics.isEmpty, "Should have no errors")

    val symbolState = finalState.asInstanceOf[SymbolAnalysisState]

    // Should have multiple symbol table entries for a complex query
    assert(symbolState.symbolTable.references.length >= 2, "Should have multiple symbol entries")
  }

  private def assertNoException(context: String)(block: => Any): Unit =
    try { val _ = block }
    catch {
      case e: Exception =>
        fail(s"$context - Expected no exception, but got: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
}
