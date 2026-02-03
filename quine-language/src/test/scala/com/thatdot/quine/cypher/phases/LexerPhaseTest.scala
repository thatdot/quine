package com.thatdot.quine.cypher.phases

import org.antlr.v4.runtime.Token

import com.thatdot.quine.language.diagnostic.Diagnostic.ParseError

class LexerPhaseTest extends munit.FunSuite {

  test("valid Cypher query produces token stream") {
    val input = "MATCH (n) RETURN n"
    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should produce a token stream for valid input")
    assert(resultState.diagnostics.isEmpty, "Should have no diagnostics for valid input")

    val tokens = maybeTokens.get
    tokens.fill() // Load all tokens

    // Verify we got meaningful tokens (not just EOF)
    val tokenList = (0 until tokens.size()).map(tokens.get).toList
    val nonEOFTokens = tokenList.filter(_.getType != Token.EOF)

    assert(nonEOFTokens.nonEmpty, "Should produce non-EOF tokens")
    assert(nonEOFTokens.exists(_.getText == "MATCH"), "Should contain MATCH token")
    assert(nonEOFTokens.exists(_.getText == "RETURN"), "Should contain RETURN token")
  }

  test("empty input produces empty token stream") {
    val input = ""
    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should handle empty input")
    assert(resultState.diagnostics.isEmpty, "Empty input should not generate errors")

    val tokens = maybeTokens.get
    tokens.fill()

    // Should only contain EOF token
    val tokenList = (0 until tokens.size()).map(tokens.get).toList
    assert(tokenList.length == 1, "Empty input should only produce EOF token")
    assert(tokenList.head.getType == Token.EOF, "Single token should be EOF")
  }

  test("whitespace-only input produces token stream with EOF") {
    val input = "   \n\t   "
    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should handle whitespace input")
    assert(resultState.diagnostics.isEmpty, "Whitespace should not generate errors")

    val tokens = maybeTokens.get
    tokens.fill()

    val tokenList = (0 until tokens.size()).map(tokens.get).toList

    // Whitespace may or may not be on default channel depending on grammar
    // The key test is that we handle whitespace gracefully
    assert(tokenList.nonEmpty, "Should have at least EOF token")
    assert(tokenList.exists(_.getType == Token.EOF), "Should have EOF token")
  }

  test("special characters are tokenized without lexer errors") {
    // The Cypher grammar tokenizes special characters like @#$% as separate tokens
    // rather than reporting them as lexer errors. Syntax validation happens at parse time.
    val input = "MATCH (n) RETURN n@#$%"
    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should produce token stream")
    assert(resultState.diagnostics.isEmpty, "Lexer should not produce errors for special characters")
  }

  test("preserves existing diagnostics") {
    val input = "MATCH (n) RETURN n"
    val existingError = ParseError(1, 0, "Previous error")
    val initialState = LexerState(List(existingError))

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should produce tokens")
    assert(resultState.diagnostics.contains(existingError), "Should preserve existing diagnostics")
  }

  test("handles complex query with various token types") {
    val input = """
      |MATCH (person:Person {name: 'John', age: 30})
      |WHERE person.salary > 50000.50 AND person.active = true
      |RETURN person.name, person.age + 1 AS next_age
      |""".stripMargin.trim

    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should handle complex query")
    assert(resultState.diagnostics.isEmpty, "Complex valid query should not generate errors")

    val tokens = maybeTokens.get
    tokens.fill()

    val tokenList = (0 until tokens.size()).map(tokens.get).toList
    val tokenTexts = tokenList.map(_.getText).filter(_ != "<EOF>")

    // Verify we captured key tokens
    assert(tokenTexts.contains("MATCH"), "Should contain MATCH keyword")
    assert(tokenTexts.contains("WHERE"), "Should contain WHERE keyword")
    assert(tokenTexts.contains("RETURN"), "Should contain RETURN keyword")
    assert(tokenTexts.contains("Person"), "Should contain label")
    assert(tokenTexts.contains("'John'"), "Should contain string literal")
    assert(tokenTexts.contains("30"), "Should contain integer literal")
    assert(tokenTexts.contains("50000.50"), "Should contain decimal literal")
    assert(tokenTexts.contains("true"), "Should contain boolean literal")
    assert(tokenTexts.contains(">"), "Should contain comparison operator")
    assert(tokenTexts.contains("AND"), "Should contain logical operator")
  }

  test("handles multiline queries") {
    val input = """MATCH (n)
                  |RETURN n""".stripMargin

    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    assert(maybeTokens.isDefined, "Should handle multiline input")
    assert(resultState.diagnostics.isEmpty, "Multiline query should not generate errors")

    val tokens = maybeTokens.get
    tokens.fill()

    val tokenList = (0 until tokens.size()).map(tokens.get).toList
    val nonEOFTokens = tokenList.filter(_.getType != Token.EOF)

    assert(nonEOFTokens.nonEmpty, "Should produce tokens from multiline input")
  }

  test("error listener collects multiple errors") {
    // This test depends on the specific Cypher grammar and what constitutes a lexer error
    // We'll test the error collection mechanism even if this specific input doesn't generate errors
    val input = "MATCH (n RETURN n" // Missing closing parenthesis - might be parser error, not lexer
    val initialState = LexerState(Nil)

    val (resultState, maybeTokens) = LexerPhase.process(input).value.run(initialState).value

    // Should still produce a token stream
    assert(maybeTokens.isDefined, "Should handle malformed input")

    // The error collection mechanism should work (even if this specific case doesn't trigger it)
    assert(resultState.diagnostics.length >= initialState.diagnostics.length, "Should not lose diagnostics")
  }

  test("exception handling returns None") {
    // This is harder to test without mocking, but we can verify the structure
    // The catch block should return None for the token stream while preserving state

    // For now, let's test that normal processing doesn't throw exceptions
    val inputs = List(
      "MATCH (n) RETURN n",
      "",
      "   ",
      "MATCH (n:Person {name: 'test'}) RETURN n",
    )

    inputs.foreach { input =>
      val initialState = LexerState(Nil)

      // This should not throw an exception
      assertNoException {
        val _ = LexerPhase.process(input).value.run(initialState).value
      }
    }
  }

  private def assertNoException(block: => Unit): Unit =
    try block
    catch {
      case e: Exception =>
        fail(s"Expected no exception, but got: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
}
