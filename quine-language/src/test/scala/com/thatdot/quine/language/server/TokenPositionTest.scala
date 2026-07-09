package com.thatdot.quine.language.server

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, Token}

import com.thatdot.quine.cypher.parsing.CypherLexer

class TokenPositionTest extends munit.FunSuite {

  /** Lexes `text` to the full token vector (ending with EOF), as the language service does. */
  private def lex(text: String): Vector[Token] = {
    val lexer = new CypherLexer(CharStreams.fromString(text))
    lexer.removeErrorListeners()
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()
    tokenStream.getTokens.asScala.toVector
  }

  // --- Totality: caretTokenIndex must be total, never throwing on a token vector that does not
  // match the caret (only reachable when the EOF token ANTLR normally appends is absent). Before
  // the fix `tokens(indexWhere(...))` threw IndexOutOfBounds on the -1 miss. ---

  test("caretTokenIndex returns None for an empty token vector instead of throwing") {
    assertEquals(TokenPosition.caretTokenIndex(Vector.empty, 0), None)
  }

  test("caretTokenIndex returns None when no token matches the caret (EOF token absent)") {
    val withoutEof = lex("abc").filterNot(_.getType == Token.EOF)
    assert(withoutEof.nonEmpty, "expected a word token before EOF was dropped")
    // Caret past the end of every remaining token: indexWhere finds no match.
    assertEquals(TokenPosition.caretTokenIndex(withoutEof, 100), None)
  }

  // --- Behavior preserved by the extraction/refactor. ---

  test("caretTokenIndex at the start of a word selects that word's token") {
    assertEquals(TokenPosition.caretTokenIndex(lex("MATCH"), 0), Some(0))
  }

  test("caretTokenIndex in an empty buffer resolves to the EOF token's index") {
    val tokens = lex("")
    val eofIndex = tokens.indexWhere(_.getType == Token.EOF)
    assertEquals(TokenPosition.caretTokenIndex(tokens, 0), Some(eofIndex))
  }

  test("dottedNameSpan expands a word to its full dotted run") {
    val tokens = lex("RETURN gen.integer")
    // The hovered word `gen` (or `integer`) belongs to the span covering both.
    val genIndex = tokens.indexWhere(_.getText == "gen")
    val integerIndex = tokens.indexWhere(_.getText == "integer")
    assertEquals(TokenPosition.dottedNameSpan(tokens, genIndex), TokenPosition.TokenSpan(genIndex, integerIndex))
    assertEquals(TokenPosition.dottedNameSpan(tokens, integerIndex), TokenPosition.TokenSpan(genIndex, integerIndex))
  }

  test("committedDottedPrefix is empty for a plain word and the namespace for a dotted caret") {
    val plain = lex("RETURN re")
    assertEquals(TokenPosition.committedDottedPrefix(plain, "RETURN re".length), "")

    val dotted = lex("RETURN gen.")
    assertEquals(TokenPosition.committedDottedPrefix(dotted, "RETURN gen.".length), "gen.")
  }
}
