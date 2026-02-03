package com.thatdot.quine.language.server

import com.thatdot.quine.language.semantic.SemanticType

import LanguageServerHelper._

class QuineLanguageServerTest extends munit.FunSuite {
  var languageServer: QuineLanguageServer = null

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    languageServer = new QuineLanguageServer()
  }
  override def afterEach(context: AfterEach): Unit = {
    super.afterEach(context)
    languageServer = null
  }

  test("should be able to retrieve semantic token types") {
    val (tokenTypes, _) = getTokenTypesAndModifiers(languageServer)

    assertNotEquals(tokenTypes.length, 0)

    List(
      SemanticType.MatchKeyword,
      SemanticType.WhereKeyword,
      SemanticType.AsKeyword,
      SemanticType.FunctionApplication,
    ).foreach { keyword =>
      assert(tokenTypes.contains(keyword.toString))
    }
  }

  // This test currently fails. Implementation defines an empty list for tokenModifiers.
  test("should be able to retrieve semantic token modifiers".fail) {
    val (_, tokenModifiers) = getTokenTypesAndModifiers(languageServer)

    assertNotEquals(tokenModifiers.length, 0)
  }
}
