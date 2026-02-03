package com.thatdot.quine.language.server

import scala.util.Try

import munit._

import com.thatdot.quine.language.semantic.SemanticType

import TextDocumentServiceHelper._

class QuineTextDocumentServiceTest extends FunSuite {
  var textDocumentService: QuineTextDocumentService = _

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    textDocumentService = new QuineTextDocumentService(
      new ContextAwareLanguageService(),
    )
  }

  override def afterEach(context: AfterEach): Unit = {
    super.afterEach(context)
    textDocumentService = null
  }

  test(
    "should be able to invoke didOpen method, should persist document in TextDocumentManager and should be able to retrieve it back",
  ) {
    val uri = "file:///tmp/test.txt"
    val text = "MATCH (n) RETURN n"

    assert(textDocumentService.getTextDocument(uri) == null)

    openTextDocument(textDocumentService, uri, text)
    assertEquals(textDocumentService.getTextDocument(uri).getText(), text)
  }

  test(
    "should be able to invoke didChange, going from an empty TextDocument, and changing its text content",
  ) {
    val uri = "file:///tmp/test.txt"
    val text = "MATCH (n) RETURN n"

    openTextDocument(textDocumentService, uri, "")
    assertEquals(textDocumentService.getTextDocument(uri).getText, "")

    changeTextDocument(textDocumentService, uri, text)
    assertEquals(textDocumentService.getTextDocument(uri).getText, text)
  }

  test(
    "should be able to invoke didClose, which successfully removes text document from text document manager",
  ) {
    val uri = "file:///tmp/test.txt"

    openTextDocument(textDocumentService, uri, "")
    assert(textDocumentService.getTextDocument(uri) != null)

    closeTextDocument(textDocumentService, uri)
    assert(textDocumentService.getTextDocument(uri) == null)
  }

  test(
    "should be able to get intellisense by invoking completion method".fail,
  ) {
    val uri = "file:///tmp/test.txt"
    // Testing if I can get intellisense for the following cypher query. The completion should return "RETURN"
    val text = "MATCH (n) RETUR"
    openTextDocument(textDocumentService, uri, text)

    assertEquals(
      getCompletionItems(textDocumentService, uri, 0, 15).head.getLabel,
      "RETURN",
    )
  }

  test("should be able to get diagnostics on TextDocuments") {
    val uri = "file:///tmp/test.txt"
    // Should return a diagnostic error for the following cypher query. WHERE is misspelled
    val text = "MATCH (n) WHER id(n) = idFrom(\"Bob\") SET n.name = \"Bob\""
    openTextDocument(textDocumentService, uri, text)

    val expectedDiagnosticMessage =
      "no viable alternative at input 'MATCH (n) WHER'"
    val resultDiagnosticMessage =
      getDiagnostics(textDocumentService, uri).head.getMessage

    assertEquals(resultDiagnosticMessage, expectedDiagnosticMessage)
  }

  test("should be able to get semantic tokens for TextDocuments") {
    val uri = "file:///tmp/test.txt"
    // Should return a list of semantic tokens for the following cypher query.
    val text = "MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\""
    openTextDocument(textDocumentService, uri, text)

    val semanticTypes: List[SemanticType] =
      getSemanticTokens(textDocumentService, uri)
        .grouped(5)
        .toList
        .map((semanticTokens: List[Int]) => SemanticType.fromInt(semanticTokens(3)))

    val expectedListOfSemanticTypes = List(
      SemanticType.MatchKeyword,
      SemanticType.Variable,
      SemanticType.WhereKeyword,
      SemanticType.FunctionApplication,
      SemanticType.Variable,
      SemanticType.FunctionApplication,
      SemanticType.StringLiteral,
      SemanticType.Variable,
      SemanticType.Property,
      SemanticType.StringLiteral,
    )

    assert(semanticTypes.length == 10)
    assertEquals(semanticTypes, expectedListOfSemanticTypes)
  }

  test("should be able to perform semantic analysis, diagnostics, and completion, on various cypher texts") {
    case class Position(line: Int, char: Int)
    case class CypherWithPosition(query: String, position: Position)

    val uri = "file:///tmp/test.txt"
    val cypherStrings: List[CypherWithPosition] = List(
      CypherWithPosition("", Position(0, 0)),
      CypherWithPosition("MATCH (n) RETURN", Position(0, 16)),
    )

    for (CypherWithPosition(text, Position(line, char)) <- cypherStrings) {
      val textDocumentServiceActions = Try {
        openTextDocument(textDocumentService, uri, text)
        getSemanticTokens(textDocumentService, uri)
        getDiagnostics(textDocumentService, uri)
        getCompletionItems(textDocumentService, uri, line, char)
        closeTextDocument(textDocumentService, uri)
      }
      assert(textDocumentServiceActions.isSuccess)
    }
  }
}
