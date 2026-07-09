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

  test("queryKind is exposed as the custom JSON-RPC request method quine/queryKind") {
    // lsp4j discovers @JsonRequest-annotated methods on the local service object
    // (AnnotationUtil.findRpcMethods walks the concrete class), so this annotation
    // is the wire-level method registration.
    val method = classOf[QuineLanguageServer].getMethod("queryKind", classOf[QueryKindParams])
    val annotation = method.getAnnotation(classOf[org.eclipse.lsp4j.jsonrpc.services.JsonRequest])

    assertEquals(annotation.value, "quine/queryKind")
  }

  test("queryKind classifies an open document and answers unknown for unopened documents") {
    val uri = "file:///tmp/test.txt"
    TextDocumentServiceHelper.openTextDocument(languageServer.getTextDocumentService, uri, "MATCH (n) RETURN n")

    val openResult = languageServer
      .queryKind(new QueryKindParams(new org.eclipse.lsp4j.TextDocumentIdentifier(uri)))
      .get()
    assertEquals(openResult.getKind, QueryKind.NODE)

    val unopenedResult = languageServer
      .queryKind(new QueryKindParams(new org.eclipse.lsp4j.TextDocumentIdentifier("file:///tmp/other.txt")))
      .get()
    assertEquals(unopenedResult.getKind, QueryKind.UNKNOWN)
  }
}
