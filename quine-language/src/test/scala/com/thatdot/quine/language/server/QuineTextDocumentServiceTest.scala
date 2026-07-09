package com.thatdot.quine.language.server

import scala.util.Try

import munit._
import org.eclipse.lsp4j.{
  CompletionItemKind,
  DiagnosticSeverity,
  MarkupKind,
  Position,
  Range,
  TextDocumentContentChangeEvent,
}

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

  test("queryKind classifies the stored document text and tracks didChange") {
    val uri = "file:///tmp/test.txt"
    def queryKind(uri: String): QueryKind =
      textDocumentService
        .queryKind(new QueryKindParams(new org.eclipse.lsp4j.TextDocumentIdentifier(uri)))
        .getKind

    openTextDocument(textDocumentService, uri, "MATCH (n) RETURN n")
    assertEquals(queryKind(uri), QueryKind.NODE)

    changeTextDocument(textDocumentService, uri, "MATCH (n) RETURN n.prop")
    assertEquals(queryKind(uri), QueryKind.TABLE)
  }

  test("queryKind answers unknown for a document that was never opened") {
    val result = textDocumentService.queryKind(
      new QueryKindParams(new org.eclipse.lsp4j.TextDocumentIdentifier("file:///tmp/never-opened.txt")),
    )
    assertEquals(result.getKind, QueryKind.UNKNOWN)
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

  test("didChange with a ranged content change applies an incremental edit to the stored text") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) RETURN n")

    // Monaco's LSP client reports edits as ranged TextDocumentContentChangeEvents:
    // replacing the space after "(n)" with a newline turns the query multi-line.
    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 9), new Position(0, 10)), "\n"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "MATCH (n)\nRETURN n")
    assertEquals(getDiagnostics(textDocumentService, uri), List.empty[org.eclipse.lsp4j.Diagnostic])
  }

  test("didChange applies content changes in order, each against the text the previous one produced") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) RETURN n")

    // LSP 3.17: for changes c1 and c2 on a document in state S, c1 moves the
    // document from S to S' and c2 is computed on S'. The second change's range
    // (line 1) only exists once the first change has introduced the newline.
    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 9), new Position(0, 10)), "\n"),
        new TextDocumentContentChangeEvent(new Range(new Position(1, 8), new Position(1, 8)), " LIMIT 1"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "MATCH (n)\nRETURN n LIMIT 1")
  }

  test("didChange clamps a ranged change's positions to the document") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n)")

    // LSP 3.17: a character value greater than the line length defaults back to
    // the line length, so this change appends at the end of the line.
    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 50), new Position(0, 50)), " RETURN n"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "MATCH (n) RETURN n")
  }

  test("didChange resolves ranged-edit positions in UTF-16 code units across a surrogate pair") {
    val uri = "file:///tmp/test.txt"
    // The grinning-face emoji is one Unicode code point but two UTF-16 code units.
    // In `RETURN "😀" AS x` the columns are, in UTF-16 units: R0 E1 T2 U3 R4 N5
    // space6 quote7 then the emoji at 8-9 then quote10 space11 A12 S13 space14 x15.
    // LSP positions count UTF-16 code units (the protocol's default position
    // encoding), and Java String offsets are themselves UTF-16 code units, so an
    // LSP column maps directly to a String offset with no surrogate conversion.
    openTextDocument(textDocumentService, uri, "RETURN \"😀\" AS x")

    // Replace the alias `x` at UTF-16 columns 15-16 — a range whose start sits past
    // the surrogate pair. A correct conversion lands exactly on `x`, not one unit
    // off (which a code-point miscount would produce).
    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 15), new Position(0, 16)), "emoji"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "RETURN \"😀\" AS emoji")
  }

  test("didChange inserts at a zero-width range immediately after a surrogate pair") {
    val uri = "file:///tmp/test.txt"
    // The closing quote sits at UTF-16 column 10 (the emoji occupies 8-9). A
    // zero-width insertion at column 10 must land between the emoji and the quote,
    // confirming the offset lands on the trailing surrogate boundary, not before it.
    openTextDocument(textDocumentService, uri, "RETURN \"😀\" AS x")

    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 10), new Position(0, 10)), "!"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "RETURN \"😀!\" AS x")
  }

  test("a ranged didChange across a surrogate pair followed by diagnostics produces no off-by-one error range") {
    val uri = "file:///tmp/test.txt"
    // Round-trips an offset through the ranged-edit path and then through the
    // diagnostic-range path on the same surrogate-bearing buffer. Misspelling the
    // alias keyword after the emoji makes the parser error observable; the
    // resulting LSP range must count the emoji as two UTF-16 units.
    openTextDocument(textDocumentService, uri, "RETURN \"😀\" A x")

    // Replace the bad `A` (UTF-16 columns 12-13) with `AS`, producing a valid query.
    changeTextDocument(
      textDocumentService,
      uri,
      List(
        new TextDocumentContentChangeEvent(new Range(new Position(0, 12), new Position(0, 13)), "AS"),
      ),
    )

    assertEquals(textDocumentService.getTextDocument(uri).getText, "RETURN \"😀\" AS x")
    assertEquals(getDiagnostics(textDocumentService, uri), List.empty[org.eclipse.lsp4j.Diagnostic])
  }

  test("didChange without a range replaces the whole document") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) RETURN n")

    changeTextDocument(textDocumentService, uri, "MATCH (m)\nRETURN m\n")

    assertEquals(textDocumentService.getTextDocument(uri).getText, "MATCH (m)\nRETURN m\n")
    assertEquals(getDiagnostics(textDocumentService, uri), List.empty[org.eclipse.lsp4j.Diagnostic])
  }

  test("an empty document produces no diagnostics") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "")

    assertEquals(getDiagnostics(textDocumentService, uri), List.empty[org.eclipse.lsp4j.Diagnostic])
  }

  test("a whitespace-only document produces no diagnostics") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, " \n\t\n")

    assertEquals(getDiagnostics(textDocumentService, uri), List.empty[org.eclipse.lsp4j.Diagnostic])
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
    "should be able to get intellisense by invoking completion method",
  ) {
    val uri = "file:///tmp/test.txt"
    // The caret touches the partially typed RETURN keyword, so the grammar-driven
    // candidates are the keywords valid where the word starts; the client filters
    // them against the typed prefix (LSP filterText defaults to the label).
    val text = "MATCH (n) RETUR"
    openTextDocument(textDocumentService, uri, text)

    val items = getCompletionItems(textDocumentService, uri, 0, 15)
    // The list now mixes keyword items with function/procedure name items; the
    // grammar-driven keyword assertions are scoped to the keyword-kind items.
    val keywordItems = items.filter(_.getKind == CompletionItemKind.Keyword)
    val keywords = keywordItems.map(_.getLabel)

    assert(keywords.contains("RETURN"), s"RETURN is valid after a MATCH clause; got $keywords")
    assert(!keywords.contains("LIMIT"), s"LIMIT is not valid after a MATCH clause; got $keywords")
    keywordItems.foreach { item =>
      assertEquals(item.getInsertText, item.getLabel)
    }
  }

  test("completion offers function and procedure name items alongside keywords") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "RETURN ")

    val items = getCompletionItems(textDocumentService, uri, 0, 7)
    val kinds = items.map(_.getKind).toSet

    assert(kinds.contains(CompletionItemKind.Keyword), s"expected keyword items; got kinds $kinds")
    assert(kinds.contains(CompletionItemKind.Function), s"expected function items; got kinds $kinds")
    assert(kinds.contains(CompletionItemKind.Method), s"expected procedure (Method) items; got kinds $kinds")
  }

  test("name completion items carry kind, signature detail, and markdown docs for documented names") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "RETURN ")

    val items = getCompletionItems(textDocumentService, uri, 0, 7)

    val idFrom = items.find(_.getLabel == "idFrom").getOrElse(fail("expected an idFrom completion"))
    assertEquals(idFrom.getKind, CompletionItemKind.Function)
    assert(idFrom.getDetail != null && idFrom.getDetail.contains("idFrom("), s"idFrom detail: ${idFrom.getDetail}")
    val docs = idFrom.getDocumentation.getRight
    assertEquals(docs.getKind, MarkupKind.MARKDOWN)
    assert(docs.getValue.nonEmpty, "idFrom carries a documentation body")

    val recentNodes = items.find(_.getLabel == "recentNodes").getOrElse(fail("expected a recentNodes completion"))
    assertEquals(recentNodes.getKind, CompletionItemKind.Method)

    // A standard openCypher built-in is offered by name only — no signature, no docs.
    val toInteger = items.find(_.getLabel == "toInteger").getOrElse(fail("expected a toInteger completion"))
    assertEquals(toInteger.getKind, CompletionItemKind.Function)
    assertEquals(toInteger.getDetail, null)
    assertEquals(toInteger.getDocumentation, null)
  }

  test("a full-name function completion replaces the typed word with the whole name") {
    val uri = "file:///tmp/test.txt"
    // The caret is on the plain word idFr, no dot before it, so the whole name is inserted.
    openTextDocument(textDocumentService, uri, "RETURN idFr")

    val items = getCompletionItems(textDocumentService, uri, 0, 11)
    val idFrom = items.find(_.getLabel == "idFrom").getOrElse(fail("expected an idFrom completion"))

    val edit = idFrom.getTextEdit.getLeft
    assertEquals(edit.getRange, new Range(new Position(0, 7), new Position(0, 11)))
    assertEquals(edit.getNewText, "idFrom")
    assertEquals(idFrom.getInsertText, "idFrom")
    assertEquals(applyEdit("RETURN idFr", edit), "RETURN idFrom")
  }

  test("an after-dot procedure completion replaces only the segment, reconstructing the full name") {
    val uri = "file:///tmp/test.txt"
    // The caret follows reify., so accepting reify.time must replace only the ti segment.
    openTextDocument(textDocumentService, uri, "CALL reify.ti")

    val items = getCompletionItems(textDocumentService, uri, 0, 13)
    val reifyTime = items.find(_.getLabel == "reify.time").getOrElse(fail("expected a reify.time completion"))

    val edit = reifyTime.getTextEdit.getLeft
    assertEquals(edit.getRange, new Range(new Position(0, 11), new Position(0, 13)))
    assertEquals(edit.getNewText, "time")
    assertEquals(reifyTime.getInsertText, "time")
    // Applying the edit reconstructs the full dotted name rather than reify.reify.time.
    assertEquals(applyEdit("CALL reify.ti", edit), "CALL reify.time")
  }

  test("completion items carry a textEdit replacing the word prefix before the caret") {
    val uri = "file:///tmp/test.txt"
    // The caret sits at the end of the partially typed RETUR (columns 10-14), so
    // accepting an item must replace that prefix, not insert at the caret. The
    // textEdit range also anchors the client's prefix filtering (Monaco computes
    // the word to filter against from the edit range's start).
    openTextDocument(textDocumentService, uri, "MATCH (n) RETUR")

    val items = getCompletionItems(textDocumentService, uri, 0, 15)

    assert(items.nonEmpty, "expected completion items")
    items.foreach { item =>
      val edit = item.getTextEdit
      assert(edit != null, s"item ${item.getLabel} carries a textEdit")
      assertEquals(edit.getLeft.getRange, new Range(new Position(0, 10), new Position(0, 15)))
      assertEquals(edit.getLeft.getNewText, item.getLabel)
    }
  }

  test("completion textEdit replaces only up to a mid-word caret") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) RETUR")

    // LSP 3.17: a completion textEdit range must be single-line and contain the
    // requested position; with the caret inside RETUR only the prefix before the
    // caret is replaced.
    val items = getCompletionItems(textDocumentService, uri, 0, 12)

    assert(items.nonEmpty, "expected completion items")
    items.foreach { item =>
      assertEquals(item.getTextEdit.getLeft.getRange, new Range(new Position(0, 10), new Position(0, 12)))
    }
  }

  test("completion textEdit collapses to the caret when the caret does not touch a word") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) ")

    val items = getCompletionItems(textDocumentService, uri, 0, 10)

    assert(items.nonEmpty, "expected completion items")
    items.foreach { item =>
      assertEquals(item.getTextEdit.getLeft.getRange, new Range(new Position(0, 10), new Position(0, 10)))
    }
  }

  test("completion textEdit covers the word prefix on later lines of multi-line input") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n)\nWHER")

    val items = getCompletionItems(textDocumentService, uri, 1, 4)

    assert(items.map(_.getLabel).contains("WHERE"), "WHERE is valid on the second line")
    items.foreach { item =>
      assertEquals(item.getTextEdit.getLeft.getRange, new Range(new Position(1, 0), new Position(1, 4)))
    }
  }

  test("completion textEdit clamps an out-of-range caret to the line end") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) RETUR")

    // LSP 3.17: a character beyond the line length defaults back to the line length.
    val items = getCompletionItems(textDocumentService, uri, 0, 99)

    assert(items.nonEmpty, "expected completion items")
    items.foreach { item =>
      assertEquals(item.getTextEdit.getLeft.getRange, new Range(new Position(0, 10), new Position(0, 15)))
    }
  }

  test("completion textEdit counts characters in UTF-16 code units") {
    val uri = "file:///tmp/test.txt"
    // The ghost emoji is one code point but two UTF-16 code units; RETUR occupies
    // UTF-16 columns 27-31, so the prefix range before the caret is (0,27)-(0,32).
    openTextDocument(textDocumentService, uri, "MATCH (n) WHERE n.x = \"👻\" RETUR")

    val items = getCompletionItems(textDocumentService, uri, 0, 32)

    assert(items.map(_.getLabel).contains("RETURN"), "RETURN is valid where RETUR is typed")
    items.foreach { item =>
      assertEquals(item.getTextEdit.getLeft.getRange, new Range(new Position(0, 27), new Position(0, 32)))
    }
  }

  test("hover on an idFrom invocation returns markdown documentation with the docs link") {
    val uri = "file:///tmp/test.txt"
    // idFrom occupies columns 24-29 (0-based); its opening parenthesis is at column 30.
    openTextDocument(textDocumentService, uri, "MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\"")

    val hover = getHover(textDocumentService, uri, 0, 27)

    assert(hover != null, "expected a hover for idFrom")
    val contents = hover.getContents.getRight
    assertEquals(contents.getKind, MarkupKind.MARKDOWN)
    assert(contents.getValue.contains("idFrom("), "hover markdown shows the signature")
    assert(
      contents.getValue.contains("https://quine.io/core-concepts/id-provider/#idfrom"),
      s"hover markdown links to the quine.io docs: ${contents.getValue}",
    )
    // LSP 3.17: the optional range marks the text the hover applies to — here, the function name.
    assertEquals(hover.getRange, new Range(new Position(0, 24), new Position(0, 30)))
  }

  test("hover answers at the first character and at the end boundary of the function name") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\"")

    for (character <- List(24, 30)) {
      val hover = getHover(textDocumentService, uri, 0, character)
      assert(hover != null, s"expected a hover for idFrom at character $character")
      assertEquals(hover.getRange, new Range(new Position(0, 24), new Position(0, 30)))
    }
  }

  test("hover on keywords and variables answers null") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) WHERE id(n) = idFrom(\"Bob\") SET n.name = \"Bob\"")

    // LSP 3.17: a null result is the no-hover answer.
    assertEquals(getHover(textDocumentService, uri, 0, 2), null) // MATCH keyword
    assertEquals(getHover(textDocumentService, uri, 0, 7), null) // variable n
    assertEquals(getHover(textDocumentService, uri, 0, 33), null) // string literal "Bob"
  }

  test("hover on a variable that shares a function's name answers null without an invocation") {
    val uri = "file:///tmp/test.txt"
    // The variable `datetime` is never invoked (no parenthesis follows), so it must not
    // hover as the datetime() function it happens to share a name with.
    openTextDocument(textDocumentService, uri, "MATCH (datetime) RETURN datetime")

    assertEquals(getHover(textDocumentService, uri, 0, 26), null)

    // The same name in invocation position does hover.
    changeTextDocument(textDocumentService, uri, "RETURN datetime()")
    val hover = getHover(textDocumentService, uri, 0, 9)
    assert(hover != null, "expected a hover for the datetime() invocation")
    assert(hover.getContents.getRight.getValue.contains("https://quine.io/reference/cypher/temporal-functions/"))
  }

  test("hover on standard Cypher functions answers null") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "MATCH (n) WHERE id(n) = idFrom(\"Bob\") RETURN n")

    // id() is standard Cypher, not a Quine addition: nothing to document.
    assertEquals(getHover(textDocumentService, uri, 0, 17), null)
  }

  test("hover covers the full dotted name of a namespaced function") {
    val uri = "file:///tmp/test.txt"
    // text.split spans columns 7-16; hovering either name segment documents the function.
    openTextDocument(textDocumentService, uri, "RETURN text.split(\"a,b\", \",\")")

    for (character <- List(8, 13)) {
      val hover = getHover(textDocumentService, uri, 0, character)
      assert(hover != null, s"expected a hover for text.split at character $character")
      assert(hover.getContents.getRight.getValue.contains("text.split("))
      assertEquals(hover.getRange, new Range(new Position(0, 7), new Position(0, 17)))
    }
  }

  test("hover positions and ranges count characters in UTF-16 code units") {
    val uri = "file:///tmp/test.txt"
    // The ghost emoji is one code point but two UTF-16 code units; idFrom occupies
    // UTF-16 columns 39-44.
    openTextDocument(
      textDocumentService,
      uri,
      "MATCH (n) WHERE n.x = \"👻\" AND id(n) = idFrom(\"Bob\") RETURN n",
    )

    val hover = getHover(textDocumentService, uri, 0, 41)

    assert(hover != null, "expected a hover for idFrom after the emoji")
    assertEquals(hover.getRange, new Range(new Position(0, 39), new Position(0, 45)))
  }

  test("hover on a document that was never opened answers null") {
    assertEquals(getHover(textDocumentService, "file:///tmp/never-opened.txt", 0, 0), null)
  }

  test("hover on a reify.time call documents the procedure from either name segment") {
    val uri = "file:///tmp/test.txt"
    // reify.time spans columns 5-14 (0-based); its opening parenthesis is at column 15.
    openTextDocument(textDocumentService, uri, "CALL reify.time(datetime(), [\"year\"]) YIELD node RETURN node")

    for (character <- List(7, 12)) {
      val hover = getHover(textDocumentService, uri, 0, character)
      assert(hover != null, s"expected a hover for reify.time at character $character")
      val contents = hover.getContents.getRight
      assertEquals(contents.getKind, MarkupKind.MARKDOWN)
      assert(contents.getValue.contains("reify.time("), "hover markdown shows the signature")
      assert(
        contents.getValue.contains("https://quine.io/reference/cypher/reify-time/"),
        s"hover markdown links to the reify-time reference: ${contents.getValue}",
      )
      // LSP 3.17: the optional range marks the text the hover applies to — the procedure name.
      assertEquals(hover.getRange, new Range(new Position(0, 5), new Position(0, 15)))
    }
  }

  test("hover on a function invoked inside a procedure's arguments documents the function") {
    val uri = "file:///tmp/test.txt"
    // datetime occupies columns 16-23 inside reify.time's argument list.
    openTextDocument(textDocumentService, uri, "CALL reify.time(datetime(), [\"year\"]) YIELD node RETURN node")

    val hover = getHover(textDocumentService, uri, 0, 18)

    assert(hover != null, "expected a hover for the datetime() invocation")
    assert(hover.getContents.getRight.getValue.contains("https://quine.io/reference/cypher/temporal-functions/"))
    assertEquals(hover.getRange, new Range(new Position(0, 16), new Position(0, 24)))
  }

  test("hover documents a procedure in standalone and in-query YIELD call forms") {
    val uri = "file:///tmp/test.txt"
    // recentNodes spans columns 5-15; its opening parenthesis is at column 16.
    for (text <- List("CALL recentNodes(10)", "CALL recentNodes(10) YIELD node RETURN node")) {
      openTextDocument(textDocumentService, uri, text)

      val hover = getHover(textDocumentService, uri, 0, 8)

      assert(hover != null, s"expected a hover for recentNodes in: $text")
      val contents = hover.getContents.getRight
      assert(contents.getValue.contains("recentNodes("), "hover markdown shows the signature")
      assert(
        contents.getValue.contains("https://quine.io/reference/cypher/cypher-procedures/"),
        s"hover markdown links to the procedures reference: ${contents.getValue}",
      )
      assertEquals(hover.getRange, new Range(new Position(0, 5), new Position(0, 16)))
    }
  }

  test("hover on an unknown procedure answers null") {
    val uri = "file:///tmp/test.txt"
    openTextDocument(textDocumentService, uri, "CALL myCustomProcedure(10)")

    // LSP 3.17: a null result is the no-hover answer.
    assertEquals(getHover(textDocumentService, uri, 0, 8), null)
  }

  test("hover on a variable that shares a procedure's name answers null without an invocation") {
    val uri = "file:///tmp/test.txt"
    // The variable `log` is never invoked (no parenthesis follows), so it must not hover
    // as the log procedure it happens to share a name with.
    openTextDocument(textDocumentService, uri, "MATCH (log) RETURN log")

    assertEquals(getHover(textDocumentService, uri, 0, 8), null)
    assertEquals(getHover(textDocumentService, uri, 0, 20), null)
  }

  test("should be able to get diagnostics on TextDocuments") {
    val uri = "file:///tmp/test.txt"
    // Should return a diagnostic error for the following cypher query. WHERE is misspelled
    val text = "MATCH (n) WHER id(n) = idFrom(\"Bob\") SET n.name = \"Bob\""
    openTextDocument(textDocumentService, uri, text)

    val expectedDiagnosticMessage =
      "no viable alternative at input 'MATCH (n) WHER'"
    val resultDiagnostic =
      getDiagnostics(textDocumentService, uri).head

    assertEquals(resultDiagnostic.getMessage, expectedDiagnosticMessage)
    // The misspelled WHER occupies columns 10-13, so the LSP range (0-based,
    // end-exclusive) covering the offending token is (0,10)-(0,14).
    assertEquals(resultDiagnostic.getRange, new Range(new Position(0, 10), new Position(0, 14)))
    assertEquals(resultDiagnostic.getSeverity, DiagnosticSeverity.Error)
  }

  test("every diagnostic carries an explicit severity") {
    val uri = "file:///tmp/test.txt"
    // Two diagnostic kinds: a misspelled WHERE is a ParseError, and a query
    // that parses but projects an undefined variable is a SymbolAnalysisError
    val queries = List(
      "MATCH (n) WHER id(n) = 1 RETURN n",
      "MATCH (n) RETURN m",
    )

    for (text <- queries) {
      openTextDocument(textDocumentService, uri, text)
      val diagnostics = getDiagnostics(textDocumentService, uri)
      closeTextDocument(textDocumentService, uri)

      assert(diagnostics.nonEmpty, s"expected at least one diagnostic for: $text")
      // LSP 3.17 leaves the interpretation of an omitted severity to the
      // client, so the server must set one explicitly. Everything the pipeline
      // reports for these queries is a hard error.
      diagnostics.foreach(d => assertEquals(d.getSeverity, DiagnosticSeverity.Error))
    }
  }

  test("diagnostic ranges cover the offending token on later lines of multi-line input") {
    val uri = "file:///tmp/test.txt"
    // WHERE is misspelled on the second line of the query
    val text = "MATCH (n)\nWHER id(n) = 1 RETURN n"
    openTextDocument(textDocumentService, uri, text)

    val resultDiagnostic = getDiagnostics(textDocumentService, uri).head

    // ANTLR reports the WHER token at line 2 (1-based), column 0; in LSP
    // coordinates (0-based) that is line 1, characters 0-4 (end-exclusive).
    assertEquals(resultDiagnostic.getRange, new Range(new Position(1, 0), new Position(1, 4)))
  }

  test("diagnostic ranges collapse to a zero-width range for errors at end of input") {
    val uri = "file:///tmp/test.txt"
    // RETURN is missing its projection, so the parser errors at the EOF token
    val text = "MATCH (n) RETURN"
    openTextDocument(textDocumentService, uri, text)

    val resultDiagnostic = getDiagnostics(textDocumentService, uri).head

    assertEquals(resultDiagnostic.getMessage, "mismatched input '<EOF>' expecting {DISTINCT, SP}")
    // The EOF token has no extent, so the range is zero-width at the end of the text.
    assertEquals(resultDiagnostic.getRange, new Range(new Position(0, 16), new Position(0, 16)))
  }

  test("diagnostic ranges count characters in UTF-16 code units") {
    val uri = "file:///tmp/test.txt"
    // The ghost emoji is one Unicode code point but two UTF-16 code units, and
    // RETURN is misspelled after it
    val text = "MATCH (n) WHERE n.name = \"👻\" RETRN n"
    openTextDocument(textDocumentService, uri, text)

    val resultDiagnostic = getDiagnostics(textDocumentService, uri).head

    // ANTLR's CodePointCharStream reports RETRN at code-point column 29; LSP
    // positions are UTF-16 code units (the emoji counts as two), so the range
    // covering the five-character RETRN token is (0,30)-(0,35).
    assertEquals(resultDiagnostic.getRange, new Range(new Position(0, 30), new Position(0, 35)))
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
        .flatMap((semanticTokens: List[Int]) => SemanticType.fromInt(semanticTokens(3)))

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

  test("semantic tokens report UTF-16 columns and lengths across a surrogate pair") {
    val uri = "file:///tmp/test.txt"
    // 😀 (U+1F600) is one Unicode code point but two UTF-16 code units. ANTLR reports columns and
    // lengths in code points; LSP semantic tokens are UTF-16, so the string-literal token's length
    // and every token to its right must account for the surrogate pair.
    val text = "MATCH (n) WHERE n.name = \"😀\" RETURN n"
    openTextDocument(textDocumentService, uri, text)

    val tokens = decodeSemanticTokens(getSemanticTokens(textDocumentService, uri))
    // Slicing in UTF-16 units (Java String): a code-point length drops the closing quote, and a
    // token left at a code-point column slices the wrong text.
    def slice(token: AbsToken): String = text.substring(token.col, math.min(token.col + token.length, text.length))

    val stringLiteral = tokens
      .find(_.semanticType == SemanticType.StringLiteral)
      .getOrElse(fail("expected a string-literal semantic token"))
    assertEquals(slice(stringLiteral), "\"😀\"")

    assert(
      tokens.exists(token => slice(token) == "RETURN"),
      s"expected a token after the astral character to slice to RETURN in UTF-16; got ${tokens.map(slice)}",
    )
  }

  test("each semantic token stays within its own line") {
    val uri = "file:///tmp/test.txt"
    val text = "MATCH (n)\nWHERE n.name = \"Bob\"\nRETURN n"
    openTextDocument(textDocumentService, uri, text)

    val lines = text.split("\n", -1)
    decodeSemanticTokens(getSemanticTokens(textDocumentService, uri)).foreach { token =>
      assert(token.col >= 0 && token.length >= 0, s"non-negative position: $token")
      assert(
        token.col + token.length <= lines(token.line).length,
        s"token $token must not extend past line ${token.line} (UTF-16 length ${lines(token.line).length})",
      )
    }
  }

  test("semantic tokens are delta-encoded in ascending (line, column) order") {
    val uri = "file:///tmp/test.txt"
    // For a left-directed relationship the visitor emits the variable token before the `<`
    // arrow token, so their source order is not column-sorted. LSP 3.17 requires semantic
    // tokens in ascending (line, startChar) order; an unsorted stream produces a negative
    // deltaStartChar on the same line, which garbles every downstream token.
    val text = "MATCH (a)<-[r]-(b) RETURN r"
    openTextDocument(textDocumentService, uri, text)

    val data = getSemanticTokens(textDocumentService, uri)

    // The raw delta stream must never step backwards within a line: a token that stays on the
    // same line (deltaLine == 0) must have a non-negative deltaStartChar.
    data.grouped(5).foreach {
      case List(deltaLine, deltaChar, _, _, _) =>
        if (deltaLine == 0)
          assert(deltaChar >= 0, s"same-line deltaStartChar must be non-negative; got $deltaChar in $data")
      case _ => ()
    }

    // Decoded to absolute positions, the tokens are monotonically non-decreasing in (line, col).
    val tokens = decodeSemanticTokens(data)
    tokens.sliding(2).foreach {
      case List(prev, next) =>
        val ascending = prev.line < next.line || (prev.line == next.line && prev.col <= next.col)
        assert(ascending, s"tokens must be ascending in (line, col); $prev preceded $next")
      case _ => ()
    }
  }

  test("completion resolves the word-prefix edit range on a later line of a CRLF document") {
    val uri = "file:///tmp/test.txt"
    // The \n-only line model must still place the caret on the right line of a \r\n buffer.
    openTextDocument(textDocumentService, uri, "MATCH (n)\r\nRETUR")

    val items = getCompletionItems(textDocumentService, uri, 1, 5)
    assert(items.nonEmpty, "expected completion items")
    items.foreach { item =>
      val edit = item.getTextEdit.getLeft
      assertEquals(
        edit.getRange,
        new Range(new Position(1, 0), new Position(1, 5)),
        s"item ${item.getLabel} edit range should cover RETUR on line 1",
      )
    }
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

  /** A semantic token at an absolute position, in the units a client sees: 0-based line and
    * 0-based UTF-16 start column, with the length in UTF-16 code units.
    */
  private case class AbsToken(line: Int, col: Int, length: Int, semanticType: SemanticType)

  /** Decodes the flat LSP semantic-tokens `data` (5 ints per token: deltaLine, deltaStartChar,
    * length, tokenType, modifiers) into absolute [[AbsToken]]s the way a client does, so a test
    * can assert real columns and lengths rather than deltas.
    */
  private def decodeSemanticTokens(data: List[Int]): List[AbsToken] = {
    var line = 0
    var col = 0
    data.grouped(5).toList.flatMap {
      case List(deltaLine, deltaChar, length, typeId, _) =>
        if (deltaLine > 0) { line += deltaLine; col = deltaChar }
        else col += deltaChar
        SemanticType.fromInt(typeId).map(AbsToken(line, col, length, _))
      case _ => None
    }
  }

  /** Applies a single-line LSP text edit to a buffer, the way a client commits a completion, so
    * a test can assert the buffer an accepted completion produces.
    */
  private def applyEdit(text: String, edit: org.eclipse.lsp4j.TextEdit): String = {
    val lines = text.split("\n", -1)
    val range = edit.getRange
    val lineIndex = range.getStart.getLine
    val line = lines(lineIndex)
    val edited =
      line.substring(0, range.getStart.getCharacter) + edit.getNewText + line.substring(range.getEnd.getCharacter)
    lines.updated(lineIndex, edited).mkString("\n")
  }
}
