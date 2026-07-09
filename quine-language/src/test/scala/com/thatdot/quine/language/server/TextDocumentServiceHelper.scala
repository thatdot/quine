package com.thatdot.quine.language.server

import scala.jdk.CollectionConverters._

import org.eclipse.lsp4j.services.TextDocumentService
import org.eclipse.lsp4j.{
  CompletionItem,
  CompletionParams,
  Diagnostic,
  DidChangeTextDocumentParams,
  DidCloseTextDocumentParams,
  DidOpenTextDocumentParams,
  DocumentDiagnosticParams,
  Hover,
  HoverParams,
  Position,
  SemanticTokensParams,
  TextDocumentContentChangeEvent,
  TextDocumentIdentifier,
  TextDocumentItem,
  VersionedTextDocumentIdentifier,
}

object TextDocumentServiceHelper {
  def openTextDocument(
    textDocumentService: TextDocumentService,
    uri: String,
    text: String,
  ): Unit = {
    val languageId = "quineCypher"
    textDocumentService.didOpen(
      new DidOpenTextDocumentParams(
        new TextDocumentItem(
          uri,
          languageId,
          1,
          text,
        ),
      ),
    )
  }

  def changeTextDocument(
    textDocumentService: TextDocumentService,
    uri: String,
    text: String,
  ): Unit =
    changeTextDocument(textDocumentService, uri, List(new TextDocumentContentChangeEvent(text)))

  def changeTextDocument(
    textDocumentService: TextDocumentService,
    uri: String,
    contentChanges: List[TextDocumentContentChangeEvent],
  ): Unit =
    textDocumentService.didChange(
      new DidChangeTextDocumentParams(
        new VersionedTextDocumentIdentifier(uri, null),
        contentChanges.asJava,
      ),
    )

  def closeTextDocument(
    textDocumentService: TextDocumentService,
    uri: String,
  ): Unit =
    textDocumentService.didClose(
      new DidCloseTextDocumentParams(
        new TextDocumentIdentifier(uri),
      ),
    )

  def getCompletionItems(
    textDocumentService: TextDocumentService,
    uri: String,
    line: Int,
    character: Int,
  ): List[CompletionItem] =
    textDocumentService
      .completion(
        new CompletionParams(
          new TextDocumentIdentifier(uri),
          new Position(line, character),
        ),
      )
      .get()
      .getLeft()
      .asScala
      .toList

  def getDiagnostics(
    textDocumentService: TextDocumentService,
    uri: String,
  ): List[Diagnostic] =
    textDocumentService
      .diagnostic(
        new DocumentDiagnosticParams(
          new TextDocumentIdentifier(uri),
        ),
      )
      .get()
      .getLeft()
      .getItems()
      .asScala
      .toList

  /** Requests hover information at a position; null means the server has no hover there
    * (lsp4j's representation of LSP 3.17's null result).
    */
  def getHover(
    textDocumentService: TextDocumentService,
    uri: String,
    line: Int,
    character: Int,
  ): Hover =
    textDocumentService
      .hover(
        new HoverParams(
          new TextDocumentIdentifier(uri),
          new Position(line, character),
        ),
      )
      .get()

  def getSemanticTokens(
    textDocumentService: TextDocumentService,
    uri: String,
  ): List[Int] =
    textDocumentService
      .semanticTokensFull(
        new SemanticTokensParams(
          new TextDocumentIdentifier(uri),
        ),
      )
      .get()
      .getData()
      .asScala
      .toList
      .map(_.toInt)
}
