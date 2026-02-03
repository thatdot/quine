package com.thatdot.quine.language.server

import java.util.Arrays

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
    textDocumentService.didChange(
      new DidChangeTextDocumentParams(
        new VersionedTextDocumentIdentifier(uri, null),
        Arrays.asList(new TextDocumentContentChangeEvent(text)),
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
