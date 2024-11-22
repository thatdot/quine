package com.thatdot.quine.app.routes.websocketquinepattern

import io.circe.generic.semiauto._
import io.circe.{Decoder, DecodingFailure, HCursor}

sealed trait JsonRpcResult

case class CompletionItem(insertText: String)
case class CompletionList(items: List[CompletionItem]) extends JsonRpcResult

case class CompletionProvider(triggerCharacters: List[String])
case class Legend(tokenTypes: List[String], tokenModifiers: List[String])
case class SemanticTokensProvider(legend: Legend, range: Boolean, full: Boolean)
case class DiagnosticProvider(interFileDependencies: Boolean, workspaceDiagnostics: Boolean)
case class ServerCapabilities(
  textDocumentSync: 1,
  completionProvider: CompletionProvider,
  semanticTokensProvider: SemanticTokensProvider,
  diagnosticProvider: DiagnosticProvider,
)
case class InitializeResult(capabilities: ServerCapabilities) extends JsonRpcResult

case class DiagnosticItem(message: String)
case class DiagnosticResult(kind: String, items: List[DiagnosticItem]) extends JsonRpcResult

case class JsonRpcResponse(jsonrpc: "2.0", id: Int, result: JsonRpcResult)

object JsonRpcResponse {
  implicit val completionItemDecoder: Decoder[CompletionItem] = deriveDecoder

  implicit val completionProviderDecoder: Decoder[CompletionProvider] = deriveDecoder
  implicit val legendDecoder: Decoder[Legend] = deriveDecoder
  implicit val semanticTokensProviderDecoder: Decoder[SemanticTokensProvider] = deriveDecoder
  implicit val diagnosticProviderDecoder: Decoder[DiagnosticProvider] = deriveDecoder
  implicit val serverCapabilitiesResultItemDecoder: Decoder[ServerCapabilities] = deriveDecoder
  implicit val initializeResultItemDecoder: Decoder[InitializeResult] = deriveDecoder

  implicit val diagnosticItemDecoder: Decoder[DiagnosticItem] = deriveDecoder
  implicit val diagnosticResultDecoder: Decoder[DiagnosticResult] = deriveDecoder

  implicit val completionListDecoder: Decoder[CompletionList] =
    Decoder.decodeList[CompletionItem].map(CompletionList)

  implicit val jsonRpcResultDecoder: Decoder[JsonRpcResult] = new Decoder[JsonRpcResult] {
    def apply(c: HCursor): Decoder.Result[JsonRpcResult] =
      if (c.downField("capabilities").succeeded) {
        c.as[InitializeResult]
      } else if (c.downField("kind").succeeded) {
        c.as[DiagnosticResult]
      } else if (c.focus.exists(_.isArray)) {
        c.as[CompletionList]
      } else {
        Left(DecodingFailure("Unknown JsonRpcResult type", c.history))
      }
  }

  implicit val jsonRpcResponseDecoder: Decoder[JsonRpcResponse] = deriveDecoder
}
