package com.thatdot.quine.app.routes.websocketquinepattern

import io.circe.generic.semiauto._
import io.circe.{Decoder, DecodingFailure, HCursor}

sealed trait JsonRpcResult

case class TextEdit(range: Range, newText: String)

/** A completion item as the server puts it on the wire. The textEdit is required: without it
  * Monaco anchors the item to an empty range at the caret, which breaks prefix filtering and
  * makes accepting an item insert beside the typed prefix instead of replacing it.
  */
case class CompletionItem(insertText: String, textEdit: TextEdit)
case class CompletionList(items: List[CompletionItem]) extends JsonRpcResult

case class CompletionProvider(triggerCharacters: List[String])
case class Legend(tokenTypes: List[String], tokenModifiers: List[String])
case class SemanticTokensProvider(legend: Legend, full: Boolean)
case class DiagnosticProvider(interFileDependencies: Boolean, workspaceDiagnostics: Boolean)
case class ServerCapabilities(
  textDocumentSync: 2,
  hoverProvider: Boolean,
  completionProvider: CompletionProvider,
  semanticTokensProvider: SemanticTokensProvider,
  diagnosticProvider: DiagnosticProvider,
)
case class InitializeResult(capabilities: ServerCapabilities) extends JsonRpcResult

case class DiagnosticItem(message: String, severity: Int)
case class DiagnosticResult(kind: String, items: List[DiagnosticItem]) extends JsonRpcResult

case class QueryKindResult(kind: String) extends JsonRpcResult

/** A `textDocument/hover` result: Markdown contents plus the range the hover applies to. */
case class MarkupContent(kind: String, value: String)
case class HoverResult(contents: MarkupContent, range: Range) extends JsonRpcResult

/** A semantic-tokens result: the delta-encoded token data array of LSP 3.17's
  * `textDocument/semanticTokens/full`.
  */
case class SemanticTokensResult(data: List[Int]) extends JsonRpcResult

case class JsonRpcResponse(jsonrpc: "2.0", id: Int, result: JsonRpcResult)

/** A JSON-RPC error object (JSON-RPC 2.0 §5.1). */
case class JsonRpcError(code: Int, message: String)

/** A JSON-RPC error response: carries `id` and `error` instead of `result`. */
case class JsonRpcErrorResponse(jsonrpc: "2.0", id: Int, error: JsonRpcError)

object JsonRpcResponse {
  implicit val positionDecoder: Decoder[Position] = deriveDecoder
  implicit val rangeDecoder: Decoder[Range] = deriveDecoder
  implicit val textEditDecoder: Decoder[TextEdit] = deriveDecoder
  implicit val completionItemDecoder: Decoder[CompletionItem] = deriveDecoder

  implicit val completionProviderDecoder: Decoder[CompletionProvider] = deriveDecoder
  implicit val legendDecoder: Decoder[Legend] = deriveDecoder
  implicit val semanticTokensProviderDecoder: Decoder[SemanticTokensProvider] = deriveDecoder
  implicit val diagnosticProviderDecoder: Decoder[DiagnosticProvider] = deriveDecoder
  implicit val serverCapabilitiesResultItemDecoder: Decoder[ServerCapabilities] = deriveDecoder
  implicit val initializeResultItemDecoder: Decoder[InitializeResult] = deriveDecoder

  implicit val diagnosticItemDecoder: Decoder[DiagnosticItem] = deriveDecoder
  implicit val diagnosticResultDecoder: Decoder[DiagnosticResult] = deriveDecoder
  implicit val queryKindResultDecoder: Decoder[QueryKindResult] = deriveDecoder
  implicit val markupContentDecoder: Decoder[MarkupContent] = deriveDecoder
  implicit val hoverResultDecoder: Decoder[HoverResult] = deriveDecoder
  implicit val semanticTokensResultDecoder: Decoder[SemanticTokensResult] = deriveDecoder
  implicit val jsonRpcErrorDecoder: Decoder[JsonRpcError] = deriveDecoder
  implicit val jsonRpcErrorResponseDecoder: Decoder[JsonRpcErrorResponse] = deriveDecoder

  implicit val completionListDecoder: Decoder[CompletionList] =
    Decoder.decodeList[CompletionItem].map(CompletionList)

  implicit val jsonRpcResultDecoder: Decoder[JsonRpcResult] = new Decoder[JsonRpcResult] {
    def apply(c: HCursor): Decoder.Result[JsonRpcResult] =
      if (c.downField("capabilities").succeeded) {
        c.as[InitializeResult]
      } else if (c.downField("items").succeeded) {
        // Diagnostic reports carry both "kind" (the report kind, e.g. "full") and "items".
        c.as[DiagnosticResult]
      } else if (c.downField("contents").succeeded) {
        // A hover result carries "contents" (and the range the hover applies to).
        c.as[HoverResult]
      } else if (c.downField("kind").succeeded) {
        // A quine/queryKind verdict carries only "kind".
        c.as[QueryKindResult]
      } else if (c.downField("data").succeeded) {
        // A semantic-tokens result carries the delta-encoded "data" array.
        c.as[SemanticTokensResult]
      } else if (c.focus.exists(_.isArray)) {
        c.as[CompletionList]
      } else {
        Left(DecodingFailure("Unknown JsonRpcResult type", c.history))
      }
  }

  implicit val jsonRpcResponseDecoder: Decoder[JsonRpcResponse] = deriveDecoder
}
