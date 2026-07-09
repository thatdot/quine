package com.thatdot.quine.app.routes.websocketquinepattern

import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._

sealed trait JsonRpcRequestParams

case class DiagnosticParams(textDocument: TextDocumentIdentifier) extends JsonRpcRequestParams

case class Position(line: Int, character: Int)
case class TextDocumentIdentifier(uri: String)
case class CompletionParams(textDocument: TextDocumentIdentifier, position: Position) extends JsonRpcRequestParams

case class HoverParams(textDocument: TextDocumentIdentifier, position: Position) extends JsonRpcRequestParams

case class QueryKindParams(textDocument: TextDocumentIdentifier) extends JsonRpcRequestParams

case class SemanticTokensParams(textDocument: TextDocumentIdentifier) extends JsonRpcRequestParams

case class Capabilities()
case class InitializeParams(capabilities: Capabilities) extends JsonRpcRequestParams

case class JsonRpcRequest(jsonrpc: "2.0", id: Int, method: String, params: JsonRpcRequestParams)

object JsonRpcRequest {
  implicit val positionEncoder: Encoder[Position] = deriveEncoder
  implicit val textDocumentIdentifieEncoder: Encoder[TextDocumentIdentifier] = deriveEncoder
  implicit val capabilitiesEncoder: Encoder[Capabilities] = deriveEncoder

  implicit val jsonRpcParamsEncoder: Encoder[JsonRpcRequestParams] = Encoder.instance {
    case completionParams: CompletionParams => completionParams.asJson
    case initializeParams: InitializeParams => initializeParams.asJson
    case diagnosticParams: DiagnosticParams => diagnosticParams.asJson
    case hoverParams: HoverParams => hoverParams.asJson
    case queryKindParams: QueryKindParams => queryKindParams.asJson
    case semanticTokensParams: SemanticTokensParams => semanticTokensParams.asJson
  }

  implicit val diagnosticParamsEncoder: Encoder[DiagnosticParams] = deriveEncoder
  implicit val hoverParamsEncoder: Encoder[HoverParams] = deriveEncoder
  implicit val queryKindParamsEncoder: Encoder[QueryKindParams] = deriveEncoder
  implicit val semanticTokensParamsEncoder: Encoder[SemanticTokensParams] = deriveEncoder
  implicit val completionParamsEncoder: Encoder[CompletionParams] = deriveEncoder
  implicit val initializeParamsEncoder: Encoder[InitializeParams] = deriveEncoder

  implicit val jsonRpcRequestEncoder: Encoder[JsonRpcRequest] = deriveEncoder
}
