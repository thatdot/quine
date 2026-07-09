package com.thatdot.quine.app.routes.websocketquinepattern

import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._

sealed trait JsonRpcNotificationParams

case class TextDocumentItem(uri: String, text: String, languageId: String)
case class DidOpenParams(textDocument: TextDocumentItem) extends JsonRpcNotificationParams
case class VersionedTextDocumentIdentifier(uri: String, version: Int)
case class Range(start: Position, end: Position)

/** An incremental content change as Monaco's LSP client sends it: the text replaces the range. */
case class ContentChange(range: Range, rangeLength: Int, text: String)
case class DidChangeParams(textDocument: VersionedTextDocumentIdentifier, contentChanges: List[ContentChange])
    extends JsonRpcNotificationParams
case class JsonRpcNotification(jsonrpc: "2.0", method: String, params: JsonRpcNotificationParams)

object JsonRpcNotification {
  import JsonRpcRequest.positionEncoder

  implicit val textDocumentItemEncoder: Encoder[TextDocumentItem] = deriveEncoder
  implicit val didOpenParamsEncoder: Encoder[DidOpenParams] = deriveEncoder
  implicit val versionedTextDocumentIdentifierEncoder: Encoder[VersionedTextDocumentIdentifier] = deriveEncoder
  implicit val rangeEncoder: Encoder[Range] = deriveEncoder
  implicit val contentChangeEncoder: Encoder[ContentChange] = deriveEncoder
  implicit val didChangeParamsEncoder: Encoder[DidChangeParams] = deriveEncoder
  implicit val jsonRpcNotificationEncoder: Encoder[JsonRpcNotification] = deriveEncoder

  implicit val jsonRpcNotificationParamsEncoder: Encoder[JsonRpcNotificationParams] = Encoder.instance {
    case didOpenParams: DidOpenParams => didOpenParams.asJson
    case didChangeParams: DidChangeParams => didChangeParams.asJson
  }
}
