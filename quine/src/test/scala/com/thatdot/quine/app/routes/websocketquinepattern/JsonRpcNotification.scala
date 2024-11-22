package com.thatdot.quine.app.routes.websocketquinepattern

import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._

sealed trait JsonRpcNotificationParams

case class TextDocumentItem(uri: String, text: String, languageId: String)
case class DidOpenParams(textDocument: TextDocumentItem) extends JsonRpcNotificationParams
case class JsonRpcNotification(jsonrpc: "2.0", method: String, params: JsonRpcNotificationParams)

object JsonRpcNotification {
  implicit val textDocumentItemEncoder: Encoder[TextDocumentItem] = deriveEncoder
  implicit val didOpenParamsEncoder: Encoder[DidOpenParams] = deriveEncoder
  implicit val jsonRpcNotificationEncoder: Encoder[JsonRpcNotification] = deriveEncoder

  implicit val jsonRpcNotificationParamsEncoder: Encoder[JsonRpcNotificationParams] = Encoder.instance {
    case didOpenParams: DidOpenParams => didOpenParams.asJson
  }
}
