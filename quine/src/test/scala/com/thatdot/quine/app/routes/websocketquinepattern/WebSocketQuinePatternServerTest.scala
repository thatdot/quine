package com.thatdot.quine.app.routes.websocketquinepattern

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ws.TextMessage.Strict
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures.{PatienceConfig, whenReady}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import JsonRpcResponse._

class WebSocketQuinePatternServerTest extends AnyFunSuite with BeforeAndAfterAll {
  implicit var system: ActorSystem = _
  implicit var materializer: Materializer = _

  implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(500, Millis))

  def createMessageFlow(): Flow[Message, Message, _] = (new WebSocketQuinePatternServer(system)).messagesFlow

  override def beforeAll(): Unit = {
    system = ActorSystem("TestActorSystem")
    materializer = Materializer(system)
    super.beforeAll()
  }

  override def afterAll(): Unit =
    whenReady(system.terminate()) { _ =>
      super.afterAll()
    }

  /** Returns the string content part of a JRPC message
    *
    * @param jrpc_full_message_string The full JRPC message, with Content-Length header and Content Part
    * @return The Content Part of the JRPC message
    */
  def jrpc_content_part(jrpc_full_message_string: String): String =
    jrpc_full_message_string.split("\r\n\r\n", 2) match {
      case Array(_, json) => json
      case _ => fail("Content-Length header not found in the message")
    }

  test("ensure Initialize message is handled correctly") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val expectedResponse =
      JsonRpcResponse(
        jsonrpc = "2.0",
        id = 1,
        result = InitializeResult(capabilities =
          ServerCapabilities(
            textDocumentSync = 1,
            completionProvider = CompletionProvider(triggerCharacters = List(".", " ")),
            diagnosticProvider = DiagnosticProvider(interFileDependencies = false, workspaceDiagnostics = false),
            semanticTokensProvider = SemanticTokensProvider(
              legend = Legend(
                tokenTypes = List(
                  "MatchKeyword",
                  "ReturnKeyword",
                  "AsKeyword",
                  "WhereKeyword",
                  "CreateKeyword",
                  "AndKeyword",
                  "PatternVariable",
                  "AssignmentOperator",
                  "AdditionOperator",
                  "NodeLabel",
                  "NodeVariable",
                  "Variable",
                  "Edge",
                  "FunctionApplication",
                  "Parameter",
                  "StringLiteral",
                  "NullLiteral",
                  "BooleanLiteral",
                  "IntLiteral",
                  "DoubleLiteral",
                  "Property",
                ),
                tokenModifiers = List(),
              ),
              range = true,
              full = true,
            ),
          ),
        ),
      )

    val initializeMessageResultFuture = Source
      .single(TextMessage.Strict(initializeRequest))
      .via(messageFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.headOption match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(actualResponse) => assert(actualResponse == expectedResponse)
            case Left(error) => fail(s"Initialize Response did not parse: ${error}")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("ensure streamed Initialize message is handled correctly") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequestParts = List(
      "{",
      "\"jsonrpc\": \"2.0\",",
      "\"id\": 1,",
      "\"method\": \"initialize\",",
      "\"params\": {",
      "\"capabilities\": {}",
      "}",
      "}",
    )

    val initializeRequestStreamed = TextMessage.Streamed(
      Source(initializeRequestParts),
    )

    val expectedMessage =
      JsonRpcResponse(
        jsonrpc = "2.0",
        id = 1,
        result = InitializeResult(capabilities =
          ServerCapabilities(
            textDocumentSync = 1,
            completionProvider = CompletionProvider(triggerCharacters = List(".", " ")),
            diagnosticProvider = DiagnosticProvider(interFileDependencies = false, workspaceDiagnostics = false),
            semanticTokensProvider = SemanticTokensProvider(
              legend = Legend(
                tokenTypes = List(
                  "MatchKeyword",
                  "ReturnKeyword",
                  "AsKeyword",
                  "WhereKeyword",
                  "CreateKeyword",
                  "AndKeyword",
                  "PatternVariable",
                  "AssignmentOperator",
                  "AdditionOperator",
                  "NodeLabel",
                  "NodeVariable",
                  "Variable",
                  "Edge",
                  "FunctionApplication",
                  "Parameter",
                  "StringLiteral",
                  "NullLiteral",
                  "BooleanLiteral",
                  "IntLiteral",
                  "DoubleLiteral",
                  "Property",
                ),
                tokenModifiers = List(),
              ),
              range = true,
              full = true,
            ),
          ),
        ),
      )

    val initializeMessageResultFuture = Source
      .single(initializeRequestStreamed)
      .via(messageFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.headOption match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(actualMessage) => assert(actualMessage == expectedMessage)
            case Left(error) => fail(s"Initialize Response did not parse: ${error}")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("open a text document with some initial Cypher and request completion items on it") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val uri = "file:///tmp/file.txt"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = "MATCH (n) RETURN n", languageId = "cypher")),
      ).asJson.noSpaces

    val completionRequest = JsonRpcRequest(
      jsonrpc = "2.0",
      id = 2,
      method = "textDocument/completion",
      params = CompletionParams(
        textDocument = TextDocumentIdentifier(uri),
        position = Position(line = 0, character = 15),
      ),
    ).asJson.noSpaces

    val expectedCompletionMessage =
      JsonRpcResponse(
        jsonrpc = "2.0",
        id = 2,
        result = CompletionList(items = List(CompletionItem(insertText = "bar"), CompletionItem(insertText = "foo"))),
      )

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(completionRequest),
        ),
      )
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(actualCompletionMessage) => assert(actualCompletionMessage == expectedCompletionMessage)
            case Left(error) => fail(s"Initialize Response did not parse: ${error}")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("open a text document with some initial Cypher and request diagnostics on it") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val uri = "file:///tmp/file.txt"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = "MATCH (n) RETUR n", languageId = "cypher")),
      ).asJson.noSpaces

    val diagnosticsRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "textDocument/diagnostic",
        params = DiagnosticParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    val initializeMessageResultFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(diagnosticsRequest),
        ),
      )
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, DiagnosticResult(_, actualDiagnosticItems))) =>
              val expectedDiagnosticItems =
                List(DiagnosticItem(message = "no viable alternative at input 'MATCH (n) RETUR'"))
              assert(actualDiagnosticItems == expectedDiagnosticItems)
            case _ => fail("Couldn't parse message returned from server")
          }
        case _ => fail("No Messages received")
      }
    }
  }
}
