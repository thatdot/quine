package com.thatdot.quine.app.routes.websocketquinepattern

import scala.concurrent.duration._

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ws.TextMessage.Strict
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString

import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures.{PatienceConfig, whenReady}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import sttp.ws.WebSocketFrame

import JsonRpcResponse._

class WebSocketQuinePatternServerTest extends AnyFunSuite with BeforeAndAfterAll {
  implicit var system: ActorSystem = _
  implicit var materializer: Materializer = _

  implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(500, Millis))

  def createMessageFlow(): Flow[Message, Message, _] = (new WebSocketQuinePatternServer(system)).messagesFlow

  // A real WebSocket client holds the inbound side open for the whole session; the flow stops the
  // LSPActor when the inbound stream completes. So a test expecting responses AFTER its requests
  // must keep the inbound side open — these append `Source.never` to the request source and let the
  // outbound `.take(n)` end the graph. (Teardown itself is covered by the tear-down test below.)

  override def beforeAll(): Unit = {
    system = ActorSystem("TestActorSystem")
    materializer = Materializer(system)
    super.beforeAll()
  }

  override def afterAll(): Unit =
    whenReady(system.terminate()) { _ =>
      super.afterAll()
    }

  /** Returns the raw JSON payload of a server-sent WebSocket frame.
    *
    * Server-to-client frames are raw JSON with no Content-Length header prefix; WebSocket
    * framing provides message boundaries. This method is a pass-through that makes call sites
    * self-documenting about the expected frame format.
    *
    * @param raw_message_string The raw WebSocket frame payload (a JSON-RPC object)
    * @return The same string, ready for JSON decoding
    */
  def jrpc_content_part(raw_message_string: String): String = raw_message_string

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
            textDocumentSync = 2,
            hoverProvider = true,
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
                  "EdgeLabel",
                ),
                tokenModifiers = List(),
              ),
              full = true,
            ),
          ),
        ),
      )

    val initializeMessageResultFuture = Source
      .single(TextMessage.Strict(initializeRequest))
      .concat(Source.never)
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

  test("completing the inbound stream tears the connection down instead of leaking the actor") {
    // The flow couples the two halves and stops the actor when the inbound stream completes. The
    // observable contract: once the client's inbound side finishes, the outbound side finishes too.
    // Draining the outbound side without a `.take(n)` bound proves teardown fires — otherwise the
    // outbound Source.actorRef would stay open forever and this future would never complete.
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val terminationFuture: scala.concurrent.Future[Done] = Source
      .single(TextMessage.Strict(initializeRequest))
      .via(messageFlow)
      .runWith(Sink.ignore)

    whenReady(terminationFuture) { done =>
      assert(done == Done)
    }
  }

  test("a JSON-RPC request delivered as a binary frame is Content-Length framed and answered") {
    // A binary frame carries one complete JSON-RPC message just like a text frame; it must be
    // framed into the language server's stream identically. Unframed bytes desync lsp4j's
    // StreamMessageProducer, so the request would never parse and no response would arrive.
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val resultFuture = Source
      .single(BinaryMessage.Strict(ByteString(initializeRequest, "UTF-8")))
      .concat(Source.never)
      .via(messageFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(resultFuture) { msgSeq =>
      msgSeq.headOption match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(response) => assert(response.id == 1)
            case Left(error) => fail(s"binary-framed initialize response did not parse: ${error}")
          }
        case _ => fail("No response received for the binary-framed request")
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
            textDocumentSync = 2,
            hoverProvider = true,
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
                  "EdgeLabel",
                ),
                tokenModifiers = List(),
              ),
              full = true,
            ),
          ),
        ),
      )

    val initializeMessageResultFuture = Source
      .single(initializeRequestStreamed)
      .concat(Source.never)
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

  test("framesFlow answers an Initialize text frame with a raw-JSON text frame") {
    val framesFlow = (new WebSocketQuinePatternServer(system)).framesFlow

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val initializeFrameResultFuture = Source
      .single(WebSocketFrame.text(initializeRequest))
      .concat(Source.never)
      .via(framesFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(initializeFrameResultFuture) { frameSeq =>
      frameSeq.headOption match {
        case Some(WebSocketFrame.Text(payload, finalFragment, _)) =>
          assert(finalFragment)
          // The frame payload is raw JSON-RPC, identical to the v1 route's wire format:
          // no Content-Length header prefix.
          assert(!payload.startsWith("Content-Length"))
          decode[JsonRpcResponse](jrpc_content_part(payload)) match {
            case Right(JsonRpcResponse(_, id, result)) =>
              assert(id == 1)
              assert(result.isInstanceOf[InitializeResult])
            case Left(error) => fail(s"Initialize Response did not parse: ${error}")
          }
        case other => fail(s"Expected a text frame, got: $other")
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

    // The caret at character 15 touches the RETURN keyword of "MATCH (n) RETURN n", so the
    // grammar-driven candidates are every keyword the grammar allows where that word starts
    // (after the MATCH clause), sorted alphabetically. Every item replaces the word prefix
    // before the caret — RETUR, columns 10-15 — so the client filters against the typed
    // prefix and accepting an item replaces it instead of inserting beside it.
    val expectedKeywords = List(
      "CALL",
      "CREATE",
      "DELETE",
      "DETACH",
      "FOREACH",
      "MATCH",
      "MERGE",
      "OPTIONAL",
      "REMOVE",
      "RETURN",
      "SET",
      "UNWIND",
      "WHERE",
      "WITH",
    )
    val prefixRange = Range(start = Position(line = 0, character = 10), end = Position(line = 0, character = 15))
    // v1 completion additionally offers every function and procedure name everywhere
    // (ContextAwareLanguageService makes no attempt to gate by grammar position), so rather
    // than asserting the exact set we assert that the clause keywords valid here are all
    // offered — each replacing the RETUR prefix.
    val expectedKeywordItems =
      expectedKeywords.map(keyword =>
        CompletionItem(insertText = keyword, textEdit = TextEdit(range = prefixRange, newText = keyword)),
      )

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(completionRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, CompletionList(actualItems))) =>
              assert(expectedKeywordItems.forall(actualItems.contains))
            case Right(other) => fail(s"Expected a CompletionList response, got: ${other}")
            case Left(error) => fail(s"Completion Response did not parse: ${error}")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("an incremental didChange that splits a valid query across lines keeps it diagnostic-clean") {
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

    // Monaco's LSP client reports edits as ranged content changes regardless of the
    // advertised sync kind: replacing the space after "(n)" with a newline turns the
    // query multi-line and must leave it diagnostic-clean.
    val changeNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didChange",
        params = DidChangeParams(
          textDocument = VersionedTextDocumentIdentifier(uri, version = 2),
          contentChanges = List(
            ContentChange(
              range = Range(start = Position(line = 0, character = 9), end = Position(line = 0, character = 10)),
              rangeLength = 1,
              text = "\n",
            ),
          ),
        ),
      ).asJson.noSpaces

    val diagnosticsRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "textDocument/diagnostic",
        params = DiagnosticParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(changeNotification),
          TextMessage.Strict(diagnosticsRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, DiagnosticResult(_, actualDiagnosticItems))) =>
              assert(actualDiagnosticItems == List.empty)
            case _ => fail("Couldn't parse message returned from server")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("an empty text document produces no diagnostics") {
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
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = "", languageId = "cypher")),
      ).asJson.noSpaces

    val diagnosticsRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "textDocument/diagnostic",
        params = DiagnosticParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(diagnosticsRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, DiagnosticResult(_, actualDiagnosticItems))) =>
              assert(actualDiagnosticItems == List.empty)
            case _ => fail("Couldn't parse message returned from server")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("classify open documents with the quine/queryKind custom request") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    // Five documents covering each verdict: a provably node-returning query, the Explorer's
    // standalone procedure-call sample (node-returning by its registry signature), a property
    // projection (runnable but not provably node-returning), a multipart write query with no
    // RETURN (side-effectful), and an empty buffer (no verdict).
    val documents = List(
      ("file:///tmp/node.cypher", "MATCH (n) RETURN n", "node"),
      ("file:///tmp/call.cypher", "CALL recentNodes(10)", "node"),
      ("file:///tmp/table.cypher", "MATCH (n) RETURN n.prop", "table"),
      (
        "file:///tmp/sideeffects.cypher",
        """WITH "MattDot" AS name MATCH (n) WHERE id(n) = idFrom("Person", name) SET n:Person, n.name=name""",
        "sideEffects",
      ),
      ("file:///tmp/empty.cypher", "", "unknown"),
    )

    val openNotifications = documents.map { case (uri, text, _) =>
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = text, languageId = "cypher")),
      ).asJson.noSpaces
    }

    val queryKindRequests = documents.zipWithIndex.map { case ((uri, _, _), index) =>
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = index + 2,
        method = "quine/queryKind",
        params = QueryKindParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces
    }

    val messagesFuture =
      Source(
        (initializeRequest :: openNotifications ::: queryKindRequests).map(TextMessage.Strict(_)),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(6)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      documents.zipWithIndex.foreach { case ((uri, _, expectedKind), index) =>
        msgSeq.lift(index + 1) match {
          case Some(Strict(full_message)) =>
            decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
              case Right(actualResponse) =>
                assert(
                  actualResponse == JsonRpcResponse("2.0", index + 2, QueryKindResult(expectedKind)),
                  s"Unexpected verdict for $uri: $actualResponse",
                )
              case Left(error) => fail(s"queryKind response for $uri did not parse: ${error}")
            }
          case _ => fail(s"No queryKind response received for $uri")
        }
      }
    }
  }

  test("hover on an idFrom invocation answers markdown containing the quine.io docs link") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val uri = "file:///tmp/hover.cypher"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(
          textDocument = TextDocumentItem(
            uri,
            text = "MATCH (n) WHERE id(n) = idFrom(\"Bob\") RETURN n",
            languageId = "cypher",
          ),
        ),
      ).asJson.noSpaces

    // The position is inside idFrom (columns 24-29); per LSP 3.17 the response carries
    // MarkupContent plus the range of the hovered function name. It is a request, so the
    // answer travels back as a JSON-RPC response with a "result" member.
    val hoverRequest = JsonRpcRequest(
      jsonrpc = "2.0",
      id = 2,
      method = "textDocument/hover",
      params = HoverParams(
        textDocument = TextDocumentIdentifier(uri),
        position = Position(line = 0, character = 27),
      ),
    ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(hoverRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, id, HoverResult(contents, range))) =>
              assert(id == 2)
              assert(contents.kind == "markdown")
              assert(contents.value.contains("idFrom("))
              assert(contents.value.contains("https://quine.io/core-concepts/id-provider/#idfrom"))
              assert(
                range == Range(
                  start = Position(line = 0, character = 24),
                  end = Position(line = 0, character = 30),
                ),
              )
            case other => fail(s"hover response did not parse as a hover result: $other")
          }
        case _ => fail("No hover response received")
      }
    }
  }

  test("hover on a reify.time procedure call answers markdown containing the quine.io docs link") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val uri = "file:///tmp/hover-procedure.cypher"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(
          textDocument = TextDocumentItem(
            uri,
            text = "CALL reify.time(datetime(), [\"year\"]) YIELD node RETURN node",
            languageId = "cypher",
          ),
        ),
      ).asJson.noSpaces

    // The position is inside the `time` segment of reify.time (the dotted name spans columns
    // 5-14); per LSP 3.17 the response carries MarkupContent plus the range of the hovered
    // procedure name. It is a request, so the answer travels back as a JSON-RPC response with
    // a "result" member.
    val hoverRequest = JsonRpcRequest(
      jsonrpc = "2.0",
      id = 2,
      method = "textDocument/hover",
      params = HoverParams(
        textDocument = TextDocumentIdentifier(uri),
        position = Position(line = 0, character = 12),
      ),
    ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(hoverRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, id, HoverResult(contents, range))) =>
              assert(id == 2)
              assert(contents.kind == "markdown")
              assert(contents.value.contains("reify.time("))
              assert(contents.value.contains("https://quine.io/reference/cypher/reify-time/"))
              assert(
                range == Range(
                  start = Position(line = 0, character = 5),
                  end = Position(line = 0, character = 15),
                ),
              )
            case other => fail(s"hover response did not parse as a hover result: $other")
          }
        case _ => fail("No hover response received")
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
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, DiagnosticResult(_, actualDiagnosticItems))) =>
              // severity 1 is DiagnosticSeverity.Error (LSP 3.17): a parse error is a hard error
              val expectedDiagnosticItems =
                List(DiagnosticItem(message = "no viable alternative at input 'MATCH (n) RETUR'", severity = 1))
              assert(actualDiagnosticItems == expectedDiagnosticItems)
            case _ => fail("Couldn't parse message returned from server")
          }
        case _ => fail("No Messages received")
      }
    }
  }

  test("a JSON-RPC error response reaches the client") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    // A method the server does not implement: lsp4j answers it with a MethodNotFound error
    // response (JSON-RPC code -32601), which must be forwarded to the client so the request
    // settles instead of hanging forever.
    val unknownMethodRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "quine/doesNotExist",
        params = QueryKindParams(textDocument = TextDocumentIdentifier("file:///tmp/file.txt")),
      ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(unknownMethodRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcErrorResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcErrorResponse(_, id, error)) =>
              assert(id == 2)
              // -32601 is JSON-RPC 2.0's MethodNotFound
              assert(error.code == -32601)
            case Left(error) => fail(s"Error response did not parse: ${error}")
          }
        case _ => fail("No error response received")
      }
    }
  }

  test("semantic tokens on a multipart query answer with tokens and leave the session usable") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    // A multipart query (WITH ... MATCH ... SET): the worst-case construct for the semantic
    // visitors. Both requests below ride the same connection, so the quine/queryKind answer
    // proves the semantic-tokens request did not kill the server's message-listener thread.
    val uri = "file:///tmp/multipart.cypher"
    val multipartQuery =
      "WITH \"MattDot\" AS name MATCH (n) WHERE id(n) = idFrom(\"Person\", name) SET n:Person, n.name=name"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = multipartQuery, languageId = "cypher")),
      ).asJson.noSpaces

    val semanticTokensRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "textDocument/semanticTokens/full",
        params = SemanticTokensParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    val queryKindRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 3,
        method = "quine/queryKind",
        params = QueryKindParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          TextMessage.Strict(semanticTokensRequest),
          TextMessage.Strict(queryKindRequest),
        ),
      )
        .concat(Source.never)
        .via(messageFlow)
        .take(3)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, id, SemanticTokensResult(data))) =>
              assert(id == 2)
              // 16 tokens × 5 integers of LSP 3.17's delta encoding
              assert(data.length == 16 * 5)
            case other => fail(s"semanticTokens response did not parse as a token result: $other")
          }
        case _ => fail("No semanticTokens response received")
      }
      msgSeq.lift(2) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(actualResponse) =>
              assert(actualResponse == JsonRpcResponse("2.0", 3, QueryKindResult("sideEffects")))
            case Left(error) => fail(s"queryKind response did not parse: ${error}")
          }
        case _ => fail("No queryKind response received after the semanticTokens request")
      }
    }
  }

  test("a streamed didChange is applied before a later strict didChange that overtakes its fold") {
    // A streamed frame is folded asynchronously; a strict frame arriving mid-fold must NOT overtake
    // it, or the two didChanges apply out of order and the buffer ends wrong. Both changes replace
    // the same range — the final `n` of "MATCH (n) RETURN n" — so positions are order-independent
    // but the surviving edit is not: the streamed change makes it invalid (RETURN m), the strict
    // change restores it (RETURN n). Applied in order the buffer ends valid; reordered, invalid.
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 1,
        method = "initialize",
        params = InitializeParams(capabilities = Capabilities()),
      ).asJson.noSpaces

    val uri = "file:///tmp/ordering.cypher"
    val openNotification =
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didOpen",
        params = DidOpenParams(textDocument = TextDocumentItem(uri, text = "MATCH (n) RETURN n", languageId = "cypher")),
      ).asJson.noSpaces

    // The final `n` sits at column 17; both edits replace the single-character range (0,17)-(0,18).
    val replaceFinalToken = (newText: String, version: Int) =>
      JsonRpcNotification(
        jsonrpc = "2.0",
        method = "textDocument/didChange",
        params = DidChangeParams(
          textDocument = VersionedTextDocumentIdentifier(uri, version = version),
          contentChanges = List(
            ContentChange(
              range = Range(start = Position(line = 0, character = 17), end = Position(line = 0, character = 18)),
              rangeLength = 1,
              text = newText,
            ),
          ),
        ),
      ).asJson.noSpaces

    // Streamed change to the invalid `m`, its fold deliberately delayed so the strict change below
    // is delivered to the actor while the fold is still in flight.
    val streamedChangeToInvalid =
      TextMessage.Streamed(Source.single(replaceFinalToken("m", 2)).initialDelay(150.millis))
    // Strict change restoring the valid `n`; it must be applied AFTER the streamed change.
    val strictChangeToValid = TextMessage.Strict(replaceFinalToken("n", 3))

    val diagnosticsRequest =
      JsonRpcRequest(
        jsonrpc = "2.0",
        id = 2,
        method = "textDocument/diagnostic",
        params = DiagnosticParams(textDocument = TextDocumentIdentifier(uri)),
      ).asJson.noSpaces

    // The diagnostics request is delayed well past the streamed fold so it observes the settled
    // buffer (both didChanges applied), not an intermediate state.
    val messagesFuture =
      Source(
        Seq(
          TextMessage.Strict(initializeRequest),
          TextMessage.Strict(openNotification),
          streamedChangeToInvalid,
          strictChangeToValid,
        ),
      )
        .concat(Source.single(TextMessage.Strict(diagnosticsRequest)).initialDelay(500.millis))
        .concat(Source.never)
        .via(messageFlow)
        .take(2)
        .runWith(Sink.seq)

    whenReady(messagesFuture) { msgSeq =>
      msgSeq.lift(1) match {
        case Some(Strict(full_message)) =>
          decode[JsonRpcResponse](jrpc_content_part(full_message)) match {
            case Right(JsonRpcResponse(_, _, DiagnosticResult(_, actualDiagnosticItems))) =>
              assert(
                actualDiagnosticItems == List.empty,
                s"expected the strict didChange to win (valid buffer); got diagnostics $actualDiagnosticItems",
              )
            case _ => fail("Couldn't parse message returned from server")
          }
        case _ => fail("No diagnostics response received")
      }
    }
  }
}
