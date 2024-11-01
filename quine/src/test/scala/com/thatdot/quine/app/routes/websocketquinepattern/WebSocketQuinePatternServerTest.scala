package com.thatdot.quine.app.routes.websocketquinepattern

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ws.TextMessage.Strict
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import io.circe.parser.parse
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures.{PatienceConfig, whenReady}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

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

  test("ensure Initialize message is handled correctly") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 1,
      |  "method": "initialize",
      |  "params": {
      |    "capabilities": {}
      |  }
      |}""".stripMargin

    val initializeResponse =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 1,
      |  "result": {
      |    "capabilities": {
      |      "semanticTokensProvider": {
      |        "legend": {
      |          "tokenTypes": [
      |            "MatchKeyword",
      |            "ReturnKeyword",
      |            "AsKeyword",
      |            "WhereKeyword",
      |            "CreateKeyword",
      |            "AndKeyword",
      |            "PatternVariable",
      |            "AssignmentOperator",
      |            "AdditionOperator",
      |            "NodeLabel",
      |            "NodeVariable",
      |            "Variable",
      |            "Edge",
      |            "FunctionApplication",
      |            "Parameter",
      |            "StringLiteral",
      |            "NullLiteral",
      |            "BooleanLiteral",
      |            "IntLiteral",
      |            "DoubleLiteral",
      |            "Property"
      |          ],
      |          "tokenModifiers": []
      |        },
      |        "range": true,
      |        "full": true
      |      }
      |    }
      |  }
      |}""".stripMargin

    val initializeMessageResultFuture = Source
      .single(TextMessage.Strict(initializeRequest))
      .via(messageFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.headOption match {
        case Some(Strict(message)) =>
          val actualMessage = parse(message)
          val expectedMessage = parse(initializeResponse)
          assert(actualMessage == expectedMessage)
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

    val initializeResponse =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 1,
      |  "result": {
      |    "capabilities": {
      |      "semanticTokensProvider": {
      |        "legend": {
      |          "tokenTypes": [
      |            "MatchKeyword",
      |            "ReturnKeyword",
      |            "AsKeyword",
      |            "WhereKeyword",
      |            "CreateKeyword",
      |            "AndKeyword",
      |            "PatternVariable",
      |            "AssignmentOperator",
      |            "AdditionOperator",
      |            "NodeLabel",
      |            "NodeVariable",
      |            "Variable",
      |            "Edge",
      |            "FunctionApplication",
      |            "Parameter",
      |            "StringLiteral",
      |            "NullLiteral",
      |            "BooleanLiteral",
      |            "IntLiteral",
      |            "DoubleLiteral",
      |            "Property"
      |          ],
      |          "tokenModifiers": []
      |        },
      |        "range": true,
      |        "full": true
      |      }
      |    }
      |  }
      |}""".stripMargin

    val initializeMessageResultFuture = Source
      .single(initializeRequestStreamed)
      .via(messageFlow)
      .take(1)
      .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { msgSeq =>
      msgSeq.headOption match {
        case Some(Strict(message)) =>
          val actualMessage = parse(message)
          val expectedMessage = parse(initializeResponse)
          assert(actualMessage == expectedMessage)
        case _ => fail("No Messages received")
      }
    }
  }

  test("open a text document with some initial Cypher and request completion items on it") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 1,
      |  "method": "initialize",
      |  "params": {
      |    "capabilities": {}
      |  }
      |}""".stripMargin

    val openNotification =
      """{
      |  "jsonrpc": "2.0",
      |  "method": "textDocument/didOpen",
      |  "params": {
      |    "textDocument": {
      |      "uri": "file:///tmp/file.txt",
      |      "text": "MATCH (n) RETURN n",
      |      "languageId": "cypher"
      |    }
      |  }
      |}""".stripMargin

    val completionRequest =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 2,
      |  "method": "textDocument/completion",
      |  "params": {
      |    "textDocument": {
      |      "uri": "file:///tmp/file.txt"
      |    },
      |    "position": {
      |      "line": 0,
      |      "character": 15
      |    }
      |  }
      |}""".stripMargin

    val expectedCompletionResponse =
      """{  
      |  "jsonrpc": "2.0",
      |  "id": 2,
      |  "result": [{ "insertText": "bar" }, { "insertText": "foo" }]
      |}""".stripMargin

    val initializeMessageResultFuture =
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

    whenReady(initializeMessageResultFuture) { msgSeq =>
      val completion = msgSeq.lift(1)
      completion match {
        case Some(Strict(msg)) => assert(parse(msg) == parse(expectedCompletionResponse))
        case _ => fail("No Messages received")
      }
    }
  }

  test("open a text document with some initial Cypher and request diagnostics on it") {
    val messageFlow: Flow[Message, Message, _] = createMessageFlow()

    val initializeRequest =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 1,
      |  "method": "initialize",
      |  "params": {
      |    "capabilities": {}
      |  }
      |}""".stripMargin

    val openNotification =
      """{
      |  "jsonrpc": "2.0",
      |  "method": "textDocument/didOpen",
      |  "params": {
      |    "textDocument": {
      |      "uri": "file:///tmp/file.txt",
      |      "text": "MATCH (n) RETUR n",
      |      "languageId": "cypher"
      |    }
      |  }
      |}""".stripMargin

    val diagnosticsRequest =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 2,
      |  "method": "textDocument/diagnostic",
      |  "params": {
      |    "textDocument": {
      |      "uri": "file:///tmp/file.txt"
      |    }
      |  }
      |}""".stripMargin

    val expectedDiagnosticResponse =
      """{
      |  "jsonrpc": "2.0",
      |  "id": 2,
      |  "result": {
      |    "kind": "full",
      |    "items": [
      |      {
      |        "range": {},
      |        "message": "no viable alternative at input 'MATCH (n) RETUR'"
      |      }
      |    ]
      |  }
      |}""".stripMargin

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
      val completion = msgSeq.lift(1)
      completion match {
        case Some(Strict(msg)) =>
          assert(parse(msg) == parse(expectedDiagnosticResponse))
        case _ => fail("No Messages received")
      }
    }
  }
}
