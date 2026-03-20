package com.thatdot.quine.v2api

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.testkit.TestKit

import io.circe.Json
import io.circe.parser.decode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import sttp.ws.WebSocketFrame

import com.thatdot.api.v2.QueryWebSocketProtocol._
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions.{V2QueryExecutor, V2QueryWebSocketFlow}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.CypherQuery

class V2QueryWebSocketFlowSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("V2QueryWebSocketFlowSpec")
  implicit val materializer: Materializer = Materializer(system)
  implicit val logConfig: LogConfig = LogConfig.permissive

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  /** Stub executor that returns canned results and records call arguments. */
  class StubExecutor extends V2QueryExecutor {
    @volatile var lastUseQuinePattern: Option[Boolean] = None
    @volatile var lastAtTime: Option[Milliseconds] = None

    def executeNodeQuery(
      query: CypherQuery,
      atTime: Option[Milliseconds],
      useQuinePattern: Boolean,
    ): (Source[UiNode, NotUsed], Boolean, Boolean) = {
      lastUseQuinePattern = Some(useQuinePattern)
      lastAtTime = atTime
      val nodes = List(UiNode("node-1", 0, "TestLabel", Map("key" -> Json.fromString("value"))))
      (Source(nodes), true, false)
    }

    def executeEdgeQuery(
      query: CypherQuery,
      atTime: Option[Milliseconds],
      useQuinePattern: Boolean,
    ): (Source[UiEdge, NotUsed], Boolean, Boolean) = {
      lastUseQuinePattern = Some(useQuinePattern)
      lastAtTime = atTime
      val edges = List(UiEdge("a", "KNOWS", "b"))
      (Source(edges), true, false)
    }

    def executeTextQuery(
      query: CypherQuery,
      atTime: Option[Milliseconds],
      useQuinePattern: Boolean,
    ): (Seq[String], Source[Seq[Json], NotUsed], Boolean, Boolean) = {
      lastUseQuinePattern = Some(useQuinePattern)
      lastAtTime = atTime
      val rows = List(Seq(Json.fromString("hello")))
      (Seq("col1"), Source(rows), true, false)
    }

    def isReady: Boolean = true
    def executionContext: ExecutionContext = system.dispatcher
  }

  private def parseServerMessage(frame: WebSocketFrame): ServerMessage = frame match {
    case WebSocketFrame.Text(payload, _, _) =>
      decode[ServerMessage](payload)(ServerMessage.decoder).fold(e => fail(s"Failed to decode: $e\n$payload"), identity)
    case other => fail(s"Expected text frame, got: $other")
  }

  private def sendText(json: String): WebSocketFrame.Text =
    WebSocketFrame.Text(json, finalFragment = true, rsv = None)

  test("RunQuery Node produces QueryStarted, NodeResults, QueryFinished") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"MATCH (n) RETURN n","sort":"Node"}"""))

    val started = parseServerMessage(sub.expectNext(3.seconds))
    started shouldBe a[QueryStarted]
    started.asInstanceOf[QueryStarted].queryId shouldBe 0

    val results = parseServerMessage(sub.expectNext(3.seconds))
    results shouldBe a[NodeResults]
    results.asInstanceOf[NodeResults].results should have size 1
    results.asInstanceOf[NodeResults].results.head.id shouldBe "node-1"

    val finished = parseServerMessage(sub.expectNext(3.seconds))
    finished shouldBe QueryFinished(0)

    pub.sendComplete()
  }

  test("RunQuery Edge produces QueryStarted, EdgeResults, QueryFinished") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":1,"query":"MATCH ()-[e]->() RETURN e","sort":"Edge"}"""))

    val started = parseServerMessage(sub.expectNext(3.seconds))
    started shouldBe a[QueryStarted]

    val results = parseServerMessage(sub.expectNext(3.seconds))
    results shouldBe a[EdgeResults]
    results.asInstanceOf[EdgeResults].results.head.edgeType shouldBe "KNOWS"

    parseServerMessage(sub.expectNext(3.seconds)) shouldBe QueryFinished(1)

    pub.sendComplete()
  }

  test("RunQuery Text produces QueryStarted, TabularResults, QueryFinished") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":2,"query":"RETURN 1","sort":"Text"}"""))

    val started = parseServerMessage(sub.expectNext(3.seconds))
    started shouldBe a[QueryStarted]
    started.asInstanceOf[QueryStarted].columns shouldBe Some(Seq("col1"))

    val results = parseServerMessage(sub.expectNext(3.seconds))
    results shouldBe a[TabularResults]
    results.asInstanceOf[TabularResults].columns shouldBe Seq("col1")

    parseServerMessage(sub.expectNext(3.seconds)) shouldBe QueryFinished(2)

    pub.sendComplete()
  }

  test("CancelQuery for running query returns MessageOk") {
    val executor = new StubExecutor {
      override def executeNodeQuery(
        query: CypherQuery,
        atTime: Option[Milliseconds],
        useQuinePattern: Boolean,
      ): (Source[UiNode, NotUsed], Boolean, Boolean) =
        // Never-ending source so the query stays running
        (Source.maybe[UiNode].mapMaterializedValue(_ => NotUsed), true, false)
    }
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":5,"query":"MATCH (n) RETURN n","sort":"Node"}"""))

    val started = parseServerMessage(sub.expectNext(3.seconds))
    started shouldBe a[QueryStarted]

    pub.sendNext(sendText("""{"type":"CancelQuery","queryId":5}"""))
    val cancelResponse = parseServerMessage(sub.expectNext(3.seconds))
    cancelResponse shouldBe MessageOk

    pub.sendComplete()
  }

  test("CancelQuery for unknown ID returns MessageError") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"CancelQuery","queryId":99}"""))

    val response = parseServerMessage(sub.expectNext(3.seconds))
    response shouldBe a[MessageError]
    response.asInstanceOf[MessageError].error should include("99")

    pub.sendComplete()
  }

  test("duplicate queryId returns MessageError") {
    val executor = new StubExecutor {
      override def executeNodeQuery(
        query: CypherQuery,
        atTime: Option[Milliseconds],
        useQuinePattern: Boolean,
      ): (Source[UiNode, NotUsed], Boolean, Boolean) =
        (Source.maybe[UiNode].mapMaterializedValue(_ => NotUsed), true, false)
    }
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"q1","sort":"Node"}"""))
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]

    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"q2","sort":"Node"}"""))
    val response = parseServerMessage(sub.expectNext(3.seconds))
    response shouldBe a[MessageError]
    response.asInstanceOf[MessageError].error should include("already being used")

    pub.sendComplete()
  }

  test("malformed message returns MessageError") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"not_valid": true}"""))

    val response = parseServerMessage(sub.expectNext(3.seconds))
    response shouldBe a[MessageError]

    pub.sendComplete()
  }

  test("atTime is passed through to executor") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)

    // Without atTime
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"MATCH (n) RETURN n","sort":"Node"}"""))
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]
    executor.lastAtTime shouldBe None

    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[NodeResults]
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe QueryFinished(0)

    // With atTime
    pub.sendNext(
      sendText("""{"type":"RunQuery","queryId":1,"query":"MATCH (n) RETURN n","sort":"Node","atTime":12345}"""),
    )
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]
    executor.lastAtTime shouldBe Some(Milliseconds(12345))

    pub.sendComplete()
  }

  test("authorizer that denies RunQuery produces MessageError") {
    val executor = new StubExecutor
    val authorizer: V2QueryWebSocketFlow.MessageAuthorizer = {
      case _: RunQuery => Left("Insufficient permissions: GraphWrite required")
      case other => Right(other)
    }
    val flow = V2QueryWebSocketFlow.buildFlow(executor, authorizeMessage = Some(authorizer))
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"MATCH (n) RETURN n","sort":"Node"}"""))

    val response = parseServerMessage(sub.expectNext(3.seconds))
    response shouldBe a[MessageError]
    response.asInstanceOf[MessageError].error should include("GraphWrite")

    pub.sendComplete()
  }

  test("authorizer that denies CancelQuery produces MessageError") {
    val executor = new StubExecutor {
      override def executeNodeQuery(
        query: CypherQuery,
        atTime: Option[Milliseconds],
        useQuinePattern: Boolean,
      ): (Source[UiNode, NotUsed], Boolean, Boolean) =
        (Source.maybe[UiNode].mapMaterializedValue(_ => NotUsed), true, false)
    }
    val authorizer: V2QueryWebSocketFlow.MessageAuthorizer = {
      case _: CancelQuery => Left("Insufficient permissions: QueryCancel required")
      case other => Right(other)
    }
    val flow = V2QueryWebSocketFlow.buildFlow(executor, authorizeMessage = Some(authorizer))
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)

    // RunQuery is allowed
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"q","sort":"Node"}"""))
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]

    // CancelQuery is denied
    pub.sendNext(sendText("""{"type":"CancelQuery","queryId":0}"""))
    val response = parseServerMessage(sub.expectNext(3.seconds))
    response shouldBe a[MessageError]
    response.asInstanceOf[MessageError].error should include("QueryCancel")

    pub.sendComplete()
  }

  test("authorizer that allows all messages does not interfere with normal flow") {
    val executor = new StubExecutor
    val authorizer: V2QueryWebSocketFlow.MessageAuthorizer = msg => Right(msg)
    val flow = V2QueryWebSocketFlow.buildFlow(executor, authorizeMessage = Some(authorizer))
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"MATCH (n) RETURN n","sort":"Node"}"""))

    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[NodeResults]
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe QueryFinished(0)

    pub.sendComplete()
  }

  test("QuinePattern interpreter is passed through to executor") {
    val executor = new StubExecutor
    val flow = V2QueryWebSocketFlow.buildFlow(executor)
    val (pub, sub) = TestSource[WebSocketFrame]().via(flow).toMat(TestSink[WebSocketFrame]())(Keep.both).run()

    sub.request(10)

    // Default (Cypher)
    pub.sendNext(sendText("""{"type":"RunQuery","queryId":0,"query":"q","sort":"Node"}"""))
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]
    executor.lastUseQuinePattern shouldBe Some(false)

    // Wait for results + finished
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[NodeResults]
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe QueryFinished(0)

    // Explicit QuinePattern
    pub.sendNext(
      sendText("""{"type":"RunQuery","queryId":1,"query":"q","sort":"Node","interpreter":"QuinePattern"}"""),
    )
    parseServerMessage(sub.expectNext(3.seconds)) shouldBe a[QueryStarted]
    executor.lastUseQuinePattern shouldBe Some(true)

    pub.sendComplete()
  }
}
