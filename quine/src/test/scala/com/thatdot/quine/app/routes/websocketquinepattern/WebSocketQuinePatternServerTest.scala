package com.thatdot.quine.app.routes.websocketquinepattern

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString

import io.circe.Json
import io.circe.syntax._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures.{PatienceConfig, whenReady}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

class WebSocketQuinePatternServerTest extends AnyFunSuite with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = Materializer(system)
  implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(500, Millis))

  def createTestServer(): WebSocketQuinePatternServer = new WebSocketQuinePatternServer(system)

  override def afterAll(): Unit =
    whenReady(system.terminate()) { _ =>
      super.afterAll()
    }

  test("ensure binary messages and undefined messages are handled correctly") {
    val server = createTestServer()
    val messageFlow: Flow[Message, Message, _] = server.messagesFlow
    val testString: String = "Test123"

    val binaryMessageResultFuture = Source
      .single(BinaryMessage.Strict(ByteString(testString)))
      .via(messageFlow)
      .runWith(Sink.seq)

    val messageResultFuture = Source
      .single(TextMessage(testString))
      .via(messageFlow)
      .runWith(Sink.seq)

    whenReady(binaryMessageResultFuture) { binaryMessageResult =>
      assert(binaryMessageResult == Seq(TextMessage.Strict("BinaryNotSupported".asJson.noSpaces)))
    }

    whenReady(messageResultFuture) { messageResult =>
      assert(messageResult == Seq(TextMessage.Strict("UnsupportedTextMessage".asJson.noSpaces)))
    }
  }

  test("ensure Initialize message is handled correctly") {
    val server = createTestServer()
    val messageFlow: Flow[Message, Message, _] = server.messagesFlow

    val initializeMessageResultFuture = Source
      .single(TextMessage.Strict(Json.fromString("Initialize").noSpaces))
      .via(messageFlow)
      .runWith(Sink.seq)

    whenReady(initializeMessageResultFuture) { messageResult =>
      assert(messageResult == Seq(TextMessage.Strict(Json.fromString("InitializeSuccess").noSpaces)))
    }
  }
}
