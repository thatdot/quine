package com.thatdot.quine

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl._
import akka.util.{ByteString, Timeout}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.bolt.Protocol
import com.thatdot.quine.graph.{CypherOpsGraph, GraphService, QuineIdLongProvider}
import com.thatdot.quine.persistor.InMemoryPersistor
import com.thatdot.quine.util.HexConversions

class Bolt extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  def toHex(str: String): ByteString = ByteString(HexConversions.parseHexBinary(str.filter(_ != ' ')))

  implicit val timeout: Timeout = Timeout(3600 seconds)
  implicit val graph: CypherOpsGraph = Await.result(
    GraphService(
      "bolt-protocol-test-system",
      persistor = _ => InMemoryPersistor.empty,
      idProvider = QuineIdLongProvider()
    ),
    timeout.duration
  )
  implicit val system: ActorSystem = graph.system

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  val flow: Flow[ByteString, ByteString, NotUsed] = Protocol.bolt

  test("Handshake version negotiation should succeed when client offers v1") {
    val (pub, sub) =
      TestSource.probe[ByteString].via(flow).toMat(TestSink.probe[ByteString])(Keep.both).run()

    pub.sendNext(toHex("00 00 00 00" * 3))
    pub.sendNext(toHex("00 00 00 01"))
    val negotiatedVersion = sub.requestNext()

    negotiatedVersion shouldEqual toHex("00 00 00 01")
    assertThrows[AssertionError](
      sub.expectComplete()
    )
  }
  test("Handshake version negotiation should fail when client does not offer v1") {
    val (pub, sub) =
      TestSource.probe[ByteString].via(flow).toMat(TestSink.probe[ByteString])(Keep.both).run()

    pub.sendNext(toHex("00 00 01 07" * 4))
    val negotiatedVersion = sub.requestNext()

    // TODO server should DC when no valid version is offered
    pendingUntilFixed {
      negotiatedVersion shouldEqual toHex("00 00 00 00")
      try {
        val _ = sub.expectError()
      } catch {
        case _: AssertionError => fail("Connection was not closed when version did not match")
      }
    }
  }

}
