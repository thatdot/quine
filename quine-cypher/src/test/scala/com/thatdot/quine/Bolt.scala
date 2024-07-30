package com.thatdot.quine

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.stream.testkit.scaladsl._
import org.apache.pekko.util.{ByteString, Timeout}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.bolt.Protocol
import com.thatdot.quine.graph.{CypherOpsGraph, GraphService, QuineIdLongProvider}
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}
import com.thatdot.quine.util.ByteConversions
import com.thatdot.quine.util.Log._

class Bolt extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  def toHex(str: String): ByteString = ByteString(ByteConversions.parseHexBinary(str.filter(_ != ' ')))

  implicit val timeout: Timeout = Timeout(3600 seconds)
  implicit val logConfig: LogConfig = LogConfig.testing
  implicit val graph: CypherOpsGraph = Await.result(
    GraphService(
      "bolt-protocol-test-system",
      effectOrder = EventEffectOrder.MemoryFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
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
