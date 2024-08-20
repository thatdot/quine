package com.thatdot.quine.ingest2.sources

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.ActorSystem

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest2.source.IngestBounds
import com.thatdot.quine.app.ingest2.sources.NumberIteratorSource
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.ingest2.IngestSourceTestSupport.streamedCypherValues

class DelimitedSourcesSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("StreamDecodersSpec")
  implicit val ec: ExecutionContext = actorSystem.getDispatcher
  val meter: IngestMeter = IngestMetered.ingestMeter(None, "test")

  override def afterAll(): Unit =
    actorSystem.terminate().foreach(_ => ())

  describe("NumberIteratorSource") {
    it("streams cypher values") {
      val numberIteratorSource =
        NumberIteratorSource(IngestBounds(2L, Some(10L)), meter).decodedSource
      val values = streamedCypherValues(numberIteratorSource).toList
      values.length shouldEqual 10
      values.head shouldEqual Expr.Integer(2L)
    }
  }

}
