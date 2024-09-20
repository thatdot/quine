package com.thatdot.quine.ingest2.sources

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest2.codec.StringDecoder
import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.app.ingest2.sources.withKillSwitches
import com.thatdot.quine.app.routes.{IngestMeter, IngestMetered}
import com.thatdot.quine.app.{Metrics, ShutdownSwitch}
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.ingest2.IngestSourceTestSupport.{randomString, streamedCypherValues}

/** A frame source type unique to this streaming source. */
case class TestFrame(value: String)

case class TestSource(values: Iterable[TestFrame]) {
  val ackFlow: Flow[TestFrame, Done, NotUsed] = Flow[TestFrame].map(_ => Done)

  val source: Source[TestFrame, ShutdownSwitch] = withKillSwitches(Source.fromIterator(() => values.iterator))
  val meter: IngestMeter = IngestMetered.ingestMeter(
    None,
    randomString(),
    HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
  )

  def framedSource: FramedSource = FramedSource[TestFrame](
    source,
    meter,
    _.value.getBytes, //source extracts bytes from the value member of TestFrame
  )

}

class FramedSourceSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("StreamDecodersSpec")
  implicit val ec: ExecutionContext = actorSystem.getDispatcher
  describe("test source") {
    it("extract values from TestFrame") {
      val inputData: List[TestFrame] = List("A", "B", "C").map(TestFrame)
      val testSource = TestSource(inputData)
      val decodedSource = testSource.framedSource.toDecoded(StringDecoder)
      streamedCypherValues(decodedSource) shouldBe List(
        Expr.Str("A"),
        Expr.Str("B"),
        Expr.Str("C"),
      )
    }
  }
}
