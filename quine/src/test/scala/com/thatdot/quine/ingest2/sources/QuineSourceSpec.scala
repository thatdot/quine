package com.thatdot.quine.ingest2.sources

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{Attributes, Materializer}

import io.circe.Json
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.model.ingest2.sources.{QuineFrame, QuineSource}
import com.thatdot.quine.app.routes.IngestMetered
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString

class QuineSourceSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("QuineSourceSpec")
  implicit val mat: Materializer = Materializer(system)

  override def afterAll(): Unit = Await.ready(system.terminate(), 10.seconds): Unit

  private def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  private def newSource(): QuineSource = {
    val meter = IngestMetered.ingestMeter(
      defaultNamespaceId,
      randomString(),
      HostQuineMetrics(enableDebugMetrics = false, metricRegistry = Metrics, omitDefaultNamespace = true),
    )
    QuineSource(meter)
  }

  private def reply(source: QuineSource, json: Json, frame: QuineFrame): Unit =
    await(Source.single((json, frame)).runWith(source.replies.get.mapMaterializedValue(_ => Future.unit))): Unit

  describe("QuineSource") {

    it("merges every connection's records into one stream, each tagged with its connection") {
      val source = newSource()
      val received = source.stream.map(_._2).take(2).runWith(Sink.seq)
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val b = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      Source.single(Json.fromString("from a")).runWith(a.inbound)
      Source.single(Json.fromString("from b")).runWith(b.inbound)
      await(received).toSet shouldBe Set(
        QuineFrame(a.id, Json.fromString("from a")),
        QuineFrame(b.id, Json.fromString("from b")),
      )
    }

    it("sends a record's reply only to the connection that sent the record") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val b = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val repliesToA = a.outbound.take(1).runWith(Sink.seq)
      val repliesToB = b.outbound.takeWithin(300.millis).runWith(Sink.seq)
      Thread.sleep(100) // let both reply streams start, which registers them
      reply(source, Json.fromString("score for a"), QuineFrame(a.id, Json.Null))
      await(repliesToA) shouldBe Seq(Json.fromString("score for a"))
      await(repliesToB) shouldBe empty
    }

    it("drops replies for a connection that has detached") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      await(a.outbound.take(0).runWith(Sink.ignore)) // the reply source terminates: a detaches
      noException should be thrownBy reply(source, Json.fromString("late"), QuineFrame(a.id, Json.Null))
    }

    it("waits for a connection that reads slowly instead of dropping its replies") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 2, rescoreBufferSize = 4)
      // A consumer that pulls nothing until asked, with the smallest buffer a stage allows.
      val pending = a.outbound.runWith(Sink.queue[Json]().withAttributes(Attributes.inputBuffer(1, 1)))
      Thread.sleep(100) // let the outbound stream start, which registers the connection
      // 100 replies to a connection that isn't reading: the sink stops taking them once its queue is full.
      val allTaken = Source(1 to 100)
        .map(i => (Json.fromInt(i), QuineFrame(a.id, Json.Null)))
        .watchTermination()(Keep.right)
        .to(source.replies.get)
        .run()
      Thread.sleep(300)
      allTaken.isCompleted shouldBe false
      // Once the connection reads, every reply arrives, in order.
      val delivered = (1 to 100).map(_ => await(pending.pull()))
      delivered shouldBe (1 to 100).map(i => Some(Json.fromInt(i)))
      noException should be thrownBy await(allTaken)
    }

    it("stops waiting for a connection that detaches, dropping what it can't deliver") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 2, rescoreBufferSize = 4)
      val pending = a.outbound.runWith(Sink.queue[Json]().withAttributes(Attributes.inputBuffer(1, 1)))
      Thread.sleep(100) // let the outbound stream start, which registers the connection
      val allTaken = Source(1 to 100)
        .map(i => (Json.fromInt(i), QuineFrame(a.id, Json.Null)))
        .watchTermination()(Keep.right)
        .to(source.replies.get)
        .run()
      Thread.sleep(300)
      allTaken.isCompleted shouldBe false
      pending.cancel() // the connection closes while the sink waits on it
      noException should be thrownBy await(allTaken)
    }

    it("answers a connection directly, outside the ingest") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val replies = a.outbound.take(1).runWith(Sink.seq)
      Thread.sleep(100) // let the reply stream start, which registers it
      source.replyDirectly(a.id, Json.obj("error" -> Json.fromString("bad frame")))
      await(replies) shouldBe Seq(Json.obj("error" -> Json.fromString("bad frame")))
    }

    it("completes every connection's replies when the ingest terminates") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val done = a.outbound.runWith(Sink.ignore)
      Thread.sleep(100) // let the reply stream start, which registers it
      source.onTermination()
      noException should be thrownBy await(done)
    }

    it("sends a rescore on an attached connection, alongside its replies, and counts it") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      val frames = a.outbound.take(2).runWith(Sink.seq)
      Thread.sleep(100) // let the outbound stream start, which registers it
      source.offerRescore(Json.fromString("rescore")) shouldBe true
      reply(source, Json.fromString("score"), QuineFrame(a.id, Json.Null))
      await(frames).toSet shouldBe Set(Json.fromString("rescore"), Json.fromString("score"))
      source.rescoreCounts shouldBe QuineSource.RescoreCounts(sent = 1, dropped = 0)
    }

    it("passes over a connection whose rescore queue is full") {
      val source = newSource()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 1)
      val b = source.attach(replyBufferSize = 4, rescoreBufferSize = 1)
      // Consumers that pull nothing until asked, with the smallest buffer a stage allows.
      val pendingA = a.outbound.runWith(Sink.queue[Json]().withAttributes(Attributes.inputBuffer(1, 1)))
      val pendingB = b.outbound.runWith(Sink.queue[Json]().withAttributes(Attributes.inputBuffer(1, 1)))
      Thread.sleep(100) // let the outbound streams start, which registers them
      val taken = (1 to 20).count(i => source.offerRescore(Json.fromInt(i)))
      def drain(pending: org.apache.pekko.stream.scaladsl.SinkQueueWithCancel[Json]) = Iterator
        .continually(Try(Await.result(pending.pull(), 300.millis)).toOption.flatten)
        .takeWhile(_.isDefined)
        .flatten
        .toList
      val delivered = drain(pendingA) ++ drain(pendingB)
      delivered should have size taken.toLong
      taken should be < 20 // both queues filled, and the rest were refused rather than blocking
      taken should be >= 2 // each connection took at least one
      source.rescoreCounts.sent shouldBe taken.toLong
    }

    it("refuses a rescore when no connection is attached, and counts a drop only when told to") {
      val source = newSource()
      source.offerRescore(Json.fromString("rescore")) shouldBe false
      source.rescoreCounts shouldBe QuineSource.RescoreCounts(sent = 0, dropped = 0)
      source.rescoreDropped()
      source.rescoreCounts shouldBe QuineSource.RescoreCounts(sent = 0, dropped = 1)
    }

    it("completes the replies of a connection that attaches after the ingest terminated") {
      val source = newSource()
      source.onTermination()
      val a = source.attach(replyBufferSize = 4, rescoreBufferSize = 4)
      noException should be thrownBy await(a.outbound.runWith(Sink.ignore))
    }
  }
}
