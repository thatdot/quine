package com.thatdot.quine.compiler.cypher

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

import akka.pattern.Patterns
import akka.stream.scaladsl.{Keep, Sink}

import org.scalatest.Assertion

import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineValue}

class HistoricalQueryTests extends CypherHarness("historical-query-tests") {
  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher
  var t0: Milliseconds = Milliseconds(0L)
  var t1: Milliseconds = Milliseconds(0L)
  var t2: Milliseconds = Milliseconds(0L)
  var t3: Milliseconds = Milliseconds(0L)
  var t4: Milliseconds = Milliseconds(0L)
  var t5: Milliseconds = Milliseconds(0L)
  val qid: QuineId = idProv.customIdToQid(0L)
  val getNodeCypherQuery: String = s"""MATCH (n) WHERE strId(n) = "${qid.pretty}" RETURN n"""

  override def beforeAll(): Unit = {
    // Pause to ensure timestamps are distinct at millisecond granularity
    def pause(): Future[Unit] = {
      val promise = Promise[Unit]()
      graph.system.scheduler.scheduleOnce(2 milliseconds)(promise.success(()))
      promise.future
    }

    Await.result(
      for {
        _ <- Patterns.retry(
          () => Future(graph.requiredGraphIsReady()),
          attempts = 100,
          delay = 200.millis,
          graph.system.scheduler,
          graph.system.dispatcher
        )
        _ = (t0 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.literalOps.setProp(qid, "prop1", QuineValue.Integer(1L))
        _ <- pause()
        _ = (t1 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.literalOps.setProp(qid, "prop2", QuineValue.Integer(2L))
        _ <- pause()
        _ = (t2 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.snapshotInMemoryNodes()
        _ <- pause()
        _ = (t3 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.literalOps.setProp(qid, "prop3", QuineValue.Integer(3L))
        _ <- pause()
        _ = (t4 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.snapshotInMemoryNodes()
        _ <- pause()
        _ = (t5 = Milliseconds.currentTime())
      } yield (),
      timeout.duration
    )
  }

  def assertPropertiesAtTime(time: Milliseconds, expected: Map[Symbol, Value]): Future[Assertion] = {
    val queryResults = queryCypherValues(
      getNodeCypherQuery,
      atTime = Some(time)
    )
    queryResults.results
      .toMat(Sink.seq)(Keep.right)
      .run()
      .map(r => assert(r == Vector(Vector(Expr.Node(qid, Set.empty, expected)))))
  }

  it("query before any events or snapshots") {
    assertPropertiesAtTime(t0, Map.empty)
  }

  it("query after first event") {
    assertPropertiesAtTime(
      t1,
      Map(
        Symbol("prop1") -> Expr.Integer(1L)
      )
    )
  }

  it("query after second event") {
    assertPropertiesAtTime(
      t2,
      Map(
        Symbol("prop1") -> Expr.Integer(1L),
        Symbol("prop2") -> Expr.Integer(2L)
      )
    )
  }

  it("query after first snapshot") {
    assertPropertiesAtTime(
      t3,
      Map(
        Symbol("prop1") -> Expr.Integer(1L),
        Symbol("prop2") -> Expr.Integer(2L)
      )
    )
  }

  it("query after first event after snapshot") {
    assertPropertiesAtTime(
      t4,
      Map(
        Symbol("prop1") -> Expr.Integer(1L),
        Symbol("prop2") -> Expr.Integer(2L),
        Symbol("prop3") -> Expr.Integer(3L)
      )
    )
  }

  it("query after last snapshot") {
    assertPropertiesAtTime(
      t5,
      Map(
        Symbol("prop1") -> Expr.Integer(1L),
        Symbol("prop2") -> Expr.Integer(2L),
        Symbol("prop3") -> Expr.Integer(3L)
      )
    )
  }
}
