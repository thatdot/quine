package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

import akka.actor.ActorSystem
import akka.pattern.Patterns
import akka.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite

import com.thatdot.quine.model.{Milliseconds, PropertyValue, QuineId, QuineValue}
import com.thatdot.quine.persistor.{InMemoryPersistor, PersistenceAgent}

class HistoricalQueryTests extends AsyncFunSuite with BeforeAndAfterAll {

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  def makePersistor(system: ActorSystem): PersistenceAgent = InMemoryPersistor.empty

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  implicit val graph: LiteralOpsGraph = Await.result(
    GraphService(
      "historical-query-tests",
      persistor = makePersistor,
      idProvider = idProvider
    ),
    timeout.duration
  )
  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher

  var t0: Milliseconds = Milliseconds(0L)
  var t1: Milliseconds = Milliseconds(0L)
  var t2: Milliseconds = Milliseconds(0L)
  var t3: Milliseconds = Milliseconds(0L)
  var t4: Milliseconds = Milliseconds(0L)
  var t5: Milliseconds = Milliseconds(0L)

  val qid: QuineId = idProvider.customIdToQid(42L) // meaning of life

  override def beforeAll(): Unit = {
    // Pause to ensure timestamps are distinct at millisecond granularity
    def pause(): Future[Unit] = {
      val promise = Promise[Unit]()
      graph.system.scheduler.scheduleOnce(2.milliseconds)(promise.success(()))
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
      timeout.duration * 2L
    )
  }

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  test("query before any events or snapshots") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t0)).map { props =>
      assert(props == Map.empty)
    }
  }

  test("query after first event") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t1)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L))
      )
      assert(props == expected)
    }
  }

  test("query after second event") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t2)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L))
      )
      assert(props == expected)
    }
  }

  test("query after first snapshot") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t3)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L))
      )
      assert(props == expected)
    }
  }

  test("query after first event after snapshot") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t4)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L)),
        Symbol("prop3") -> PropertyValue(QuineValue.Integer(3L))
      )
      assert(props == expected)
    }
  }

  test("query after last snapshot") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = Some(t5)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L)),
        Symbol("prop3") -> PropertyValue(QuineValue.Integer(3L))
      )
      assert(props == expected)
    }
  }

  test("query in present") {
    assume(runnable)
    graph.literalOps.getProps(qid, atTime = None).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L)),
        Symbol("prop3") -> PropertyValue(QuineValue.Integer(3L))
      )
      assert(props == expected)
    }
  }
}
