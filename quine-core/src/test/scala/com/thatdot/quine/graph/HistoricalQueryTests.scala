package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AsyncFunSuite

import com.thatdot.quine.model.{Milliseconds, PropertyValue, QuineId, QuineValue}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor
}

class HistoricalQueryTests extends AsyncFunSuite with BeforeAndAfterAll {

  // Override this if tests need to be skipped
  def runnable: Boolean = true

  def makePersistor(system: ActorSystem): PrimePersistor =
    new StatelessPrimePersistor(
      PersistenceConfig(),
      None,
      (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns)
    )(Materializer.matFromSystem(system))

  implicit val timeout: Timeout = Timeout(10.seconds)
  val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  val graph: LiteralOpsGraph = Await.result(
    GraphService(
      "historical-query-tests",
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = makePersistor,
      idProvider = idProvider
    ),
    timeout.duration
  )
  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher
  val namespace: NamespaceId = None // Use default namespace

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
        _ <- graph.literalOps(namespace).setProp(qid, "prop1", QuineValue.Integer(1L))
        _ <- pause()
        _ = (t1 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.literalOps(namespace).setProp(qid, "prop2", QuineValue.Integer(2L))
        _ <- pause()
        _ = (t2 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.requestNodeSleep(namespace, qid)
        _ <- pause()
        _ = (t3 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.literalOps(namespace).setProp(qid, "prop3", QuineValue.Integer(3L))
        _ <- pause()
        _ = (t4 = Milliseconds.currentTime())
        _ <- pause()
        _ <- graph.requestNodeSleep(namespace, qid)
        _ <- pause()
        _ = (t5 = Milliseconds.currentTime())
      } yield (),
      timeout.duration * 2L
    )
  }

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  test("query before any events or sleeps") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t0)).map { props =>
      assert(props == Map.empty)
    }
  }

  test("logState properties before any events or sleeps") {
    assume(runnable)
    graph.literalOps(namespace).logState(qid, atTime = Some(t0)).map { s =>
      assert(s.properties.isEmpty)
    }
  }

  test("logState journal before any events or sleeps") {
    assume(runnable)
    graph.literalOps(namespace).logState(qid, atTime = Some(t0)).map { s =>
      assert(s.journal.isEmpty)
    }
  }

  test("query after first event") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t1)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L))
      )
      assert(props == expected)
    }
  }

  test("query after second event") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t2)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L))
      )
      assert(props == expected)
    }
  }

  test("query after first sleep") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t3)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L))
      )
      assert(props == expected)
    }
  }

  test("query after first event after sleep") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t4)).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L)),
        Symbol("prop3") -> PropertyValue(QuineValue.Integer(3L))
      )
      assert(props == expected)
    }
  }

  test("query after last sleep") {
    assume(runnable)
    graph.literalOps(namespace).getProps(qid, atTime = Some(t5)).map { props =>
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
    graph.literalOps(namespace).getProps(qid, atTime = None).map { props =>
      val expected = Map(
        Symbol("prop1") -> PropertyValue(QuineValue.Integer(1L)),
        Symbol("prop2") -> PropertyValue(QuineValue.Integer(2L)),
        Symbol("prop3") -> PropertyValue(QuineValue.Integer(3L))
      )
      assert(props == expected)
    }
  }
}
