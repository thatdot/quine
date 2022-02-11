package com.thatdot.quine.gremlin

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer}
import akka.util.Timeout

import org.scalactic.source.Position
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfterAll}

import com.thatdot.quine.graph._
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.InMemoryPersistor

class GremlinHarness(graphName: String) extends AnyFunSuite with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(10.seconds)
  implicit val idProv: QuineUUIDProvider.type = QuineUUIDProvider
  implicit val graph: LiteralOpsGraph = Await.result(
    GraphService(
      graphName,
      persistor = _ => InMemoryPersistor.empty,
      idProvider = idProv
    ),
    timeout.duration
  )
  implicit val materializer: Materializer = graph.materializer

  val gremlin: GremlinQueryRunner = GremlinQueryRunner(graph)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  /** Check that a given query matches an expected output.
    *
    *  @param queryText query whose output we are checking
    *  @param expected the expected output
    *  @param pos source position of the call to `testQuery`
    */
  def testQuery(
    queryText: String,
    expected: Seq[Any],
    parameters: Map[Symbol, QuineValue] = Map.empty,
    ordered: Boolean = true
  )(implicit
    pos: Position
  ): Assertion = {
    val queryResults = gremlin.query(queryText, parameters)
    val (killSwitch, resultsFut) = queryResults
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()

    // Schedule cancellation for the query if it takes too long
    materializer.scheduleOnce(
      timeout.duration,
      () => killSwitch.abort(new java.util.concurrent.TimeoutException())
    )

    val actualResults = Await.result(resultsFut, timeout.duration)
    if (ordered)
      assert(actualResults == expected, "ordered results must match")
    else
      assert(actualResults.toSet == expected.toSet, "unordered results must match")
  }

  /** Check that a given query crashes with the given exception.
    *
    *  @param queryText query whose output we are checking
    *  @param expectedMessage the expected error
    *  @param pos source position of the call to `testQuery`
    */
  def interceptQuery(
    queryText: String,
    expectedMessage: String
  )(implicit
    pos: Position
  ): Assertion = {
    val actual = intercept[QuineGremlinException] {
      val queried = gremlin.query(queryText).runWith(Sink.ignore)
      Await.result(queried, timeout.duration)
    }
    assert(actual.pretty == expectedMessage)
  }
}
