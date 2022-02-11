package com.thatdot.quine.compiler.cypher

import scala.collection.immutable.HashSet
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer}
import akka.util.Timeout

import org.scalactic.source.Position
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.cypher.CompiledQuery
import com.thatdot.quine.persistor.InMemoryPersistor

class CypherHarness(graphName: String) extends AnyFunSpec with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(10.seconds)
  implicit val idProv: QuineIdLongProvider = QuineIdLongProvider()
  implicit val graph: GraphService = Await.result(
    GraphService(
      graphName,
      persistor = _ => InMemoryPersistor.empty,
      idProvider = QuineIdLongProvider()
    ),
    timeout.duration
  )
  implicit val materializer: Materializer = graph.materializer

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  implicit val ec: ExecutionContext = graph.shardDispatcherEC

  /** Check that a given query matches an expected output.
    *
    * @param queryText query whose output we are checking
    * @param expectedColumns the expected columns of output
    * @param expectedRows the expected output rows
    * @param expectedIsReadOnly
    * @param expectedIsIdempotent
    * @param expectedCanContainAllNodeScan
    * @param parameters query parameters
    * @param ordered whether the order of the output rows matters
    * @param skip should the test be skipped
    * @param pos source position of the call to `testQuery`
    */
  final def testQuery(
    queryText: String,
    expectedColumns: Vector[String],
    expectedRows: Seq[Vector[cypher.Value]],
    expectedIsReadOnly: Boolean = true,
    expectedIsIdempotent: Boolean = true,
    expectedCanContainAllNodeScan: Boolean = false,
    parameters: Map[String, cypher.Value] = Map.empty,
    ordered: Boolean = true,
    skip: Boolean = false
  )(implicit
    pos: Position
  ): Unit = {
    def theTest(): Assertion = {
      val queryResults = queryCypherValues(queryText, parameters = parameters)
      assert(expectedColumns.map(Symbol(_)) === queryResults.columns, "columns must match")
      val (killSwitch, rowsFut) = queryResults.results
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      // Schedule cancellation for the query if it takes too long
      materializer.scheduleOnce(
        timeout.duration,
        () => killSwitch.abort(new java.util.concurrent.TimeoutException())
      )

      val actualRows = Await.result(rowsFut, timeout.duration)
      if (ordered)
        assert(actualRows === expectedRows, "ordered rows must match")
      else
        assert(HashSet(actualRows: _*) == HashSet(expectedRows: _*), "unordered rows must match")

      assert({ Plan.fromQuery(queryResults.compiled.query).toValue; true }, "query plan can be rendered")
      assert(queryResults.compiled.query.isReadOnly == expectedIsReadOnly, "isReadOnly must match")
      assert(queryResults.compiled.query.isIdempotent == expectedIsIdempotent, "isIdempotent must match")
      assert(
        queryResults.compiled.query.canContainAllNodeScan == expectedCanContainAllNodeScan,
        "canContainAllNodeScan must match"
      )
    }

    if (skip)
      ignore(queryText)(theTest())(pos)
    else
      it(queryText)(theTest())(pos)
  }

  /** Check that a given expression matches an expected output
    *
    * @param expressionText expression whose output we are checking
    * @param expectedValue the expected output value
    * @param expectedIsReadOnly should the expression be readonly?
    * @param expectedIsIdempotent should the expression be idempotent?
    * @param expectedCanContainAllNodeScan is it possible for the expression to scan all nodes?
    * @param skip should the test be skipped
    * @param queryPreamble text to put before the expression to turn it into a query
    * @param pos source position of the call to `testExpression`
    */
  final def testExpression(
    expressionText: String,
    expectedValue: cypher.Value,
    expectedIsReadOnly: Boolean = true,
    expectedIsIdempotent: Boolean = true,
    expectedCanContainAllNodeScan: Boolean = false,
    skip: Boolean = false,
    queryPreamble: String = "RETURN "
  )(implicit
    pos: Position
  ): Unit =
    testQuery(
      queryText = queryPreamble + expressionText,
      expectedColumns = Vector(expressionText),
      expectedRows = Seq(Vector(expectedValue)),
      expectedIsReadOnly = expectedIsReadOnly,
      expectedIsIdempotent = expectedIsIdempotent,
      expectedCanContainAllNodeScan = expectedCanContainAllNodeScan,
      skip = skip
    )

  /** Check that a given query crashes with the given exception.
    *
    * @param queryText query whose output we are checking
    * @param expected exception that we expect to intercept
    * @param pos source position of the call to `interceptQuery`
    * @param manifest information about the exception type we expect
    */
  final def interceptQuery[T <: AnyRef](
    queryText: String,
    expected: T
  )(implicit
    pos: Position,
    manifest: Manifest[T]
  ): Unit = {
    def theTest(): Assertion = {
      val actual = intercept[T] {
        val queried = queryCypherValues(queryText).results.runWith(Sink.ignore)
        Await.result(queried, timeout.duration)
      }
      assert(actual == expected, "exception must match")
    }

    it(queryText)(theTest())(pos)
  }

  /** Check query static analysis output.
    *
    * @param queryText query whose output we are checking
    * @param expectedIsReadOnly
    * @param expectedIsIdempotent
    * @param expectedCanContainAllNodeScan
    */
  final def testQueryStaticAnalysis(
    queryText: String,
    expectedIsReadOnly: Boolean,
    expectedIsIdempotent: Boolean,
    expectedCanContainAllNodeScan: Boolean
  )(implicit
    pos: Position
  ): Unit =
    it(queryText) {
      val CompiledQuery(_, query, _, _, _) = compile(queryText)
      assert(query.isReadOnly == expectedIsReadOnly, "isReadOnly must match")
      assert(query.isIdempotent == expectedIsIdempotent, "isIdempotent must match")
      assert(
        query.canContainAllNodeScan == expectedCanContainAllNodeScan,
        "canContainAllNodeScan must match"
      )
    }
}
