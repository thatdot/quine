package com.thatdot.quine.compiler.cypher

import scala.collection.immutable.HashSet
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer}
import org.apache.pekko.util.Timeout

import org.scalactic.source.Position
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph._
import com.thatdot.quine.graph.cypher.{CompiledQuery, Location, RunningCypherQuery, Value}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

class CypherHarness(val graphName: String) extends AsyncFunSpec with BeforeAndAfterAll {

  object QuineIdImplicitConversions {
    implicit def toQid[A](typed: A)(implicit idProvider: QuineIdProvider.Aux[A]): QuineId =
      idProvider.customIdToQid(typed)
    implicit def fromQid(qid: QuineId)(implicit idProvider: QuineIdProvider): idProvider.CustomIdType =
      idProvider.customIdFromQid(qid).get
  }

  val timeout: Timeout = Timeout(10.seconds)
  // Used for e.g. literal ops that insert data - they use this as the timeout on relayAsk invocations.
  implicit val relayAskTimeout: Timeout = Timeout(3.seconds)
  implicit val idProv: QuineIdLongProvider = QuineIdLongProvider()
  implicit protected val logConfig: LogConfig = LogConfig.permissive
  lazy val graph: BaseGraph with CypherOpsGraph with LiteralOpsGraph = Await.result(
    GraphService(
      graphName,
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = idProv,
    ),
    timeout.duration,
  )
  val cypherHarnessNamespace: NamespaceId = None // Use default namespace
  implicit def materializer: Materializer = graph.materializer

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  sealed protected trait RunnableCypher[T] {
    def run(t: T, parameters: Map[String, cypher.Value]): RunningCypherQuery
    def testName(t: T): String
  }
  implicit final protected object RunnableString extends RunnableCypher[String] {
    def run(t: String, parameters: Map[String, cypher.Value]): RunningCypherQuery =
      queryCypherValues(t, cypherHarnessNamespace, parameters, cacheCompilation = false)(graph)
    def testName(t: String): String = s"String Query: $t"
  }
  implicit final protected object RunnableCompiledQuery extends RunnableCypher[CompiledQuery[Location.External]] {
    def run(t: CompiledQuery[Location.External], parameters: Map[String, Value]): RunningCypherQuery =
      graph.cypherOps.query(t, cypherHarnessNamespace, None, parameters)
    def testName(t: CompiledQuery[Location.External]): String =
      s"CompiledQuery: ${t.queryText.getOrElse(t.query.toString)}"
  }

  /** Check that a given query matches an expected output.
    *
    * @param queryText query whose output we are checking
    * @param expectedColumns the expected columns of output
    * @param expectedRows the expected output rows
    * @param expectedIsReadOnly
    * @param expectedCannotFail
    * @param expectedIsIdempotent
    * @param expectedCanContainAllNodeScan
    * @param parameters query parameters
    * @param ordered whether the order of the output rows matters
    * @param skip should the test be skipped
    * @param pos source position of the call to `testQuery`
    */
  final def testQuery[T](
    query: T,
    expectedColumns: Vector[String],
    expectedRows: Seq[Vector[cypher.Value]],
    expectedIsReadOnly: Boolean = true,
    expectedCannotFail: Boolean = false,
    expectedIsIdempotent: Boolean = true,
    expectedCanContainAllNodeScan: Boolean = false,
    parameters: Map[String, cypher.Value] = Map.empty,
    ordered: Boolean = true,
    skip: Boolean = false,
  )(implicit
    queryHandler: RunnableCypher[T],
    pos: Position,
  ): Unit = {
    def theTest(): Future[Assertion] = {
      val queryResults = queryHandler.run(query, parameters)
      assert(expectedColumns.map(Symbol(_)) === queryResults.columns, "columns must match")
      val (killSwitch, rowsFut) = queryResults.results
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      // Schedule cancellation for the query if it takes too long
      materializer.scheduleOnce(
        timeout.duration,
        () => killSwitch.abort(new java.util.concurrent.TimeoutException()),
      )

      rowsFut map { actualRows =>
        if (ordered)
          assert(actualRows === expectedRows, "ordered rows must match")
        else
          assert(HashSet(actualRows: _*) == HashSet(expectedRows: _*), "unordered rows must match")

        assert(Plan.fromQuery(queryResults.compiled.query).toValue.isPure, "query plan can be rendered")
        assert(queryResults.compiled.query.isReadOnly == expectedIsReadOnly, "isReadOnly must match")
        assert(queryResults.compiled.query.cannotFail == expectedCannotFail, "cannotFail must match")
        assert(queryResults.compiled.query.isIdempotent == expectedIsIdempotent, "isIdempotent must match")
        assert(
          queryResults.compiled.query.canContainAllNodeScan == expectedCanContainAllNodeScan,
          "canContainAllNodeScan must match",
        )
      }
    }

    if (skip)
      ignore(queryHandler.testName(query))(theTest())(pos)
    else
      it(queryHandler.testName(query))(theTest())(pos)
  }

  /** Check that a given expression matches an expected output
    *
    * @param expressionText expression whose output we are checking
    * @param expectedValue the expected output value
    * @param expectedIsReadOnly should the expression be readonly?
    * @param expectedCannotFail should the expression be never throw an exception?
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
    expectedCannotFail: Boolean = false,
    expectedIsIdempotent: Boolean = true,
    expectedCanContainAllNodeScan: Boolean = false,
    skip: Boolean = false,
    queryPreamble: String = "RETURN ",
  )(implicit
    pos: Position,
  ): Unit =
    testQuery(
      query = queryPreamble + expressionText,
      expectedColumns = Vector(expressionText),
      expectedRows = Seq(Vector(expectedValue)),
      expectedIsReadOnly = expectedIsReadOnly,
      expectedCannotFail = expectedCannotFail,
      expectedIsIdempotent = expectedIsIdempotent,
      expectedCanContainAllNodeScan = expectedCanContainAllNodeScan,
      skip = skip,
    )

  /** Check that a given query fails to be constructed with the given error.
    *
    * @param queryText query whose output we are checking
    * @param expected exception that we expect to intercept
    * @param pos source position of the call to `interceptQuery`
    */
  final def assertStaticQueryFailure[E <: Throwable: ClassTag](queryText: String, expectedError: E)(implicit
    pos: Position,
  ): Unit = {
    def theTest(): Assertion = {
      val actual = intercept[E](queryCypherValues(queryText, cypherHarnessNamespace, cacheCompilation = false)(graph))
      assert(actual.getMessage == expectedError.getMessage, "Query construction did not fail with expected error")
    }
    it(queryText)(theTest())
  }

  /** Check that a given query fails at runtime with the given error.
    *
    * @param queryText query whose output we are checking
    * @param expected exception that we expect to intercept
    * @param pos source position of the call to `interceptQuery`
    */
  final def assertQueryExecutionFailure[E <: Throwable: ClassTag](
    queryText: String,
    expected: E,
  )(implicit
    pos: Position,
  ): Unit = {
    def theTest(): Future[Assertion] = recoverToExceptionIf[E](
      queryCypherValues(queryText, cypherHarnessNamespace)(graph).results.runWith(Sink.ignore),
    ) map (actual =>
      assert(actual.getMessage == expected.getMessage, "Query execution did not fail with expected error"),
    )

    it(queryText)(theTest())(pos)
  }

  /** Check query static analysis output.
    *
    * @param queryText query whose output we are checking
    * @param expectedIsReadOnly
    * @param expectedCannotFail
    * @param expectedIsIdempotent
    * @param expectedCanContainAllNodeScan
    */
  final def testQueryStaticAnalysis(
    queryText: String,
    expectedIsReadOnly: Boolean,
    expectedCannotFail: Boolean,
    expectedIsIdempotent: Boolean,
    expectedCanContainAllNodeScan: Boolean,
    skip: Boolean = false,
  )(implicit
    pos: Position,
  ): Unit = {
    def theTest(): Future[Assertion] = {
      val CompiledQuery(_, query, _, _, _) = compile(queryText)
      assert(query.isReadOnly == expectedIsReadOnly, "isReadOnly must match")
      assert(query.cannotFail == expectedCannotFail, "cannotFail must match")
      assert(query.isIdempotent == expectedIsIdempotent, "isIdempotent must match")
      assert(
        query.canContainAllNodeScan == expectedCanContainAllNodeScan,
        "canContainAllNodeScan must match",
      )
    }

    if (skip)
      ignore(queryText)(theTest())(pos)
    else
      it(queryText)(theTest())(pos)
  }
}
