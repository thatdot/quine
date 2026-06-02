package com.thatdot.quine.app

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.api.v2.ResourceName
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{FileAccessPolicy, ResolutionMode}
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.StandingQueryDefinition
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.graph.InvalidQueryPattern.{DistinctIdMustDistinct, MultipleValuesCantDistinct}
import com.thatdot.quine.graph.{GraphService, NamespaceId, defaultNamespaceId}

/** Pattern-validation tests for [[QuineApp.addStandingQueryV2]] — confirms that
  * malformed standing query definitions fail the returned Future with the
  * corresponding [[com.thatdot.quine.graph.InvalidQueryPattern]].
  */
class QuineAppStandingQueryTest extends AnyFunSuite with BeforeAndAfterAll with ScalaFutures {

  implicit val logConfig: LogConfig = LogConfig.permissive

  private val graph: GraphService = IngestTestGraph.makeGraph("standing-query-validation-test")

  private val quineApp = new QuineApp(
    graph,
    helpMakeQuineBetter = false,
    FileAccessPolicy(List.empty, ResolutionMode.Dynamic),
    new LocalTapBus,
  )

  private val namespace: NamespaceId = defaultNamespaceId

  implicit val ec: ExecutionContext = graph.nodeDispatcherEC

  override def beforeAll(): Unit =
    while (!graph.isReady) Thread.sleep(10)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  test("DistinctId standing query without DISTINCT fails with DistinctIdMustDistinct") {
    val sq = StandingQueryDefinition(
      name = ResourceName.unsafeFromString("missing-distinct"),
      pattern = StandingQueryPattern.Cypher(
        query = "MATCH (n) RETURN id(n)",
        mode = StandingQueryMode.DistinctId,
      ),
    )
    val failure = quineApp.addStandingQueryV2("missing-distinct", namespace, sq).failed.futureValue
    assert(failure == DistinctIdMustDistinct)
  }

  test("MultipleValues standing query with DISTINCT fails with MultipleValuesCantDistinct") {
    val sq = StandingQueryDefinition(
      name = ResourceName.unsafeFromString("extra-distinct"),
      pattern = StandingQueryPattern.Cypher(
        query = "MATCH (n) RETURN DISTINCT id(n)",
        mode = StandingQueryMode.MultipleValues,
      ),
    )
    val failure = quineApp.addStandingQueryV2("extra-distinct", namespace, sq).failed.futureValue
    assert(failure == MultipleValuesCantDistinct)
  }
}
