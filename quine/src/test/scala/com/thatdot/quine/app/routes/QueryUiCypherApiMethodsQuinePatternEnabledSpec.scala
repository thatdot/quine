package com.thatdot.quine.app.routes

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.IngestTestGraph
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.model.{Milliseconds, QuineValue}
import com.thatdot.quine.routes.CypherQuery

class QueryUiCypherApiMethodsQuinePatternEnabledSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var originalQpEnabled: Option[String] = None

  override def beforeAll(): Unit = {
    originalQpEnabled = Option(System.getProperty("qp.enabled"))
    System.setProperty("qp.enabled", "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    originalQpEnabled match {
      case Some(value) => System.setProperty("qp.enabled", value)
      case None => System.clearProperty("qp.enabled")
    }
    super.afterAll()
  }

  "queryCypherGeneric" should "pass atTime through to QuinePattern execution" in {
    val graph = IngestTestGraph.makeGraph("api-generic-attime-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      implicit val logConfig: LogConfig = LogConfig.permissive
      implicit val mat: Materializer = Materializer(graph.system)
      implicit val timeout: Timeout = Timeout(5.seconds)
      val apiMethods = new OSSQueryUiCypherMethods(graph)
      val namespace = defaultNamespaceId

      val nodeId = graph.idProvider.newQid()

      val t1PropValue = 1L
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "prop", QuineValue.Integer(t1PropValue)),
        5.seconds,
      )
      val t1 = Milliseconds.currentTime()
      Thread.sleep(3)

      val currentPropValue = 2L
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "prop", QuineValue.Integer(currentPropValue)),
        5.seconds,
      )

      val nodeIdStr = graph.idProvider.qidToPrettyString(nodeId)
      val query = CypherQuery(s"""MATCH (n) WHERE strId(n) = "$nodeIdStr" RETURN n.prop AS value""", Map.empty)

      val (_, currentResultsSource, _, _) = apiMethods.queryCypherGeneric(query, namespace, atTime = None)
      val currentResults = Await.result(currentResultsSource.runWith(Sink.seq), 10.seconds)
      currentResults.head.head.asNumber.flatMap(_.toLong) shouldBe Some(currentPropValue)

      val (_, historicalResultsSource, _, _) = apiMethods.queryCypherGeneric(query, namespace, atTime = Some(t1))
      val historicalResults = Await.result(historicalResultsSource.runWith(Sink.seq), 10.seconds)
      historicalResults.head.head.asNumber.flatMap(_.toLong) shouldBe Some(t1PropValue)
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  "queryCypherNodes" should "pass atTime through to QuinePattern execution" in {
    val graph = IngestTestGraph.makeGraph("api-nodes-attime-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      implicit val logConfig: LogConfig = LogConfig.permissive
      implicit val mat: Materializer = Materializer(graph.system)
      implicit val timeout: Timeout = Timeout(5.seconds)
      val apiMethods = new OSSQueryUiCypherMethods(graph)
      val namespace = defaultNamespaceId

      val nodeId = graph.idProvider.newQid()

      val t1PropValue = 1L
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "prop", QuineValue.Integer(t1PropValue)),
        5.seconds,
      )
      val t1 = Milliseconds.currentTime()
      Thread.sleep(3)

      val currentPropValue = 2L
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "prop", QuineValue.Integer(currentPropValue)),
        5.seconds,
      )

      val nodeIdStr = graph.idProvider.qidToPrettyString(nodeId)
      val query = CypherQuery(s"""MATCH (n) WHERE strId(n) = "$nodeIdStr" RETURN n""", Map.empty)

      val (currentNodesSource, _, _) = apiMethods.queryCypherNodes(query, namespace, atTime = None)
      val currentNodes = Await.result(currentNodesSource.runWith(Sink.seq), 10.seconds)
      currentNodes.head.properties.get("prop").flatMap(_.asNumber).flatMap(_.toLong) shouldBe Some(2L)

      val (historicalNodesSource, _, _) = apiMethods.queryCypherNodes(query, namespace, atTime = Some(t1))
      val historicalNodes = Await.result(historicalNodesSource.runWith(Sink.seq), 10.seconds)
      historicalNodes.head.properties.get("prop").flatMap(_.asNumber).flatMap(_.toLong) shouldBe Some(1L)
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // QuinePattern does not currently bind relationship variables to values (returns null).
  // Edge traversal works, but `RETURN r` where r is a relationship variable returns null.
  // This test is pending until QuinePattern supports relationship variable binding.
  "queryCypherEdges" should "pass atTime through to QuinePattern execution" in pendingUntilFixed {
    val graph = IngestTestGraph.makeGraph("api-edges-attime-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      implicit val logConfig: LogConfig = LogConfig.permissive
      implicit val mat: Materializer = Materializer(graph.system)
      implicit val timeout: Timeout = Timeout(5.seconds)
      val apiMethods = new OSSQueryUiCypherMethods(graph)
      val namespace = defaultNamespaceId

      val nodeA = graph.idProvider.newQid()
      val nodeB = graph.idProvider.newQid()

      // t0: before edge exists
      val t0 = Milliseconds.currentTime()
      Thread.sleep(3)

      Await.result(
        graph.literalOps(namespace).addEdge(nodeA, nodeB, "KNOWS"),
        5.seconds,
      )
      Thread.sleep(3)

      val nodeAStr = graph.idProvider.qidToPrettyString(nodeA)
      val nodeBStr = graph.idProvider.qidToPrettyString(nodeB)
      val query = CypherQuery(
        s"""MATCH (a)-[r:KNOWS]->(b) WHERE strId(a) = "$nodeAStr" AND strId(b) = "$nodeBStr" RETURN r""",
        Map.empty,
      )

      val (currentEdgesSource, _, _) = apiMethods.queryCypherEdges(query, namespace, atTime = None)
      val currentEdges = Await.result(currentEdgesSource.runWith(Sink.seq), 10.seconds)
      currentEdges.head.edgeType shouldBe "KNOWS"

      val (historicalEdgesSource, _, _) = apiMethods.queryCypherEdges(query, namespace, atTime = Some(t0))
      val historicalEdges = Await.result(historicalEdgesSource.runWith(Sink.seq), 10.seconds)
      val _ = historicalEdges shouldBe empty
    } finally Await.result(graph.shutdown(), 5.seconds)
  }
}
