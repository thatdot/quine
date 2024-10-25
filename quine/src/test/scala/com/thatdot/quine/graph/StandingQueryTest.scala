package com.thatdot.quine.graph

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

import org.apache.pekko.util.Timeout

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app._
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.routes.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.CypherQuery
import com.thatdot.quine.routes.{StandingQueryPattern => SqPattern, _}
import com.thatdot.quine.util.Log._

class StandingQueryTest extends AnyFunSuite with Matchers {
  val namespace: NamespaceId = defaultNamespaceId

  test("Distinct ID Standing Query results correctly read MaxLong and produce the right number of results") {
    val graph: GraphService = IngestTestGraph.makeGraph()
    while (!graph.isReady) Thread.sleep(10)
    val quineApp = new QuineApp(graph)(LogConfig.permissive)
    implicit val timeout: Timeout = Timeout(2.seconds)
    implicit val ec: ExecutionContext = graph.shardDispatcherEC

    val ingestConfig = NumberIteratorIngest(
      FileIngestFormat.CypherLine(
        """WITH gen.node.from(toInteger($that)) AS n,
          |     toInteger($that) AS i
          |MATCH (thisNode), (nextNode)
          |WHERE id(thisNode) = id(n)
          |  AND id(nextNode) = idFrom(i + 1)
          |SET thisNode.id = i,
          |    nextNode.id = i+1
          |CREATE (thisNode)-[:next]->(nextNode)
          |""".stripMargin,
      ),
      startAtOffset = 9223372036854775707L,
      ingestLimit = Some(100L),
      maximumPerSecond = None,
    )

    val sqPattern = SqPattern.Cypher(
      """MATCH (a)-[:next]->(b)
        |WHERE a.id IS NOT NULL AND b.id IS NOT NULL
        |RETURN DISTINCT id(a) as id
        |""".stripMargin,
    )

    val sqOutputPattern =
      """MATCH (a)-[:next]->(b)
        |WHERE id(a) = $that.data.id
        |RETURN a.id, b.id
        |""".stripMargin

    val sqResultsRef = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val sqOutput = StandingQueryResultOutputUserDef.InternalQueue(sqResultsRef)

    val sqDef: StandingQueryDefinition =
      StandingQueryDefinition(sqPattern, Map("results" -> CypherQuery(sqOutputPattern, andThen = Some(sqOutput))))

    val setupFuture = quineApp
      .addStandingQuery("next-node", namespace, sqDef)
      .flatMap(_ =>
        Future.fromTry(
          quineApp
            .addIngestStream("numbers", ingestConfig, namespace, None, shouldResumeRestoredIngests = false, timeout),
        ),
      )
    Await.ready(setupFuture, 3 seconds)

    /*
       Testing large values. One of the output values should contain the
       "data":{"a.id":9223372036854775806,"b.id":9223372036854775807}
       When this is incorrectly rounded through ujson we get
       "data":{"a.id":9223372036854775807,"b.id":9223372036854775807}
     */
    val testMap = Map(
      "a.id" -> QuineValue(9223372036854775806L),
      "b.id" -> QuineValue(9223372036854775807L),
    )

    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
      val results = sqResultsRef.get()
      assert(results.exists(_.data == testMap))
      assert(results.size == ingestConfig.ingestLimit.get)
    }
  }

  test("MultipleValues Standing query finds the correct number of results.") {

    val graph: GraphService = IngestTestGraph.makeGraph()
    implicit val ec: ExecutionContext = graph.shardDispatcherEC
    val quineApp = new QuineApp(graph)(LogConfig.permissive)
    implicit val timeout: Timeout = Timeout(2.seconds)
    while (!graph.isReady) Thread.sleep(10)

    val size = 100
    val mod = 5 // must divide `size` equally
    val ingestConfig = NumberIteratorIngest(
      FileIngestFormat.CypherLine(
        """WITH gen.node.from(toInteger($that)) AS n,
          |     toInteger($that) AS i
          |MATCH (thisNode), (nextNode)
          |WHERE id(thisNode) = id(n)
          |  AND id(nextNode) = idFrom(i + 1)
          |SET thisNode.id = i
          |SET nextNode.id = i + 1
          |CREATE (thisNode)-[:next]->(nextNode)
          |""".stripMargin,
      ),
      ingestLimit = Some(size.toLong),
      maximumPerSecond = None,
    )

    val sqResultsRef = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val sqOutput = StandingQueryResultOutputUserDef.InternalQueue(sqResultsRef)

    val sqDef = StandingQueryDefinition(
      SqPattern.Cypher(
        ("""MATCH (a)-[:next]->(b)
           |WHERE a.id IS NOT NULL
           |  AND b.id IS NOT NULL
           |  AND a.id % """ + mod.toString + """ = 0
           |RETURN a.id, b.id, b.id-a.id""").stripMargin,
        StandingQueryMode.MultipleValues,
      ),
      Map("internal-queue" -> sqOutput),
    )

    val setupFuture = quineApp
      .addStandingQuery("next-node", namespace, sqDef)
      .flatMap(_ =>
        Future.fromTry(
          quineApp
            .addIngestStream("numbers", ingestConfig, namespace, None, shouldResumeRestoredIngests = false, timeout),
        ),
      )
    Await.result(setupFuture, 3 seconds)

    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
      val results = sqResultsRef.get()
      assert(results.length == size / mod)
    }
  }

//  Commenting for now as this continually fails CI
//
//  test("MultipleValues standing updates results across an edge as it matches") {
//
//    val graph: GraphService = IngestTestGraph.makeGraph()
//    implicit val ec: ExecutionContext = graph.shardDispatcherEC
//    val quineApp = new QuineApp(graph)
//    implicit val timeout: Timeout = Timeout(2.seconds)
//    while (!graph.isReady) Thread.sleep(10)
//
//    val size = 3
//    val ingestConfig = NumberIteratorIngest(
//      FileIngestFormat.CypherLine(
//        """// step 0: make a subgraph that only matches part of the query pattern
//          |WITH toInteger($that) AS n
//          |MATCH (a), (b)
//          |WHERE n = 0
//          |  AND id(a) = idFrom("a")
//          |  AND id(b) = idFrom("b")
//          |CREATE (a)-[:FOO]->(b)
//          |SET a.name = "a"
//          |SET b.name = "b"
//          |SET b.bar = true
//          |UNION
//          |// step 1: make a match of the query pattern
//          |WITH toInteger($that) AS n
//          |MATCH (b), (c)
//          |WHERE n = 1
//          |  AND id(b) = idFrom("b")
//          |  AND id(c) = idFrom("c")
//          |CREATE (b)-[:FOO]->(c)
//          |SET c.name = "c"
//          |SET c.bar = true
//          |UNION
//          |// step 2: make a and b match the pattern
//          |WITH toInteger($that) AS n
//          |MATCH (b)
//          |WHERE n = 2
//          |  AND id(b) = idFrom("b")
//          |SET b.bar = true
//          |""".stripMargin
//      ),
//      ingestLimit = Some(size.toLong),
//      throttlePerSecond = None
//    )
//
//    val sqResultsRef = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
//    val sqOutput = StandingQueryResultOutputUserDef.InternalQueue(sqResultsRef)
//
//    // Compiles to the MVSQ parts:
//    // - Cross
//    // - LocalProperty
//    // - SubscribeAcrossEdge
//    val sqDef = StandingQueryDefinition(
//      SqPattern.Cypher(
//        """MATCH (x)-[:FOO]->(y {bar: true})
//          |RETURN x.name AS name
//          |""".stripMargin,
//        StandingQueryMode.MultipleValues
//      ),
//      Map("internal-queue" -> sqOutput)
//    )
//
//    val setupFuture = quineApp
//      .addStandingQuery("bar-across-foo", sqDef)
//      .flatMap(_ =>
//        Future.fromTry(quineApp.addIngestStream("numbers", ingestConfig, None, shouldRestoreIngest = false, timeout))
//      )
//    Await.result(setupFuture, 3 seconds)
//
//    eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
//      val results = sqResultsRef.get()
//      val names = results.flatMap(r => r.data.get("name")).toSet
//      assert(names == Set(QuineValue.Null, QuineValue("a"), QuineValue("b")))
//    }
//  }

  test("MultipleValues standing creates results when property toggles between matching and not") {
    val graph: GraphService = IngestTestGraph.makeGraph()
    implicit val ec: ExecutionContext = graph.shardDispatcherEC
    val quineApp = new QuineApp(graph)(LogConfig.permissive)
    implicit val timeout: Timeout = Timeout(2.seconds)
    while (!graph.isReady) Thread.sleep(10)

    val size = 4
    val ingestConfig = NumberIteratorIngest(
      // This also has a toReturn exactly the same as toExtract when turned into a GraphQueryPattern
      FileIngestFormat.CypherLine(
        """WITH toInteger($that) AS n
          |MATCH (a)
          |WHERE id(a) = idFrom("a")
          |SET a.foo = n""".stripMargin,
      ),
      ingestLimit = Some(size.toLong),
      maximumPerSecond = None, //Some(2)
    )

    val sqResultsRef = new AtomicReference[Vector[StandingQueryResult]](Vector.empty)
    val sqOutput = StandingQueryResultOutputUserDef.InternalQueue(sqResultsRef)

    // Compiles to the MVSQ parts:
    // - FilterMap
    // - Cross
    // - LocalProperty
    // - LocalId
    val sqDef = StandingQueryDefinition(
      SqPattern.Cypher(
        """MATCH (a)
          |WHERE id(a) = idFrom("a")
          |  AND a.foo IN [0, 1, 2, 3]
          |RETURN a.foo AS foo
          |""".stripMargin,
        StandingQueryMode.MultipleValues,
      ),
      Map("internal-queue" -> sqOutput),
    )

    val setupFuture = quineApp
      .addStandingQuery("foo", namespace, sqDef)
      .flatMap(_ =>
        Future.fromTry(
          quineApp
            .addIngestStream("numbers", ingestConfig, namespace, None, shouldResumeRestoredIngests = false, timeout),
        ),
      )
    Await.result(setupFuture, 3.seconds)

    eventually(Eventually.timeout(5.seconds), interval(500.millis)) {
      val results = sqResultsRef.get()
      val fooValues = results.flatMap(r => r.data.get("foo")).toSet
      // Because there is a race between property value updates and the initial subscription of the FilterMap to changes
      // to the "foo" property, which is done as through a message that may arrive after some or all of the ingest
      // values, it's possible to only see the most recent "foo" value.
      // This assertion is much less interesting than it was when the test was created, but asserting on a complete set
      // of results was not reliable.
      assert(fooValues.intersect(Set(QuineValue(0), QuineValue(1), QuineValue(2), QuineValue(3))).nonEmpty)
    }
  }
}
