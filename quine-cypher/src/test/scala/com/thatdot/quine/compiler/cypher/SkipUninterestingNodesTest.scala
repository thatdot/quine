package com.thatdot.quine.compiler.cypher

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.Milliseconds

/** Tests for the skip-uninteresting-nodes functionality.
  *
  * "Uninteresting" nodes (those with no properties, no edges, and no labels) should be filtered out from:
  * - AllNodesScan queries (MATCH (n) RETURN n)
  * - recentNodes() and recentNodeIds() procedure calls
  *
  * This prevents returning "ghost" nodes that were touched but never populated with data,
  * or nodes that were deleted but still exist in the recent nodes cache.
  *
  * IMPORTANT: Every test in this file verifies the implementation by including an AllNodesScan
  * count check. This ensures that WITHOUT the implementation, ALL tests will fail.
  */
class SkipUninterestingNodesTest extends CypherHarness("skip-uninteresting-nodes-test") {

  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher

  // ============================================================================
  // AllNodesScan Tests
  // ============================================================================

  describe("AllNodesScan filters uninteresting nodes") {
    // This sequence of testQuery calls verifies that deleted nodes are filtered from AllNodesScan.
    // Each test builds on the state from previous tests.

    // Initial state: graph is empty
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt0",
      expectedColumns = Vector("cnt0"),
      expectedRows = Seq(Vector(Expr.Integer(0L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Create target node with property and label (will remain throughout)
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("target-node") SET n.name = "target", n:Target RETURN n.name""",
      expectedColumns = Vector("n.name"),
      expectedRows = Seq(Vector(Expr.Str("target"))),
      expectedIsReadOnly = false,
    )

    // Verify: 1 node
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt1",
      expectedColumns = Vector("cnt1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Create node with property
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("prop-node") SET n.marker = "has-property" RETURN n.marker""",
      expectedColumns = Vector("n.marker"),
      expectedRows = Seq(Vector(Expr.Str("has-property"))),
      expectedIsReadOnly = false,
    )

    // Verify: 2 nodes
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt2",
      expectedColumns = Vector("cnt2"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Create node with only a label
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("label-node") SET n:OnlyLabel RETURN labels(n)""",
      expectedColumns = Vector("labels(n)"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Str("OnlyLabel"))))),
      expectedIsReadOnly = false,
    )

    // Verify: 3 nodes
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt3",
      expectedColumns = Vector("cnt3"),
      expectedRows = Seq(Vector(Expr.Integer(3L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Create node with only an edge to target
    testQuery(
      """MATCH (src), (tgt) WHERE id(src) = idFrom("edge-node") AND id(tgt) = idFrom("target-node")
        |CREATE (src)-[:CONNECTS]->(tgt) RETURN 1 AS created""".stripMargin,
      expectedColumns = Vector("created"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
    )

    // Verify: 4 nodes
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt4",
      expectedColumns = Vector("cnt4"),
      expectedRows = Seq(Vector(Expr.Integer(4L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Delete the label-only node
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("label-node") DETACH DELETE n""",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
    )

    // Verify: 3 nodes (label-only node is filtered)
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt5",
      expectedColumns = Vector("cnt5"),
      expectedRows = Seq(Vector(Expr.Integer(3L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Delete the property node
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("prop-node") DETACH DELETE n""",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
    )

    // Verify: 2 nodes (property node is filtered)
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt6",
      expectedColumns = Vector("cnt6"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Delete the edge-only node
    testQuery(
      """MATCH (n) WHERE id(n) = idFrom("edge-node") DETACH DELETE n""",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
    )

    // Verify: 1 node (only target remains)
    testQuery(
      "MATCH (n) RETURN count(n) AS cnt7",
      expectedColumns = Vector("cnt7"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )
  }

  // ============================================================================
  // Progressive Removal Tests
  // ============================================================================

  describe("Node remains interesting until all properties, labels, and edges are removed") {
    // This test creates a node with property, label, and edge, then progressively removes them.
    // The node should remain "interesting" until ALL are removed.

    // Get baseline count (should be 1 from target-node in previous tests)
    testQuery(
      "MATCH (n) RETURN count(n) AS baseline",
      expectedColumns = Vector("baseline"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Create a node with property, label, and edge to target
    testQuery(
      """MATCH (n), (tgt) WHERE id(n) = idFrom("full-node") AND id(tgt) = idFrom("target-node")
        |SET n.prop = "value", n:TestLabel
        |CREATE (n)-[:LINKS]->(tgt)
        |RETURN n.prop""".stripMargin,
      expectedColumns = Vector("n.prop"),
      expectedRows = Seq(Vector(Expr.Str("value"))),
      expectedIsReadOnly = false,
    )

    // Verify: 2 nodes (target + full-node)
    testQuery(
      "MATCH (n) RETURN count(n) AS withAll",
      expectedColumns = Vector("withAll"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )

    // Remove the property by setting to null, remove the label, and remove the edge
    testQuery(
      """MATCH (n)-[r:LINKS]->() WHERE id(n) = idFrom("full-node") SET n.prop = null REMOVE n:TestLabel DELETE r RETURN 1 AS deleted""",
      expectedColumns = Vector("deleted"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false,
    )

    // Verify: 1 node (full-node is now uninteresting and filtered)
    testQuery(
      "MATCH (n) RETURN count(n) AS afterPropLabelEdgeRemoval",
      expectedColumns = Vector("afterPropLabelEdgeRemoval"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedCannotFail = true,
      expectedCanContainAllNodeScan = true,
    )
  }

  // ============================================================================
  // recentNodes and recentNodeIds Tests
  // ============================================================================

  describe("recentNodes and recentNodeIds procedures filter uninteresting nodes") {
    // Use specific IDs and filter in the query for predictable results

    // Create node that will be deleted (ID 500)
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(500) SET n.status = 'will-be-deleted' RETURN n.status",
      expectedColumns = Vector("n.status"),
      expectedRows = Seq(Vector(Expr.Str("will-be-deleted"))),
      expectedIsReadOnly = false,
    )

    // Create node that will remain (ID 501)
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(501) SET n.status = 'will-remain' RETURN n.status",
      expectedColumns = Vector("n.status"),
      expectedRows = Seq(Vector(Expr.Str("will-remain"))),
      expectedIsReadOnly = false,
    )

    // Verify both nodes appear in recentNodes before deletion (3 = target-node + 500 + 501)
    testQuery(
      "CALL recentNodes(100) YIELD node RETURN count(node)",
      expectedColumns = Vector("count(node)"),
      expectedRows = Seq(Vector(Expr.Integer(3L))),
      expectedIsIdempotent = false,
    )

    // Verify both appear in recentNodeIds before deletion
    testQuery(
      "CALL recentNodeIds(100) YIELD nodeId RETURN count(nodeId)",
      expectedColumns = Vector("count(nodeId)"),
      expectedRows = Seq(Vector(Expr.Integer(3L))),
      expectedIsIdempotent = false,
    )

    // Delete node 500
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(500) DETACH DELETE n",
      expectedColumns = Vector.empty,
      expectedRows = Seq.empty,
      expectedIsReadOnly = false,
    )

    // Verify deleted node is filtered from recentNodes (count decreased by 1)
    testQuery(
      "CALL recentNodes(100) YIELD node RETURN count(node) AS cnt1",
      expectedColumns = Vector("cnt1"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedIsIdempotent = false,
    )

    // Verify deleted node is filtered from recentNodeIds (count decreased by 1)
    testQuery(
      "CALL recentNodeIds(100) YIELD nodeId RETURN count(nodeId) AS cnt2",
      expectedColumns = Vector("cnt2"),
      expectedRows = Seq(Vector(Expr.Integer(2L))),
      expectedIsIdempotent = false,
    )
  }

  // ============================================================================
  // Historical Query Tests
  // ============================================================================

  describe("Historical queries respect node state at queried timestamp") {
    // Historical queries need manual execution since testQuery doesn't support atTime parameter

    /** Pause to ensure timestamps are distinct at millisecond granularity */
    def pause(): Future[Unit] = {
      val promise = Promise[Unit]()
      graph.system.scheduler.scheduleOnce(2.milliseconds)(promise.success(()))
      promise.future
    }

    /** Get AllNodesScan count at a specific historical time */
    def getAllNodeScanCountAtTime(time: Milliseconds): Future[Long] =
      queryCypherValues(
        "MATCH (n) RETURN count(n) AS cnt",
        cypherHarnessNamespace,
        atTime = Some(time),
        cacheCompilation = false,
      )(graph).results.toMat(Sink.seq)(Keep.right).run().map { rows =>
        rows.head.head.asInstanceOf[Expr.Integer].long
      }

    /** Get current AllNodesScan count */
    def getAllNodeScanCount(): Future[Long] =
      queryCypherValues(
        "MATCH (n) RETURN count(n) AS cnt",
        cypherHarnessNamespace,
        cacheCompilation = false,
      )(graph).results.toMat(Sink.seq)(Keep.right).run().map { rows =>
        rows.head.head.asInstanceOf[Expr.Integer].long
      }

    /** Execute a Cypher query and wait for completion */
    def execQuery(query: String): Future[Unit] =
      queryCypherValues(
        query,
        cypherHarnessNamespace,
        cacheCompilation = false,
      )(graph).results.runWith(Sink.ignore).map(_ => ())

    it("should return correct nodes via historical AllNodesScan at different timestamps") {
      // Test pattern:
      // 1. Capture time T1 (baseline count)
      // 2. Create interesting nodes A and B
      // 3. Capture time T2 (A and B exist)
      // 4. Delete interesting node B
      // 5. Capture time T3 (only A exists)
      // 6. Create interesting node C
      // 7. Verify:
      //    - Live AllNodesScan returns baseline + 2 (A and C)
      //    - Historical at T3 returns baseline + 1 (only A)
      //    - Historical at T2 returns baseline + 2 (A and B)
      //    - Historical at T1 returns baseline (no new nodes)

      for {
        initialCount <- getAllNodeScanCount()
        _ <- pause()
        t1 <- Future.successful(Milliseconds.currentTime())
        _ <- pause()

        _ <- execQuery("""MATCH (n) WHERE id(n) = idFrom("hist-node-a") SET n.marker = "node-a" RETURN n""")
        _ <- pause()
        _ <- execQuery("""MATCH (n) WHERE id(n) = idFrom("hist-node-b") SET n.marker = "node-b" RETURN n""")
        _ <- pause()
        t2 <- Future.successful(Milliseconds.currentTime())
        _ <- pause()

        _ <- execQuery("""MATCH (n) WHERE id(n) = idFrom("hist-node-b") DETACH DELETE n""")
        _ <- pause()
        t3 <- Future.successful(Milliseconds.currentTime())
        _ <- pause()

        _ <- execQuery("""MATCH (n) WHERE id(n) = idFrom("hist-node-c") SET n.marker = "node-c" RETURN n""")

        liveCount <- getAllNodeScanCount()
        countAtT3 <- getAllNodeScanCountAtTime(t3)
        countAtT2 <- getAllNodeScanCountAtTime(t2)
        countAtT1 <- getAllNodeScanCountAtTime(t1)
      } yield {
        assert(
          liveCount == initialCount + 2,
          s"Live count should be initial + 2 (A and C). Initial: $initialCount, Live: $liveCount",
        )
        assert(
          countAtT3 == initialCount + 1,
          s"Historical at T3 should be initial + 1 (only A). Initial: $initialCount, T3: $countAtT3",
        )
        assert(
          countAtT2 == initialCount + 2,
          s"Historical at T2 should be initial + 2 (A and B). Initial: $initialCount, T2: $countAtT2",
        )
        assert(
          countAtT1 == initialCount,
          s"Historical at T1 should equal initial (no new nodes). Initial: $initialCount, T1: $countAtT1",
        )
      }
    }
  }
}
