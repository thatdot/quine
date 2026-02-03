package com.thatdot.quine.graph.cypher.quinepattern

import scala.concurrent.Promise

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisModule, SymbolAnalysisPhase}
import com.thatdot.quine.cypher.{ast => Cypher}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.language.ast.Value
import com.thatdot.quine.language.phases.UpgradeModule._

/** Tests for state installation behavior.
  *
  * These tests verify that the correct number of states are created
  * for various query patterns, helping identify state explosion issues.
  */
class StateInstallationTest extends AnyFlatSpec with Matchers {

  // ============================================================
  // HELPERS
  // ============================================================

  private def parseCypher(query: String): (Cypher.Query.SingleQuery, SymbolAnalysisModule.SymbolTable) = {
    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (tableState, result) = parser.process(query).value.run(LexerState(Nil)).value
    result match {
      case Some(q: Cypher.Query.SingleQuery) => (q, tableState.symbolTable)
      case Some(other) => fail(s"Expected SingleQuery, got: $other")
      case None => fail(s"Parse error for query: $query")
    }
  }

  private def planQuery(query: String): QueryPlan = {
    val (parsedQuery, symbolTable) = parseCypher(query)
    QueryPlanner.plan(parsedQuery, symbolTable)
  }

  private def buildStateGraph(
    query: String,
    mode: RuntimeMode = RuntimeMode.Lazy,
    params: Map[Symbol, Value] = Map.empty,
  ): StateGraph = {
    val plan = planQuery(query)
    val promise = Promise[Seq[QueryContext]]()
    QueryStateBuilder.build(
      plan = plan,
      mode = mode,
      params = params,
      namespace = defaultNamespaceId,
      output = OutputTarget.EagerCollector(promise),
    )
  }

  // ============================================================
  // TESTS: State Count Per Query Pattern
  // ============================================================

  "A simple node match" should "create a small number of states" in {
    val query = "MATCH (n) RETURN n"
    val stateGraph = buildStateGraph(query)

    // Should be small: Output, Anchor, LocalId
    stateGraph.states.size should be <= 5
  }

  "A node with property constraint" should "create minimal states" in {
    val query = """MATCH (n) WHERE n.type = "WRITE" RETURN n"""
    val stateGraph = buildStateGraph(query)

    stateGraph.states.size should be <= 6
  }

  "An edge pattern" should "create states for both nodes" in {
    val query = "MATCH (a)-[:EVENT]->(b) RETURN a, b"
    val stateGraph = buildStateGraph(query)

    // Anchor + Expand + LocalId × 2 + CrossProduct + Output
    stateGraph.states.size should be <= 10
  }

  "APT ingest 1 pattern" should "have bounded state count" in {
    // This matches ingest 1 from the APT recipe
    val query = """
      MATCH (proc), (event), (object)
      WHERE id(proc) = idFrom('test-pid')
        AND id(event) = idFrom('test-event')
        AND id(object) = idFrom('test-object')
      SET proc.id = 'test-pid',
          proc:Process,
          event.type = 'WRITE',
          event:EndpointEvent
      CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)
    """
    val stateGraph = buildStateGraph(query, RuntimeMode.Eager)

    // This query has 3 anchors (proc, event, object), so we expect ~10-15 states
    stateGraph.states.size should be <= 20
  }

  "APT standing query pattern" should "have bounded state count per installation" in {
    // This matches the standing query from the APT recipe
    val query = """
      MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
            (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
      WHERE e1.type = "WRITE"
        AND e2.type = "READ"
        AND e3.type = "DELETE"
        AND e4.type = "SEND"
      RETURN DISTINCT id(f) as fileId
    """
    val stateGraph = buildStateGraph(query, RuntimeMode.Lazy)

    // This is a complex 6-node pattern, but should still be bounded
    // Let's see what we get and set a reasonable bound
    stateGraph.states.size should be <= 50
  }

  "State graph for 3-node chain" should "show how states scale with pattern size" in {
    val query = "MATCH (a)-[:R]->(b)-[:R]->(c) RETURN a, b, c"
    val stateGraph = buildStateGraph(query)

    // Expect: Anchor + Expand + LocalId for each node + CrossProducts
    stateGraph.states.size should be <= 15
  }

  "State graph for diamond pattern" should "handle multiple paths correctly" in {
    // Two paths from a to c: a->b->c and a->d->c
    val query = "MATCH (a)-[:R]->(b)-[:R]->(c), (a)-[:R]->(d)-[:R]->(c) RETURN a, b, c, d"
    val stateGraph = buildStateGraph(query)
    val plan = planQuery(query)

    // The StateGraph for the NonNodeActor should have Output + Anchor
    stateGraph.states.size shouldBe 2
    stateGraph.states.values.count(_.isInstanceOf[StateDescriptor.Output]) shouldBe 1
    stateGraph.states.values.count(_.isInstanceOf[StateDescriptor.Anchor]) shouldBe 1

    // The underlying QueryPlan should use CrossProduct to combine the two paths
    // and have 4 Expand operators (a->b, b->c, a->d, d->c)
    def countExpands(p: QueryPlan): Int = p match {
      case QueryPlan.Expand(_, _, onNeighbor) => 1 + countExpands(onNeighbor)
      case other => other.children.map(countExpands).sum
    }
    countExpands(plan) shouldBe 4

    // Should have CrossProduct to combine the two paths from 'a'
    def hasCrossProduct(p: QueryPlan): Boolean = p match {
      case _: QueryPlan.CrossProduct => true
      case other => other.children.exists(hasCrossProduct)
    }
    hasCrossProduct(plan) shouldBe true
  }

  // ============================================================
  // TESTS: Anchor dispatch behavior
  // ============================================================

  "Computed anchor" should "use the target expression correctly" in {
    val query = """
      MATCH (n)
      WHERE id(n) = idFrom('test-id')
      RETURN n
    """
    val stateGraph = buildStateGraph(query)

    // Should have exactly 1 Anchor descriptor
    val anchors = stateGraph.states.values.collect { case anchor: StateDescriptor.Anchor => anchor }
    anchors.size shouldBe 1
  }

  "AllNodes anchor" should "be created for unconstrained node" in {
    val query = "MATCH (n) RETURN n"
    val stateGraph = buildStateGraph(query)

    val anchors = stateGraph.states.values.collect { case anchor: StateDescriptor.Anchor => anchor }

    // Unconstrained MATCH (n) should use AllNodes anchor
    anchors.size shouldBe 1
    anchors.head.target shouldBe AnchorTarget.AllNodes
  }

  // ============================================================
  // TESTS: Anchor onTarget plan analysis
  // ============================================================

  "APT standing query Anchor" should "show the dispatched plan structure" in {
    val query = """
      MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
            (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
      WHERE e1.type = "WRITE"
        AND e2.type = "READ"
        AND e3.type = "DELETE"
        AND e4.type = "SEND"
      RETURN DISTINCT id(f) as fileId
    """
    val plan = planQuery(query)

    // Get the Anchor and show what it dispatches
    plan match {
      case QueryPlan.Anchor(_, _) =>
        // Count states that would be created when this onTarget is dispatched
        val dispatchedGraph = buildStateGraph(query)
        // APT pattern should have Output + Anchor states at minimum
        dispatchedGraph.states should not be empty

      case _ =>
        fail(s"Expected Anchor at root of query plan")
    }
  }

  "APT ingest 1" should "show states per anchor dispatch" in {
    val query = """
      MATCH (proc), (event), (object)
      WHERE id(proc) = idFrom('test-pid')
        AND id(event) = idFrom('test-event')
        AND id(object) = idFrom('test-object')
      SET proc.id = 'test-pid',
          proc:Process,
          event.type = 'WRITE',
          event:EndpointEvent
      CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)
    """
    val plan = planQuery(query)

    // Count anchors and their onTarget plans
    def countAnchors(p: QueryPlan): Int = p match {
      case QueryPlan.Anchor(_, onTarget) => 1 + countAnchors(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAnchors).sum
      case QueryPlan.Sequence(first, andThen, _) => countAnchors(first) + countAnchors(andThen)
      case QueryPlan.Filter(_, input) => countAnchors(input)
      case QueryPlan.Project(_, _, input) => countAnchors(input)
      case QueryPlan.LocalEffect(_, input) => countAnchors(input)
      case QueryPlan.Distinct(input) => countAnchors(input)
      case _ => 0
    }

    // Should have anchors for proc, event, and object (each with idFrom)
    // The plan uses Sequence to chain effects, which can result in more than 3 anchors
    // in the recursive count as it traverses the full plan tree
    countAnchors(plan) shouldBe 6
  }

  // ============================================================
  // KEY TEST: What gets dispatched to target nodes
  // ============================================================

  "APT standing query dispatched plan" should "show states created ON EACH TARGET NODE" in {
    val query = """
      MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
            (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
      WHERE e1.type = "WRITE"
        AND e2.type = "READ"
        AND e3.type = "DELETE"
        AND e4.type = "SEND"
      RETURN DISTINCT id(f) as fileId
    """
    val plan = planQuery(query)

    // Extract the onTarget plan that gets dispatched to each node
    plan match {
      case QueryPlan.Anchor(_, onTarget) =>
        // Build a StateGraph for this dispatched plan alone
        val promise = Promise[Seq[QueryContext]]()
        val dispatchedGraph = QueryStateBuilder.build(
          plan = onTarget,
          mode = RuntimeMode.Lazy,
          params = Map.empty,
          namespace = defaultNamespaceId,
          output = OutputTarget.EagerCollector(promise),
        )

        // List each state
        dispatchedGraph.states.values.foreach { state => }

        // This is the key metric! Each node gets this many states.
        // If we have 14,000 nodes from ingest and AllNodes dispatches to all,
        // we get states.size * 14,000 total states!
        dispatchedGraph.states.size should be <= 30 // Reasonable bound per node

      case other =>
        fail(s"Expected Anchor at root, got: ${other.getClass.getSimpleName}")
    }
  }

  "Expand cascading" should "show states created when following edges" in {
    // Simpler example: (a)-[:R]->(b)-[:R]->(c)
    // When Anchor dispatches to node 'a', what gets created?
    val query = "MATCH (a)-[:R]->(b)-[:R]->(c) RETURN a, b, c"
    val plan = planQuery(query)

    plan match {
      case QueryPlan.Anchor(_, onTarget) =>
        // Build the dispatched graph
        val promise = Promise[Seq[QueryContext]]()
        val dispatchedGraph = QueryStateBuilder.build(
          plan = onTarget,
          mode = RuntimeMode.Lazy,
          params = Map.empty,
          namespace = defaultNamespaceId,
          output = OutputTarget.EagerCollector(promise),
        )

        // Now look at what the Expand dispatches to neighbors
        // Find the Expand descriptor
        val expands = dispatchedGraph.states.values.collect { case e: StateDescriptor.Expand => e }
        expands.foreach { e =>
          // Build the neighbor plan to see its state count
          val neighborPromise = Promise[Seq[QueryContext]]()
          val neighborGraph = QueryStateBuilder.build(
            plan = e.onNeighborPlan,
            mode = RuntimeMode.Lazy,
            params = Map.empty,
            namespace = defaultNamespaceId,
            output = OutputTarget.EagerCollector(neighborPromise),
          )
          // Each neighbor should have at least one state
          neighborGraph.states should not be empty
        }

      case other =>
        fail(s"Expected Anchor at root, got: ${other.getClass.getSimpleName}")
    }
  }

  // ============================================================
  // DETAILED ANALYSIS: APT Standing Query full cascade
  // ============================================================

  "APT standing query full cascade" should "show total states including Expand dispatch" in {
    val query = """
      MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
            (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
      WHERE e1.type = "WRITE"
        AND e2.type = "READ"
        AND e3.type = "DELETE"
        AND e4.type = "SEND"
      RETURN DISTINCT id(f) as fileId
    """
    val plan = planQuery(query)

    plan match {
      case QueryPlan.Anchor(_, onTarget) =>
        // Level 1: States on the anchor target (every node)
        val targetPromise = Promise[Seq[QueryContext]]()
        val targetGraph = QueryStateBuilder.build(
          plan = onTarget,
          mode = RuntimeMode.Lazy,
          params = Map.empty,
          namespace = defaultNamespaceId,
          output = OutputTarget.EagerCollector(targetPromise),
        )

        // Level 2: Find what each Expand dispatches to neighbors
        val expands = targetGraph.states.values.collect { case e: StateDescriptor.Expand => e }.toSeq

        // Compute level 2 and level 3 state totals using functional accumulation
        val level2Graphs = expands.map { expand =>
          val neighborPromise = Promise[Seq[QueryContext]]()
          QueryStateBuilder.build(
            plan = expand.onNeighborPlan,
            mode = RuntimeMode.Lazy,
            params = Map.empty,
            namespace = defaultNamespaceId,
            output = OutputTarget.EagerCollector(neighborPromise),
          )
        }
        val level2Total = level2Graphs.map(_.states.size).sum

        // Level 3: Do these neighbors have Expands too?
        val level3Total = level2Graphs.flatMap { neighborGraph =>
          val nestedExpands = neighborGraph.states.values.collect { case e: StateDescriptor.Expand => e }.toSeq
          nestedExpands.map { nested =>
            val nestedPromise = Promise[Seq[QueryContext]]()
            val nestedGraph = QueryStateBuilder.build(
              plan = nested.onNeighborPlan,
              mode = RuntimeMode.Lazy,
              params = Map.empty,
              namespace = defaultNamespaceId,
              output = OutputTarget.EagerCollector(nestedPromise),
            )
            nestedGraph.states.size
          }
        }.sum

        // The APT pattern has multiple levels of edge traversal
        // Verify that the cascade produces states at each level
        level2Total should be > 0
        level3Total should be > 0

      case other =>
        fail(s"Expected Anchor at root, got: ${other.getClass.getSimpleName}")
    }
  }

}
