package com.thatdot.quine.graph.cypher.quinepattern

import cats.data.NonEmptyList
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisModule, SymbolAnalysisPhase}
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast.Expression
import com.thatdot.language.phases.UpgradeModule._
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.graph.{GraphQueryPattern, QuineIdRandomLongProvider}

/** Tests for QueryPlanner.
  *
  * These tests parse Cypher queries and verify the generated query plans
  * have the expected structure.
  */
class QueryPlannerTest extends AnyFlatSpec with Matchers {

  // ============================================================
  // PARSING HELPER
  // ============================================================

  /** Parse a Cypher query string and return the AST and symbol table */
  private def parseCypher(query: String): (Cypher.Query.SingleQuery, SymbolAnalysisModule.SymbolTable) = {
    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (tableState, result) = parser.process(query).value.run(LexerState(Nil)).value
    result match {
      case Some(q: Cypher.Query.SingleQuery) => (q, tableState.symbolTable)
      case Some(other) => fail(s"Expected SingleQuery, got: $other")
      case None => fail(s"Parse error for query: $query")
    }
  }

  /** Plan a Cypher query string */
  private def planQuery(query: String): QueryPlan = {
    val (parsedQuery, symbolTable) = parseCypher(query)
    QueryPlanner.plan(parsedQuery, symbolTable)
  }

  // ============================================================
  // PLAN INSPECTION HELPERS
  // ============================================================

  /** Count Computed anchors in a plan */
  private def countComputedAnchors(p: QueryPlan): Int = p match {
    case QueryPlan.Anchor(AnchorTarget.Computed(_), onTarget) => 1 + countComputedAnchors(onTarget)
    case QueryPlan.Anchor(AnchorTarget.AllNodes, onTarget) => countComputedAnchors(onTarget)
    case QueryPlan.Project(_, _, input) => countComputedAnchors(input)
    case QueryPlan.Filter(_, input) => countComputedAnchors(input)
    case QueryPlan.Sequence(first, andThen, _) => countComputedAnchors(first) + countComputedAnchors(andThen)
    case QueryPlan.CrossProduct(queries, _) => queries.map(countComputedAnchors).sum
    case QueryPlan.LocalEffect(_, input) => countComputedAnchors(input)
    case QueryPlan.Distinct(input) => countComputedAnchors(input)
    case QueryPlan.Unwind(_, _, subquery) => countComputedAnchors(subquery)
    case QueryPlan.Aggregate(_, _, input) => countComputedAnchors(input)
    case _ => 0
  }

  /** Check if plan contains a specific operator */
  private def containsOperator(p: QueryPlan, check: QueryPlan => Boolean): Boolean =
    if (check(p)) true
    else
      p match {
        case QueryPlan.Anchor(_, onTarget) => containsOperator(onTarget, check)
        case QueryPlan.Project(_, _, input) => containsOperator(input, check)
        case QueryPlan.Filter(_, input) => containsOperator(input, check)
        case QueryPlan.Sequence(first, andThen, _) =>
          containsOperator(first, check) || containsOperator(andThen, check)
        case QueryPlan.CrossProduct(queries, _) => queries.exists(q => containsOperator(q, check))
        case QueryPlan.LocalEffect(_, input) => containsOperator(input, check)
        case QueryPlan.Distinct(input) => containsOperator(input, check)
        case QueryPlan.Unwind(_, _, subquery) => containsOperator(subquery, check)
        case QueryPlan.Aggregate(_, _, input) => containsOperator(input, check)
        case QueryPlan.Expand(_, _, onNeighbor) => containsOperator(onNeighbor, check)
        case _ => false
      }

  /** Find all CreateHalfEdge effects in a plan */
  private def findCreateHalfEdges(p: QueryPlan): List[LocalQueryEffect.CreateHalfEdge] = p match {
    case QueryPlan.LocalEffect(effects, child) =>
      val edges = effects.collect { case e: LocalQueryEffect.CreateHalfEdge => e }
      edges ++ findCreateHalfEdges(child)
    case QueryPlan.Anchor(_, onTarget) => findCreateHalfEdges(onTarget)
    case QueryPlan.Project(_, _, input) => findCreateHalfEdges(input)
    case QueryPlan.Filter(_, input) => findCreateHalfEdges(input)
    case QueryPlan.Sequence(first, andThen, _) => findCreateHalfEdges(first) ++ findCreateHalfEdges(andThen)
    case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findCreateHalfEdges)
    case QueryPlan.Unwind(_, _, subquery) => findCreateHalfEdges(subquery)
    case _ => Nil
  }

  /** Count Expand operators in a plan */
  private def countExpands(p: QueryPlan): Int = p match {
    case QueryPlan.Expand(_, _, child) => 1 + countExpands(child)
    case QueryPlan.CrossProduct(queries, _) => queries.map(countExpands).sum
    case QueryPlan.Filter(_, input) => countExpands(input)
    case QueryPlan.Project(_, _, input) => countExpands(input)
    case QueryPlan.Sequence(first, andThen, _) => countExpands(first) + countExpands(andThen)
    case QueryPlan.Anchor(_, onTarget) => countExpands(onTarget)
    case _ => 0
  }

  // ============================================================
  // SIMPLE MATCH TESTS
  // ============================================================

  "QueryPlanner" should "plan a simple node match: MATCH (n) RETURN n" in {
    val plan = planQuery("MATCH (n) RETURN n")

    plan shouldBe a[QueryPlan.Anchor]
    val anchor = plan.asInstanceOf[QueryPlan.Anchor]
    anchor.onTarget shouldBe a[QueryPlan.Project]
  }

  it should "plan a node match with label: MATCH (n:Person) RETURN n" in {
    val plan = planQuery("MATCH (n:Person) RETURN n")

    plan shouldBe a[QueryPlan.Anchor]

    // Should contain LocalLabels with Person constraint
    containsOperator(
      plan,
      {
        case QueryPlan.LocalLabels(_, constraint) =>
          constraint match {
            case LabelConstraint.Contains(labels) => labels.contains(Symbol("Person"))
            case _ => false
          }
        case _ => false
      },
    ) shouldBe true
  }

  // ============================================================
  // WHERE CLAUSE TESTS
  // ============================================================

  it should "plan a match with property predicate: MATCH (n) WHERE n.age > 21 RETURN n" in {
    val plan = planQuery("MATCH (n) WHERE n.age > 21 RETURN n")

    containsOperator(plan, _.isInstanceOf[QueryPlan.Filter]) shouldBe true
  }

  it should "extract ID lookups from WHERE clause: MATCH (n) WHERE id(n) = $nodeId RETURN n" in {
    val plan = planQuery("MATCH (n) WHERE id(n) = $nodeId RETURN n")

    // Should produce an Anchor with Computed target (not AllNodes)
    containsOperator(
      plan,
      {
        case QueryPlan.Anchor(AnchorTarget.Computed(_), _) => true
        case _ => false
      },
    ) shouldBe true
  }

  // ============================================================
  // EDGE TRAVERSAL TESTS
  // ============================================================

  it should "plan a simple edge pattern: MATCH (a)-[:KNOWS]->(b) RETURN a, b" in {
    val plan = planQuery("MATCH (a)-[:KNOWS]->(b) RETURN a, b")

    // Should contain an Expand operator
    containsOperator(
      plan,
      {
        case QueryPlan.Expand(Some(label), _, _) => label == Symbol("KNOWS")
        case _ => false
      },
    ) shouldBe true
  }

  it should "plan incoming edge: MATCH (a)<-[:FOLLOWS]-(b) RETURN a, b" in {
    val plan = planQuery("MATCH (a)<-[:FOLLOWS]-(b) RETURN a, b")

    // Should contain an Expand with Incoming direction
    containsOperator(
      plan,
      {
        case QueryPlan.Expand(_, direction, _) =>
          direction == com.thatdot.quine.model.EdgeDirection.Incoming
        case _ => false
      },
    ) shouldBe true
  }

  it should "plan a chain pattern: MATCH (a)-[:KNOWS]->(b)-[:LIKES]->(c) RETURN a, b, c" in {
    val plan = planQuery("MATCH (a)-[:KNOWS]->(b)-[:LIKES]->(c) RETURN a, b, c")

    // We expect 2 nested Expand operators
    countExpands(plan) shouldBe 2
  }

  // ============================================================
  // MULTIPLE MATCH CLAUSES (CrossProduct)
  // ============================================================

  it should "plan multiple MATCH clauses: MATCH (a) MATCH (b) RETURN a, b" in {
    val plan = planQuery("MATCH (a) MATCH (b) RETURN a, b")

    // Should contain a CrossProduct
    containsOperator(
      plan,
      {
        case QueryPlan.CrossProduct(queries, _) => queries.size >= 2
        case _ => false
      },
    ) shouldBe true
  }

  // ============================================================
  // SEQUENCE OPERATOR (Effects after MATCH)
  // ============================================================

  it should "plan MATCH with SET: MATCH (n) SET n.updated = true RETURN n" in {
    val plan = planQuery("MATCH (n) SET n.updated = true RETURN n")

    containsOperator(plan, _.isInstanceOf[QueryPlan.Sequence]) shouldBe true
  }

  it should "plan SET property as LocalEffect" in {
    val plan = planQuery("MATCH (n) SET n.name = 'test' RETURN n")

    containsOperator(
      plan,
      {
        case QueryPlan.LocalEffect(effects, _) =>
          effects.exists {
            case LocalQueryEffect.SetProperty(_, propName, _) => propName == Symbol("name")
            case _ => false
          }
        case _ => false
      },
    ) shouldBe true
  }

  it should "plan SET labels as LocalEffect" in {
    val plan = planQuery("MATCH (n) SET n:Person:Employee RETURN n")

    containsOperator(
      plan,
      {
        case QueryPlan.LocalEffect(effects, _) =>
          effects.exists {
            case LocalQueryEffect.SetLabels(_, labels) =>
              labels.contains(Symbol("Person")) && labels.contains(Symbol("Employee"))
            case _ => false
          }
        case _ => false
      },
    ) shouldBe true
  }

  // ============================================================
  // MULTIPLE ANCHORS
  // ============================================================

  it should "plan ID anchor followed by edge traversal" in {
    val plan = planQuery("""
      MATCH (a) WHERE id(a) = $aId
      MATCH (a)-[:KNOWS]->(b)
      RETURN b
    """)

    containsOperator(
      plan,
      {
        case QueryPlan.Anchor(AnchorTarget.Computed(_), _) => true
        case _ => false
      },
    ) shouldBe true
  }

  it should "plan two ID anchors" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      RETURN a, b
    """)

    countComputedAnchors(plan) shouldBe 2
  }

  it should "plan dependent anchors with Sequence" in {
    val query = """
      MATCH (a), (b)
      WHERE id(b) = idFrom(a.x) AND id(a) = $aId
      RETURN a, b
    """
    val (parsedQuery, symbolTable) = parseCypher(query)

    val plan = QueryPlanner.plan(parsedQuery, symbolTable)

    countComputedAnchors(plan) shouldBe 2
    containsOperator(plan, _.isInstanceOf[QueryPlan.Sequence]) shouldBe true
  }

  // ============================================================
  // AGGREGATION TESTS
  // ============================================================

  it should "plan count aggregation: MATCH (n) RETURN count(n) AS cnt" in {
    val plan = planQuery("MATCH (n) RETURN count(n) AS cnt")

    plan shouldBe a[QueryPlan.Project]
    val project = plan.asInstanceOf[QueryPlan.Project]
    project.input shouldBe a[QueryPlan.Aggregate]
    val aggregate = project.input.asInstanceOf[QueryPlan.Aggregate]
    aggregate.aggregations should have size 1
    aggregate.aggregations.head shouldBe a[Aggregation.Count]
  }

  it should "plan sum aggregation: MATCH (n) RETURN sum(n.value) AS total" in {
    val plan = planQuery("MATCH (n) RETURN sum(n.value) AS total")

    plan shouldBe a[QueryPlan.Project]
    val project = plan.asInstanceOf[QueryPlan.Project]
    project.input shouldBe a[QueryPlan.Aggregate]
    val aggregate = project.input.asInstanceOf[QueryPlan.Aggregate]
    aggregate.aggregations.head shouldBe a[Aggregation.Sum]
  }

  // ============================================================
  // DISTINCT TESTS
  // ============================================================

  it should "plan DISTINCT projection: MATCH (n) RETURN DISTINCT n.type AS type" in {
    val plan = planQuery("MATCH (n) RETURN DISTINCT n.type AS type")

    plan shouldBe a[QueryPlan.Anchor]
    val anchor = plan.asInstanceOf[QueryPlan.Anchor]
    anchor.onTarget shouldBe a[QueryPlan.Distinct]
  }

  it should "plan DISTINCT with multiple columns" in {
    val plan = planQuery("MATCH (n) RETURN DISTINCT n.a AS a, n.b AS b")

    plan shouldBe a[QueryPlan.Anchor]
    val anchor = plan.asInstanceOf[QueryPlan.Anchor]
    anchor.onTarget shouldBe a[QueryPlan.Distinct]
  }

  // ============================================================
  // PROJECTION TESTS
  // ============================================================

  it should "plan property projection: MATCH (n) RETURN n.name AS name" in {
    val plan = planQuery("MATCH (n) RETURN n.name AS name")

    plan shouldBe a[QueryPlan.Anchor]
    val anchor = plan.asInstanceOf[QueryPlan.Anchor]
    anchor.onTarget shouldBe a[QueryPlan.Project]
    val project = anchor.onTarget.asInstanceOf[QueryPlan.Project]
    project.columns should have size 1
    // Projection aliases use raw binding IDs (integers from symbol analysis)
    // Note: User-facing output uses outputNameMapping to convert back to human-readable names
    project.columns.head.as.name.head.isDigit shouldBe true
  }

  // ============================================================
  // IS NOT NULL TESTS
  // ============================================================

  it should "plan pi recipe pattern with IS NOT NULL" in {
    val plan = planQuery("""
      MATCH (n:arctan)
      WHERE n.approximation IS NOT NULL AND n.denominator IS NOT NULL
      RETURN DISTINCT id(n) AS id
    """)

    // Should have property watches
    def findLocalProperties(p: QueryPlan): List[QueryPlan.LocalProperty] = p match {
      case lp: QueryPlan.LocalProperty => List(lp)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findLocalProperties)
      case QueryPlan.Sequence(first, andThen, _) => findLocalProperties(first) ++ findLocalProperties(andThen)
      case QueryPlan.Filter(_, input) => findLocalProperties(input)
      case QueryPlan.Project(_, _, input) => findLocalProperties(input)
      case QueryPlan.Distinct(input) => findLocalProperties(input)
      case QueryPlan.Anchor(_, onTarget) => findLocalProperties(onTarget)
      case _ => Nil
    }

    val props = findLocalProperties(plan)
    val propNames = props.map(_.property.name)
    propNames should contain("approximation")
    propNames should contain("denominator")
  }

  // ============================================================
  // EFFECT PLACEMENT TESTS
  // ============================================================

  "Effect placement" should "group effect with its target anchor for single node query" in {
    val plan = planQuery("MATCH (n) SET n.name = 'test' RETURN n")

    // No Computed anchors - effect runs locally on AllNodes anchor
    countComputedAnchors(plan) shouldBe 0
  }

  it should "group effect with computed anchor when ID lookup is used" in {
    val plan = planQuery("""
      MATCH (row) WHERE id(row) = $rowId
      SET row.title = $title
      RETURN row
    """)

    // Should be exactly 1 Computed anchor (for row), not 2
    countComputedAnchors(plan) shouldBe 1
  }

  it should "group effects by target node in multi-node query" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      SET a.x = 1, b.y = 2, a.z = 3
      RETURN a, b
    """)

    // Should be exactly 2 Computed anchors (one for a, one for b)
    countComputedAnchors(plan) shouldBe 2
  }

  // ============================================================
  // CROSS-NODE DEPENDENCY TESTS
  // ============================================================

  it should "sequence anchors when effect depends on value from another node" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      SET a.x = b.y
      RETURN a
    """)

    containsOperator(plan, _.isInstanceOf[QueryPlan.Sequence]) shouldBe true
  }

  // ============================================================
  // FOREACH TESTS
  // ============================================================

  it should "keep FOREACH local when targeting current node" in {
    val plan = planQuery("""
      MATCH (n)
      FOREACH (x IN [1,2,3] | SET n.value = x)
      RETURN n
    """)

    // No Computed anchors - FOREACH runs locally
    countComputedAnchors(plan) shouldBe 0
  }

  // ============================================================
  // NO-ANCHOR QUERIES
  // ============================================================

  "No-anchor queries" should "plan RETURN 1 without any Anchor" in {
    val plan = planQuery("RETURN 1 AS result")

    containsOperator(plan, _.isInstanceOf[QueryPlan.Anchor]) shouldBe false
    plan shouldBe a[QueryPlan.Project]
  }

  it should "plan CREATE without AllNodes anchor" in {
    val plan = planQuery("CREATE (n:Foo {x: 1}) RETURN n")

    containsOperator(
      plan,
      {
        case QueryPlan.LocalEffect(effects, _) =>
          effects.exists(_.isInstanceOf[LocalQueryEffect.CreateNode])
        case _ => false
      },
    ) shouldBe true

    containsOperator(
      plan,
      {
        case QueryPlan.Anchor(AnchorTarget.AllNodes, _) => true
        case _ => false
      },
    ) shouldBe false
  }

  // ============================================================
  // EDGE CREATION PLANNING TESTS
  // ============================================================

  "Edge Creation Planning" should "plan simple CREATE edge between two nodes" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      CREATE (a)-[:KNOWS]->(b)
      RETURN a, b
    """)

    // Should have CreateHalfEdge effects
    val createEdges = findCreateHalfEdges(plan)
    createEdges should not be empty

    // Should have exactly 2 CreateHalfEdge effects (one for each direction)
    createEdges should have size 2
  }

  it should "plan CREATE edge with edge properties" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      CREATE (a)-[:KNOWS {since: 2020}]->(b)
      RETURN a, b
    """)

    val createEdges = findCreateHalfEdges(plan)
    createEdges should have size 2
  }

  it should "plan bidirectional edge creation" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      CREATE (a)-[:FRIENDS_WITH]->(b), (b)-[:FRIENDS_WITH]->(a)
      RETURN a, b
    """)

    val createEdges = findCreateHalfEdges(plan)
    // 2 edges x 2 half-edges each = 4 CreateHalfEdge effects
    createEdges should have size 4
  }

  it should "plan CREATE edge using idFrom computed IDs" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = idFrom('type', 'a') AND id(b) = idFrom('type', 'b')
      CREATE (a)-[:LINKED_TO]->(b)
      RETURN a
    """)

    // Should have Computed anchors (not AllNodes)
    countComputedAnchors(plan) should be > 0

    val createEdges = findCreateHalfEdges(plan)
    createEdges should have size 2
  }

  it should "plan CREATE edge with chain pattern (a)->(b)->(c)" in {
    val plan = planQuery("""
      MATCH (a), (b), (c)
      WHERE id(a) = $aId AND id(b) = $bId AND id(c) = $cId
      CREATE (a)-[:STEP1]->(b)-[:STEP2]->(c)
      RETURN a, b, c
    """)

    val createEdges = findCreateHalfEdges(plan)
    // Chain: (a)->(b) and (b)->(c) = 2 edges x 2 half-edges = 4
    createEdges should have size 4
  }

  it should "plan CREATE edge combined with SET operations" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      SET a.name = 'Alice', b.name = 'Bob'
      CREATE (a)-[:KNOWS]->(b)
      RETURN a, b
    """)

    val createEdges = findCreateHalfEdges(plan)
    createEdges should have size 2

    // Should also have SetProperty effects
    containsOperator(
      plan,
      {
        case QueryPlan.LocalEffect(effects, _) =>
          effects.exists(_.isInstanceOf[LocalQueryEffect.SetProperty])
        case _ => false
      },
    ) shouldBe true
  }

  it should "ensure edge creation generates two half-edges" in {
    // This test verifies the fundamental property that each edge is represented
    // as two half-edges (one on each node)
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = idFrom('X') AND id(b) = idFrom('Y')
      CREATE (a)-[:REL]->(b)
    """)

    val createEdges = findCreateHalfEdges(plan)

    // Verify we have exactly 2 half-edges
    createEdges should have size 2

    // Verify they point to different targets
    val targetNodes = createEdges.map(_.other)
    targetNodes.toSet should have size 2
  }

  // ============================================================
  // AGGREGATION QUERIES
  // ============================================================

  "Aggregation queries" should "plan MATCH (n) RETURN count(n) with Aggregate operator" in {
    val plan = planQuery("MATCH (n) RETURN count(n) AS cnt")

    containsOperator(plan, _.isInstanceOf[QueryPlan.Aggregate]) shouldBe true
  }

  it should "plan MATCH (n) RETURN collect(n.x) with Aggregate operator" in {
    val plan = planQuery("MATCH (n) RETURN collect(n.x) AS items")

    containsOperator(plan, _.isInstanceOf[QueryPlan.Aggregate]) shouldBe true
  }

  // ============================================================
  // CROSS-NODE DEPENDENCY OPTIMIZATION
  // ============================================================

  "Cross-node dependency optimization" should "sequence anchors to visit each node once" in {
    val plan = planQuery("""
      MATCH (a), (b)
      WHERE id(a) = $aId AND id(b) = $bId
      SET a.x = b.y
      RETURN a
    """)

    // Should have exactly 2 Computed anchors (one per node), not 3+
    countComputedAnchors(plan) shouldBe 2
  }

  // ============================================================
  // UNWIND WITH CREATE EDGE TESTS (Harry Potter pattern)
  // ============================================================

  "UNWIND with CREATE edge" should "plan Harry Potter pattern: parent-child edge creation" in {
    val plan = planQuery("""
      MATCH (p) WHERE id(p) = idFrom('name', $parentName)
      SET p:Person
      WITH p, $children AS childrenNames
      UNWIND childrenNames AS childName
      MATCH (c) WHERE id(c) = idFrom('name', childName)
      CREATE (c)-[:has_parent]->(p)
      RETURN p, c
    """)

    val createEdges = findCreateHalfEdges(plan)

    // Should have CreateHalfEdge effects
    createEdges should not be empty

    // Verify at least one edge has label 'has_parent'
    val hasParentEdges = createEdges.filter(_.label == Symbol("has_parent"))
    hasParentEdges should not be empty

    // Verify edges reference bindings via Ident expressions
    // After symbol analysis, identifiers are Right(QuineIdentifier(n)) where n is a unique Int
    // We verify the structure is correct (Ident or SynthesizeId), not the specific name
    def exprIsBindingRef(expr: Expression): Boolean = expr match {
      case Expression.Ident(_, _, _) => true // Any Ident is a valid binding reference
      case Expression.SynthesizeId(_, _, _) => true // Also accept idFrom if planner is optimized
      case _ => false
    }

    // Edge to p (outgoing from c) - should reference some binding
    val edgeToP = hasParentEdges.find { edge =>
      edge.direction == com.thatdot.quine.model.EdgeDirection.Outgoing &&
      exprIsBindingRef(edge.other)
    }
    edgeToP shouldBe defined

    // Edge to c (incoming to c, i.e., the other half-edge) - should reference some binding
    val edgeToC = hasParentEdges.find { edge =>
      edge.direction == com.thatdot.quine.model.EdgeDirection.Incoming &&
      exprIsBindingRef(edge.other)
    }
    edgeToC shouldBe defined
  }

  it should "use binding references for edges" in {
    // This test verifies that edges correctly reference their target bindings
    val plan = planQuery("""
      MATCH (p) WHERE id(p) = $pId
      WITH p, [1,2,3] AS items
      UNWIND items AS item
      MATCH (c) WHERE id(c) = idFrom(item)
      CREATE (c)-[:rel]->(p)
      RETURN c
    """)

    val createEdges = findCreateHalfEdges(plan)
    createEdges should not be empty

    // Verify edges reference bindings via Ident expressions
    // After symbol analysis, identifiers are Right(QuineIdentifier(n)) where n is a unique Int
    def exprIsBindingRef(expr: Expression): Boolean = expr match {
      case Expression.Ident(_, _, _) => true // Any Ident is a valid binding reference
      case Expression.Parameter(_, _, _) => true // Also accept parameter
      case Expression.SynthesizeId(_, _, _) => true // Also accept idFrom
      case _ => false
    }

    // Edge to p (outgoing from c) - should reference some binding
    val edgeToP = createEdges.find { e =>
      e.direction == com.thatdot.quine.model.EdgeDirection.Outgoing &&
      exprIsBindingRef(e.other)
    }
    edgeToP shouldBe defined

    // Edge to c (incoming to c) - should reference some binding
    val edgeToC = createEdges.find { e =>
      e.direction == com.thatdot.quine.model.EdgeDirection.Incoming &&
      exprIsBindingRef(e.other)
    }
    edgeToC shouldBe defined
  }

  it should "plan nested UNWIND with edge creation between two nodes" in {
    val plan = planQuery("""
      UNWIND [1,2,3,4,5] AS x
      UNWIND [1,2,3,4,5] AS y
      MATCH (myX), (myY)
      WHERE id(myX) = idFrom('x', x) AND id(myY) = idFrom('y', y)
      SET myX:X, myY:Y, myX.x = x, myY.y = y
      CREATE (myX)-[:pairs_with]->(myY)
      RETURN myX, myY
    """)

    // Should have nested Unwind operators
    def countUnwinds(p: QueryPlan): Int = p match {
      case QueryPlan.Unwind(_, _, subquery) => 1 + countUnwinds(subquery)
      case QueryPlan.Anchor(_, onTarget) => countUnwinds(onTarget)
      case QueryPlan.Project(_, _, input) => countUnwinds(input)
      case QueryPlan.Filter(_, input) => countUnwinds(input)
      case QueryPlan.Sequence(first, andThen, _) => countUnwinds(first) + countUnwinds(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countUnwinds).sum
      case QueryPlan.LocalEffect(_, input) => countUnwinds(input)
      case _ => 0
    }

    countUnwinds(plan) shouldBe 2

    // Find the outer Unwind (may be wrapped in Project from RETURN)
    def findOuterUnwind(p: QueryPlan): Option[QueryPlan.Unwind] = p match {
      case u: QueryPlan.Unwind => Some(u)
      case QueryPlan.Project(_, _, input) => findOuterUnwind(input)
      case _ => None
    }

    val outerUnwind = findOuterUnwind(plan)
    outerUnwind shouldBe defined
    // Binding uses raw binding ID (integer from symbol analysis)
    outerUnwind.get.binding.name.head.isDigit shouldBe true

    // The inner UNWIND should be nested inside the outer UNWIND
    containsOperator(
      outerUnwind.get.subquery,
      {
        case QueryPlan.Unwind(_, innerBinding, _) => innerBinding.name.head.isDigit
        case _ => false
      },
    ) shouldBe true

    // Should have CreateHalfEdge effects for pairs_with
    val createEdges = findCreateHalfEdges(plan)
    createEdges should not be empty
    val pairsWithEdges = createEdges.filter(_.label == Symbol("pairs_with"))
    pairsWithEdges should not be empty
  }

  it should "plan simple UNWIND without MATCH" in {
    val plan = planQuery("""
      UNWIND [1,2,3] AS x
      RETURN x
    """)

    // Plan will have Project at top from RETURN, Unwind inside
    containsOperator(plan, _.isInstanceOf[QueryPlan.Unwind]) shouldBe true

    // Find the Unwind inside
    def findUnwind(p: QueryPlan): Option[QueryPlan.Unwind] = p match {
      case u: QueryPlan.Unwind => Some(u)
      case QueryPlan.Project(_, _, input) => findUnwind(input)
      case _ => None
    }

    val unwind = findUnwind(plan)
    unwind shouldBe defined
    // Binding uses raw binding ID (integer from symbol analysis)
    unwind.get.binding.name.head.isDigit shouldBe true
  }

  it should "plan UNWIND with aggregation" in {
    val plan = planQuery("""
      UNWIND [1,2,3,4,5] AS x
      RETURN sum(x) AS total
    """)

    // Should have Unwind containing Aggregate
    containsOperator(plan, _.isInstanceOf[QueryPlan.Unwind]) shouldBe true
    containsOperator(plan, _.isInstanceOf[QueryPlan.Aggregate]) shouldBe true
  }

  it should "plan UNWIND from parameter" in {
    val plan = planQuery("""
      UNWIND $items AS item
      MATCH (n) WHERE id(n) = idFrom(item)
      SET n.processed = true
      RETURN n
    """)

    // Plan will have Project at top from RETURN
    containsOperator(plan, _.isInstanceOf[QueryPlan.Unwind]) shouldBe true

    // Find the Unwind
    def findUnwind(p: QueryPlan): Option[QueryPlan.Unwind] = p match {
      case u: QueryPlan.Unwind => Some(u)
      case QueryPlan.Project(_, _, input) => findUnwind(input)
      case _ => None
    }

    val unwind = findUnwind(plan)
    unwind shouldBe defined
    // Binding uses raw binding ID (integer from symbol analysis)
    unwind.get.binding.name.head.isDigit shouldBe true

    // The Anchor should be inside the Unwind
    containsOperator(
      unwind.get.subquery,
      {
        case QueryPlan.Anchor(AnchorTarget.Computed(_), _) => true
        case _ => false
      },
    ) shouldBe true
  }

  // ============================================================
  // MULTI-NODE SET LABEL TESTS (Movie Data pattern)
  // ============================================================

  "Multi-node SET label" should "place SET label effects inside their respective anchors" in {
    // Simplified version of movie data INGEST-5 pattern
    // The SET u:User should run on u's anchor, SET m:Movie should run on m's anchor
    // Since u and m are already defined in MATCH, we should use SetLabels (not CreateNode)
    val plan = planQuery("""
      MATCH (m), (u)
      WHERE id(m) = $mId AND id(u) = $uId
      SET u:User, m:Movie
      RETURN m, u
    """)

    // Find all SetLabels effects in the plan
    def findSetLabelsEffects(p: QueryPlan): List[(QueryPlan.LocalEffect, LocalQueryEffect.SetLabels)] = p match {
      case le @ QueryPlan.LocalEffect(effects, input) =>
        val setLabels = effects.collect { case sl: LocalQueryEffect.SetLabels => (le, sl) }
        setLabels ++ findSetLabelsEffects(input)
      case QueryPlan.Anchor(_, onTarget) => findSetLabelsEffects(onTarget)
      case QueryPlan.Project(_, _, input) => findSetLabelsEffects(input)
      case QueryPlan.Filter(_, input) => findSetLabelsEffects(input)
      case QueryPlan.Sequence(first, andThen, _) => findSetLabelsEffects(first) ++ findSetLabelsEffects(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findSetLabelsEffects)
      case QueryPlan.Unwind(_, _, subquery) => findSetLabelsEffects(subquery)
      case _ => Nil
    }

    // Find all CreateNode effects in the plan (should be none for existing bindings)
    def findCreateNodeEffects(p: QueryPlan): List[LocalQueryEffect.CreateNode] = p match {
      case QueryPlan.LocalEffect(effects, input) =>
        effects.collect { case cn: LocalQueryEffect.CreateNode => cn } ++ findCreateNodeEffects(input)
      case QueryPlan.Anchor(_, onTarget) => findCreateNodeEffects(onTarget)
      case QueryPlan.Project(_, _, input) => findCreateNodeEffects(input)
      case QueryPlan.Filter(_, input) => findCreateNodeEffects(input)
      case QueryPlan.Sequence(first, andThen, _) => findCreateNodeEffects(first) ++ findCreateNodeEffects(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findCreateNodeEffects)
      case QueryPlan.Unwind(_, _, subquery) => findCreateNodeEffects(subquery)
      case _ => Nil
    }

    val setLabelsEffects = findSetLabelsEffects(plan)
    val createNodeEffects = findCreateNodeEffects(plan)

    // Key assertion: Since u and m are from MATCH, we should have SetLabels (not CreateNode)
    createNodeEffects.size shouldBe 0
    setLabelsEffects.size shouldBe 2

    // Check that we have effects for User and Movie labels
    // Note: target is cleared when effects are placed inside anchors (becomes implicit from context)
    val userEffect = setLabelsEffects.find(_._2.labels.contains(Symbol("User")))
    val movieEffect = setLabelsEffects.find(_._2.labels.contains(Symbol("Movie")))
    userEffect shouldBe defined
    movieEffect shouldBe defined
  }

  it should "place SET label effects inside anchors for movie data rating pattern" in {
    // Full pattern from movie data INGEST-5
    // Since m, u, and rtg are defined in MATCH, labels should use SetLabels (not CreateNode)
    val plan = planQuery("""
      WITH $that AS row
      MATCH (m), (u), (rtg)
      WHERE id(m) = idFrom("Movie", row.movieId)
        AND id(u) = idFrom("User", row.userId)
        AND id(rtg) = idFrom("Rating", row.movieId, row.userId, row.rating)
      SET u.name = row.name, u:User
      SET rtg.rating = row.rating, rtg:Rating
      CREATE (u)-[:SUBMITTED]->(rtg)<-[:HAS_RATING]-(m)
      CREATE (u)-[:RATED]->(m)
      RETURN m, u, rtg
    """)

    // Verify the plan has anchors for each node
    countComputedAnchors(plan) should be >= 3

    // Find all SetLabels effects in the plan
    def findSetLabelsEffects(p: QueryPlan): List[LocalQueryEffect.SetLabels] = p match {
      case QueryPlan.LocalEffect(effects, input) =>
        effects.collect { case sl: LocalQueryEffect.SetLabels => sl } ++ findSetLabelsEffects(input)
      case QueryPlan.Anchor(_, onTarget) => findSetLabelsEffects(onTarget)
      case QueryPlan.Project(_, _, input) => findSetLabelsEffects(input)
      case QueryPlan.Filter(_, input) => findSetLabelsEffects(input)
      case QueryPlan.Sequence(first, andThen, _) => findSetLabelsEffects(first) ++ findSetLabelsEffects(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findSetLabelsEffects)
      case QueryPlan.Unwind(_, _, subquery) => findSetLabelsEffects(subquery)
      case _ => Nil
    }

    // Find all CreateNode effects in the plan (should be none for existing bindings)
    def findCreateNodeEffects(p: QueryPlan): List[LocalQueryEffect.CreateNode] = p match {
      case QueryPlan.LocalEffect(effects, input) =>
        effects.collect { case cn: LocalQueryEffect.CreateNode => cn } ++ findCreateNodeEffects(input)
      case QueryPlan.Anchor(_, onTarget) => findCreateNodeEffects(onTarget)
      case QueryPlan.Project(_, _, input) => findCreateNodeEffects(input)
      case QueryPlan.Filter(_, input) => findCreateNodeEffects(input)
      case QueryPlan.Sequence(first, andThen, _) => findCreateNodeEffects(first) ++ findCreateNodeEffects(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findCreateNodeEffects)
      case QueryPlan.Unwind(_, _, subquery) => findCreateNodeEffects(subquery)
      case _ => Nil
    }

    val setLabelsEffects = findSetLabelsEffects(plan)
    val createNodeEffects = findCreateNodeEffects(plan)

    setLabelsEffects.foreach { sl => }

    // Since u and rtg are from MATCH, we should have SetLabels (not CreateNode)
    // Note: target is cleared when effects are placed inside anchors (becomes implicit from context)
    createNodeEffects.size shouldBe 0
    setLabelsEffects.exists(sl => sl.labels.contains(Symbol("User"))) shouldBe true
    setLabelsEffects.exists(sl => sl.labels.contains(Symbol("Rating"))) shouldBe true
  }

  // ============================================================
  // MOVIE DATA INGEST-1 PATTERN TEST
  // ============================================================

  "Movie data INGEST-1" should "show the Cypher AST structure" in {
    // Debug: show the Cypher AST structure to understand how it's being parsed
    val query = """
      WITH $that AS row
      MATCH (m)
      WHERE id(m) = idFrom("Movie", row.movieId)
      SET
        m:Movie,
        m.tmdbId = row.tmdbId,
        m.title = row.title
      WITH m, split(coalesce(row.genres,""), "|") AS genres
      UNWIND genres AS genre
      WITH m, genre
      MATCH (g)
      WHERE id(g) = idFrom("Genre", genre)
      SET g.genre = genre, g:Genre
      CREATE (m)-[:IN_GENRE]->(g)
    """
    val (parsedQuery, _) = parseCypher(query)

    def showQueryStructure(q: Cypher.Query): Unit =
      q match {
        case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
          spq.queryParts.foreach { _ => }
        case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
          mpq.queryParts.foreach { part =>
            part match {
              case _: Cypher.QueryPart.ReadingClausePart =>
              case _: Cypher.QueryPart.WithClausePart =>
              case _: Cypher.QueryPart.EffectPart =>
            }
          }
          mpq.into.queryParts.foreach { part =>
            part match {
              case _: Cypher.QueryPart.ReadingClausePart =>
              case _: Cypher.QueryPart.WithClausePart =>
              case _: Cypher.QueryPart.EffectPart =>
            }
          }
        case _: Cypher.Query.Union =>
      }

    showQueryStructure(parsedQuery)
  }

  it should "plan the movie-genre pattern with UNWIND" in {
    // This is the INGEST-1 pattern from movieData-qp.yaml
    // It has: MATCH -> SET -> WITH -> UNWIND -> WITH -> MATCH -> SET -> CREATE
    val plan = planQuery("""
      WITH $that AS row
      MATCH (m)
      WHERE id(m) = idFrom("Movie", row.movieId)
      SET
        m:Movie,
        m.tmdbId = row.tmdbId,
        m.title = row.title
      WITH m, split(coalesce(row.genres,""), "|") AS genres
      UNWIND genres AS genre
      WITH m, genre
      MATCH (g)
      WHERE id(g) = idFrom("Genre", genre)
      SET g.genre = genre, g:Genre
      CREATE (m)-[:IN_GENRE]->(g)
    """)

    // Verify we have anchors for both m and g
    countComputedAnchors(plan) should be >= 2

    // Find Unwind in the plan
    def findUnwind(p: QueryPlan): Option[QueryPlan.Unwind] = p match {
      case u: QueryPlan.Unwind => Some(u)
      case QueryPlan.Project(_, _, input) => findUnwind(input)
      case QueryPlan.Sequence(first, andThen, _) => findUnwind(first).orElse(findUnwind(andThen))
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findUnwind).headOption
      case QueryPlan.Anchor(_, onTarget) => findUnwind(onTarget)
      case QueryPlan.Filter(_, input) => findUnwind(input)
      case _ => None
    }

    val unwind = findUnwind(plan)
    unwind shouldBe defined

    // Verify edge creation exists
    def findCreateHalfEdge(p: QueryPlan): List[LocalQueryEffect.CreateHalfEdge] = p match {
      case QueryPlan.LocalEffect(effects, input) =>
        effects.collect { case e: LocalQueryEffect.CreateHalfEdge => e } ++ findCreateHalfEdge(input)
      case QueryPlan.Anchor(_, onTarget) => findCreateHalfEdge(onTarget)
      case QueryPlan.Project(_, _, input) => findCreateHalfEdge(input)
      case QueryPlan.Filter(_, input) => findCreateHalfEdge(input)
      case QueryPlan.Sequence(first, andThen, _) => findCreateHalfEdge(first) ++ findCreateHalfEdge(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findCreateHalfEdge)
      case QueryPlan.Unwind(_, _, subquery) => findCreateHalfEdge(subquery)
      case _ => Nil
    }

    val edges = findCreateHalfEdge(plan)
    edges.foreach { e => }
    edges.exists(_.label == Symbol("IN_GENRE")) shouldBe true
  }

  // ============================================================
  // MOVIE DATA INGEST-3 PATTERN TEST (Multi-anchor with CREATE edges)
  // ============================================================

  "Movie data INGEST-3" should "plan multi-anchor MATCH with CREATE edges" in {
    // Simplified INGEST-3 pattern: three anchors with idFrom, plus CREATE edges
    val query = """
      MATCH (p), (m), (r)
      WHERE id(p) = idFrom("Person", $tmdbId)
        AND id(m) = idFrom("Movie", $movieId)
        AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
      SET r.role = $role, r:Role
      CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
      CREATE (p)-[:ACTED_IN]->(m)
    """
    val (parsedQuery, symbolTable) = parseCypher(query)

    // Show idLookups being extracted
    val idLookups = QueryPlanner.extractIdLookups(parsedQuery)
    idLookups.foreach { lookup => }

    val plan = QueryPlanner.plan(parsedQuery, symbolTable)

    // Should have at least 3 computed anchors (for p, m, r)
    // The planner may create additional anchors for edge effect placement
    val anchorCount = countComputedAnchors(plan)
    anchorCount should be >= 3

    // Find all CreateHalfEdge effects
    def findCreateHalfEdge(p: QueryPlan): List[LocalQueryEffect.CreateHalfEdge] = p match {
      case QueryPlan.LocalEffect(effects, input) =>
        effects.collect { case e: LocalQueryEffect.CreateHalfEdge => e } ++ findCreateHalfEdge(input)
      case QueryPlan.Anchor(_, onTarget) => findCreateHalfEdge(onTarget)
      case QueryPlan.Project(_, _, input) => findCreateHalfEdge(input)
      case QueryPlan.Filter(_, input) => findCreateHalfEdge(input)
      case QueryPlan.Sequence(first, andThen, _) => findCreateHalfEdge(first) ++ findCreateHalfEdge(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.flatMap(findCreateHalfEdge)
      case QueryPlan.Unwind(_, _, subquery) => findCreateHalfEdge(subquery)
      case _ => Nil
    }

    val edges = findCreateHalfEdge(plan)
    edges.foreach { e => }

    // Should have edges for PLAYED, HAS_ROLE, and ACTED_IN
    edges.exists(_.label == Symbol("PLAYED")) shouldBe true
    edges.exists(_.label == Symbol("HAS_ROLE")) shouldBe true
    edges.exists(_.label == Symbol("ACTED_IN")) shouldBe true
  }

  // ============================================================
  // STANDING QUERY PATTERN TEST
  // ============================================================

  // ============================================================
  // DISJOINT PATTERNS WITH SHARED NODE TEST
  // ============================================================

  "Disjoint patterns with shared node" should "recognize shared node and avoid separate dispatches" in {
    // This test validates the theory that QPv2 may be treating disjoint patterns
    // (separated by comma) as completely separate even when they share a node.
    //
    // Pattern: (a)<-[:edge]-(b)-[:edge]->(c), (d)<-[:edge]-(b)
    //
    // These two patterns share node 'b'. An optimal planner should:
    // 1. Recognize they share 'b' and treat this as ONE connected pattern
    // 2. Use a single AllNodes anchor (or computed anchor for b)
    // 3. NOT use CrossProduct or multiple AllNodes anchors
    //
    // A suboptimal planner might:
    // 1. Treat these as two separate patterns
    // 2. Use CrossProduct to join them
    // 3. Require multiple AllNodes scans
    val query = """
      MATCH (a)<-[:edge]-(b)-[:edge]->(c),
            (d)<-[:edge]-(b)
      RETURN a, b, c, d
    """
    val plan = planQuery(query)

    // Count AllNodes anchors
    def countAllNodesAnchors(p: QueryPlan): Int = p match {
      case QueryPlan.Anchor(AnchorTarget.AllNodes, onTarget) => 1 + countAllNodesAnchors(onTarget)
      case QueryPlan.Anchor(_, onTarget) => countAllNodesAnchors(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAllNodesAnchors).sum
      case QueryPlan.Sequence(first, andThen, _) => countAllNodesAnchors(first) + countAllNodesAnchors(andThen)
      case QueryPlan.Filter(_, input) => countAllNodesAnchors(input)
      case QueryPlan.Project(_, _, input) => countAllNodesAnchors(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countAllNodesAnchors(onNeighbor)
      case _ => 0
    }

    val allNodesCount = countAllNodesAnchors(plan)

    // CURRENT BEHAVIOR (likely suboptimal):
    // If the planner treats these as separate patterns, we'd expect:
    // - CrossProduct OR
    // - Multiple AllNodes anchors
    //
    // OPTIMAL BEHAVIOR:
    // - Single AllNodes anchor for 'b'
    // - 3 Expand operators from b to a, c, and d
    // - No CrossProduct

    // After the pattern merging optimization:
    // - Single AllNodes anchor for the shared node 'b'
    // - 3 Expand operators (b->a, b->c, b->d)
    // - CrossProduct is used to combine local watches (LocalId + Expands), not to join separate scans
    allNodesCount shouldBe 1
    countExpands(plan) shouldBe 3
    // Note: usesCrossProduct is true but it's the "good" kind - combining local watches at a single node
  }

  it should "show APT-detection-like pattern with shared node 'f'" in {
    // This is similar to the APT detection standing query pattern
    // (e1)-[:EVENT]->(f)<-[:EVENT]-(e2), (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
    //
    // Simplified version without WHERE clause:
    val query = """
      MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2),
            (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
      RETURN f
    """
    val plan = planQuery(query)

    // Count AllNodes anchors
    def countAllNodesAnchors(p: QueryPlan): Int = p match {
      case QueryPlan.Anchor(AnchorTarget.AllNodes, onTarget) => 1 + countAllNodesAnchors(onTarget)
      case QueryPlan.Anchor(_, onTarget) => countAllNodesAnchors(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAllNodesAnchors).sum
      case QueryPlan.Sequence(first, andThen, _) => countAllNodesAnchors(first) + countAllNodesAnchors(andThen)
      case QueryPlan.Filter(_, input) => countAllNodesAnchors(input)
      case QueryPlan.Project(_, _, input) => countAllNodesAnchors(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countAllNodesAnchors(onNeighbor)
      case _ => 0
    }

    val allNodesCount = countAllNodesAnchors(plan)
    val expandCount = countExpands(plan)

    // After the pattern merging optimization:
    // - 1 AllNodes anchor (for f, the shared node)
    // - 5 Expand operators (f->e1, f->e2, f->e3, e3->p2, p2->e4)
    // - CrossProduct is used to combine local watches, not to join separate scans
    allNodesCount shouldBe 1
    expandCount shouldBe 5
  }

  // ============================================================
  // STANDING QUERY PATTERN TESTS
  // ============================================================

  "Standing query with id equality" should "use AllNodes anchor and keep id(a)=id(m) as filter" in {
    // This pattern has no idFrom constraints - just a join condition id(a) = id(m)
    val query = """
      MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
      WHERE id(a) = id(m)
      RETURN id(m) as movieId, id(p) as personId
    """
    val (parsedQuery, symbolTable) = parseCypher(query)

    // id(a) = id(m) should NOT create an IdLookup - it's a join condition
    val idLookups = QueryPlanner.extractIdLookups(parsedQuery)
    idLookups.foreach { lookup => }
    idLookups shouldBe empty // No IdLookups - id(a)=id(m) is not anchor-able

    val plan = QueryPlanner.plan(parsedQuery, symbolTable)

    // Should have an AllNodes anchor (since no computed IDs are available)
    def hasAllNodesAnchor(p: QueryPlan): Boolean = p match {
      case QueryPlan.Anchor(AnchorTarget.AllNodes, _) => true
      case QueryPlan.Anchor(_, onTarget) => hasAllNodesAnchor(onTarget)
      case QueryPlan.Sequence(first, andThen, _) => hasAllNodesAnchor(first) || hasAllNodesAnchor(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.exists(hasAllNodesAnchor)
      case QueryPlan.Filter(_, input) => hasAllNodesAnchor(input)
      case QueryPlan.Project(_, _, input) => hasAllNodesAnchor(input)
      case _ => false
    }

    hasAllNodesAnchor(plan) shouldBe true

    // Should have a Filter in the plan (for id(a) = id(m))
    def hasFilter(p: QueryPlan): Boolean = p match {
      case QueryPlan.Filter(_, _) => true
      case QueryPlan.Anchor(_, onTarget) => hasFilter(onTarget)
      case QueryPlan.Sequence(first, andThen, _) => hasFilter(first) || hasFilter(andThen)
      case QueryPlan.CrossProduct(queries, _) => queries.exists(hasFilter)
      case QueryPlan.Project(_, _, input) => hasFilter(input)
      case QueryPlan.Expand(_, _, onNeighbor) => hasFilter(onNeighbor)
      case _ => false
    }

    hasFilter(plan) shouldBe true
  }

  // ============================================================
  // APT INGEST ANALYSIS
  // ============================================================

  // ============================================================
  // APT STANDING QUERY ANALYSIS
  // ============================================================

  "APT standing query" should "merge patterns on shared node 'f' with WHERE filters" in {
    // This is the actual APT detection standing query from apt-detection-qp.yaml
    // Key pattern: (e1)-[:EVENT]->(f)<-[:EVENT]-(e2), (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
    //
    // The two comma-separated patterns share node 'f'. The planner should:
    // 1. Recognize 'f' is shared and merge the patterns
    // 2. Use a single AllNodes anchor for 'f' (since no idFrom constraint)
    // 3. Use Expand operators to traverse to e1, e2, e3, and the chain e3<-p2->e4
    // 4. Push WHERE filters appropriately (some may be on neighbors via Expand)
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

    // Count AllNodes anchors - should be exactly 1 (for f)
    def countAllNodesAnchors(p: QueryPlan): Int = p match {
      case QueryPlan.Anchor(AnchorTarget.AllNodes, onTarget) => 1 + countAllNodesAnchors(onTarget)
      case QueryPlan.Anchor(_, onTarget) => countAllNodesAnchors(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAllNodesAnchors).sum
      case QueryPlan.Sequence(first, andThen, _) => countAllNodesAnchors(first) + countAllNodesAnchors(andThen)
      case QueryPlan.Filter(_, input) => countAllNodesAnchors(input)
      case QueryPlan.Project(_, _, input) => countAllNodesAnchors(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countAllNodesAnchors(onNeighbor)
      case QueryPlan.Distinct(input) => countAllNodesAnchors(input)
      case _ => 0
    }

    // Count Filter operators (for WHERE clauses that weren't pushed down)
    def countFilters(p: QueryPlan): Int = p match {
      case QueryPlan.Filter(_, input) => 1 + countFilters(input)
      case QueryPlan.Anchor(_, onTarget) => countFilters(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countFilters).sum
      case QueryPlan.Sequence(first, andThen, _) => countFilters(first) + countFilters(andThen)
      case QueryPlan.Project(_, _, input) => countFilters(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countFilters(onNeighbor)
      case QueryPlan.Distinct(input) => countFilters(input)
      case _ => 0
    }

    // Count LocalProperty with Equal constraints (pushed-down predicates)
    def countPushedDownPredicates(p: QueryPlan): Int = p match {
      case QueryPlan.LocalProperty(_, _, PropertyConstraint.Equal(_)) => 1
      case QueryPlan.Anchor(_, onTarget) => countPushedDownPredicates(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countPushedDownPredicates).sum
      case QueryPlan.Sequence(first, andThen, _) =>
        countPushedDownPredicates(first) + countPushedDownPredicates(andThen)
      case QueryPlan.Project(_, _, input) => countPushedDownPredicates(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countPushedDownPredicates(onNeighbor)
      case QueryPlan.Distinct(input) => countPushedDownPredicates(input)
      case QueryPlan.Filter(_, input) => countPushedDownPredicates(input)
      case _ => 0
    }

    // More comprehensive Expand counter that recurses into all plan nodes
    def countAllExpands(p: QueryPlan): Int = p match {
      case QueryPlan.Expand(_, _, child) => 1 + countAllExpands(child)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAllExpands).sum
      case QueryPlan.Filter(_, input) => countAllExpands(input)
      case QueryPlan.Project(_, _, input) => countAllExpands(input)
      case QueryPlan.Sequence(first, andThen, _) => countAllExpands(first) + countAllExpands(andThen)
      case QueryPlan.Anchor(_, onTarget) => countAllExpands(onTarget)
      case QueryPlan.Distinct(input) => countAllExpands(input)
      case QueryPlan.LocalEffect(_, input) => countAllExpands(input)
      case _ => 0
    }

    val allNodesCount = countAllNodesAnchors(plan)
    val expandCount = countAllExpands(plan)
    val filterCount = countFilters(plan)
    val pushedDownCount = countPushedDownPredicates(plan)

    // Assertions:
    // 1. Single AllNodes anchor for the shared node 'f'
    allNodesCount shouldBe 1

    // 2. Five Expand operators for the edge traversals:
    //    - f->e1 (incoming EVENT)
    //    - f->e2 (incoming EVENT)
    //    - f->e3 (incoming EVENT)
    //    - e3->p2 (incoming EVENT)
    //    - p2->e4 (outgoing EVENT)
    expandCount shouldBe 5

    // 3. Predicate pushdown: e1.type = "WRITE", e2.type = "READ", e3.type = "DELETE", e4.type = "SEND"
    //    should be pushed down into LocalProperty with Equal constraints (4 total)
    //    This is the key optimization - MVSQ-style predicate pushdown
    pushedDownCount shouldBe 4

    // 4. No remaining Filter operators (all predicates should be pushed down)
    filterCount shouldBe 0
  }

  it should "show that the onTarget plan is installed on each node" in {
    // This test verifies the structure of the plan that gets dispatched to each node
    // via the AllNodes anchor. The `onTarget` portion is what gets installed.
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

    // Extract the onTarget plan from the AllNodes anchor
    def findOnTargetPlan(p: QueryPlan): Option[QueryPlan] = p match {
      case QueryPlan.Anchor(AnchorTarget.AllNodes, onTarget) => Some(onTarget)
      case QueryPlan.Project(_, _, input) => findOnTargetPlan(input)
      case QueryPlan.Distinct(input) => findOnTargetPlan(input)
      case QueryPlan.Filter(_, input) => findOnTargetPlan(input)
      case _ => None
    }

    val onTarget = findOnTargetPlan(plan)
    onTarget shouldBe defined

    // More comprehensive Expand counter
    def countAllExpands(p: QueryPlan): Int = p match {
      case QueryPlan.Expand(_, _, child) => 1 + countAllExpands(child)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAllExpands).sum
      case QueryPlan.Filter(_, input) => countAllExpands(input)
      case QueryPlan.Project(_, _, input) => countAllExpands(input)
      case QueryPlan.Sequence(first, andThen, _) => countAllExpands(first) + countAllExpands(andThen)
      case QueryPlan.Anchor(_, onTarget) => countAllExpands(onTarget)
      case QueryPlan.Distinct(input) => countAllExpands(input)
      case QueryPlan.LocalEffect(_, input) => countAllExpands(input)
      case _ => 0
    }

    // Count operators in the onTarget plan (this is what runs on each node)
    val onTargetExpands = countAllExpands(onTarget.get)

    // The onTarget plan should contain the 5 Expand operators for traversing from f
    onTargetExpands shouldBe 5

    // Also verify the structure contains the expected local watches
    def countLocalIds(p: QueryPlan): Int = p match {
      case QueryPlan.LocalId(_) => 1
      case QueryPlan.Expand(_, _, child) => countLocalIds(child)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countLocalIds).sum
      case QueryPlan.Filter(_, input) => countLocalIds(input)
      case QueryPlan.Project(_, _, input) => countLocalIds(input)
      case QueryPlan.Distinct(input) => countLocalIds(input)
      case _ => 0
    }

    val localIdCount = countLocalIds(onTarget.get)

    // LocalId is only emitted when node identity is needed:
    // - Explicit id(n) usage (only id(f) in this query)
    // - Diamond patterns requiring identity comparison (f is common root, not renamed)
    // - CREATE effects (none in this query)
    // So only f needs LocalId
    localIdCount shouldBe 1
  }

  it should "compare with MVSQ plan for the same pattern" in {
    // Construct the GraphQueryPattern manually for the APT standing query
    // Pattern: (e1)-[:EVENT]->(f)<-[:EVENT]-(e2), (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
    // WHERE e1.type = "WRITE" AND e2.type = "READ" AND e3.type = "DELETE" AND e4.type = "SEND"
    import GraphQueryPattern._

    implicit val idProvider: QuineIdRandomLongProvider.type = QuineIdRandomLongProvider

    val e1 = NodePatternId(0)
    val f = NodePatternId(1)
    val e2 = NodePatternId(2)
    val e3 = NodePatternId(3)
    val p2 = NodePatternId(4)
    val e4 = NodePatternId(5)

    val nodePatterns = List(
      NodePattern(
        e1,
        Set.empty,
        None,
        Map(Symbol("type") -> PropertyValuePattern.Value(com.thatdot.quine.model.QuineValue.Str("WRITE"))),
      ),
      NodePattern(f, Set.empty, None, Map.empty),
      NodePattern(
        e2,
        Set.empty,
        None,
        Map(Symbol("type") -> PropertyValuePattern.Value(com.thatdot.quine.model.QuineValue.Str("READ"))),
      ),
      NodePattern(
        e3,
        Set.empty,
        None,
        Map(Symbol("type") -> PropertyValuePattern.Value(com.thatdot.quine.model.QuineValue.Str("DELETE"))),
      ),
      NodePattern(p2, Set.empty, None, Map.empty),
      NodePattern(
        e4,
        Set.empty,
        None,
        Map(Symbol("type") -> PropertyValuePattern.Value(com.thatdot.quine.model.QuineValue.Str("SEND"))),
      ),
    )

    val edgePatterns = List(
      EdgePattern(e1, f, isDirected = true, Symbol("EVENT")), // (e1)-[:EVENT]->(f)
      EdgePattern(e2, f, isDirected = true, Symbol("EVENT")), // (e2)-[:EVENT]->(f) i.e. (f)<-[:EVENT]-(e2)
      EdgePattern(e3, f, isDirected = true, Symbol("EVENT")), // (e3)-[:EVENT]->(f) i.e. (f)<-[:EVENT]-(e3)
      EdgePattern(p2, e3, isDirected = true, Symbol("EVENT")), // (p2)-[:EVENT]->(e3) i.e. (e3)<-[:EVENT]-(p2)
      EdgePattern(p2, e4, isDirected = true, Symbol("EVENT")), // (p2)-[:EVENT]->(e4)
    )

    // Try with f as starting point - constraints IN node patterns (inline style)
    val patternWithFAsRoot = GraphQueryPattern(
      nodes = NonEmptyList.fromListUnsafe(nodePatterns),
      edges = edgePatterns,
      startingPoint = f, // Anchor on f
      toExtract = List(ReturnColumn.Id(f, formatAsString = false, Symbol("fileId"))),
      filterCond = None, // No filter - constraints are in node patterns
      toReturn = Nil,
      distinct = true,
    )

    // NOW TEST: What if constraints are in filterCond (WHERE clause style)?
    val nodePatternsNoConstraints = List(
      NodePattern(e1, Set.empty, None, Map.empty), // No constraint
      NodePattern(f, Set.empty, None, Map.empty),
      NodePattern(e2, Set.empty, None, Map.empty), // No constraint
      NodePattern(e3, Set.empty, None, Map.empty), // No constraint
      NodePattern(p2, Set.empty, None, Map.empty),
      NodePattern(e4, Set.empty, None, Map.empty), // No constraint
    )

    // We need to extract the type properties to use them in the filter
    val toExtractWithTypes = List(
      ReturnColumn.Id(f, formatAsString = false, Symbol("fileId")),
      ReturnColumn.Property(e1, Symbol("type"), Symbol("e1_type")),
      ReturnColumn.Property(e2, Symbol("type"), Symbol("e2_type")),
      ReturnColumn.Property(e3, Symbol("type"), Symbol("e3_type")),
      ReturnColumn.Property(e4, Symbol("type"), Symbol("e4_type")),
    )

    // WHERE e1.type = "WRITE" AND e2.type = "READ" AND e3.type = "DELETE" AND e4.type = "SEND"
    val whereClause = Some(
      Expr.And(
        Vector(
          Expr.Equal(Expr.Variable(Symbol("e1_type")), Expr.Str("WRITE")),
          Expr.Equal(Expr.Variable(Symbol("e2_type")), Expr.Str("READ")),
          Expr.Equal(Expr.Variable(Symbol("e3_type")), Expr.Str("DELETE")),
          Expr.Equal(Expr.Variable(Symbol("e4_type")), Expr.Str("SEND")),
        ),
      ),
    )

    val patternWithWhereClause = GraphQueryPattern(
      nodes = NonEmptyList.fromListUnsafe(nodePatternsNoConstraints),
      edges = edgePatterns,
      startingPoint = f,
      toExtract = toExtractWithTypes,
      filterCond = whereClause, // Constraints in WHERE clause!
      toReturn = Nil,
      distinct = true,
    )

    // Try with e1 as starting point (constrained node)
    val patternWithE1AsRoot = GraphQueryPattern(
      nodes = NonEmptyList.fromListUnsafe(nodePatterns),
      edges = edgePatterns,
      startingPoint = e1, // Anchor on e1 (has type=WRITE constraint)
      toExtract = List(ReturnColumn.Id(f, formatAsString = false, Symbol("fileId"))),
      filterCond = None,
      toReturn = Nil,
      distinct = true,
    )

    val labelsProperty = Symbol("__labels")

    // Compile MVSQ with different starting points - test passes if these compile without error
    patternWithFAsRoot.compiledMultipleValuesStandingQuery(labelsProperty, idProvider)
    patternWithWhereClause.compiledMultipleValuesStandingQuery(labelsProperty, idProvider)
    patternWithE1AsRoot.compiledMultipleValuesStandingQuery(labelsProperty, idProvider)

    // The test passes if we get here - this is for inspection
    succeed
  }

  // ============================================================
  // APT INGEST ANALYSIS
  // ============================================================

  "APT ingest 1 query" should "show the query plan for endpoint.json ingest" in {
    // This is the actual ingest 1 query from apt-detection-qp.yaml
    val query = """
      MATCH (proc), (event), (object)
      WHERE id(proc) = idFrom($that.pid)
        AND id(event) = idFrom($that)
        AND id(object) = idFrom($that.object)
      SET proc.id = $that.pid,
          proc:Process,
          event.type = $that.event_type,
          event:EndpointEvent,
          event.time = $that.time,
          object.data = $that.object
      CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)
    """
    val plan = planQuery(query)

    // Count anchors
    def countAnchors(p: QueryPlan): Int = p match {
      case QueryPlan.Anchor(_, onTarget) => 1 + countAnchors(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countAnchors).sum
      case QueryPlan.Sequence(first, andThen, _) => countAnchors(first) + countAnchors(andThen)
      case QueryPlan.Filter(_, input) => countAnchors(input)
      case QueryPlan.Project(_, _, input) => countAnchors(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countAnchors(onNeighbor)
      case QueryPlan.LocalEffect(_, input) => countAnchors(input)
      case _ => 0
    }

    // Count effects
    def countEffects(p: QueryPlan): Int = p match {
      case QueryPlan.LocalEffect(effects, input) => effects.size + countEffects(input)
      case QueryPlan.Anchor(_, onTarget) => countEffects(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countEffects).sum
      case QueryPlan.Sequence(first, andThen, _) => countEffects(first) + countEffects(andThen)
      case QueryPlan.Filter(_, input) => countEffects(input)
      case QueryPlan.Project(_, _, input) => countEffects(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countEffects(onNeighbor)
      case _ => 0
    }

    // Should have anchors for proc, event, and object (each with idFrom)
    // The plan uses Sequence to chain effects, counting 6 anchors in the full tree
    countAnchors(plan) shouldBe 6

    // Should have effects for SET and CREATE
    countEffects(plan) should be > 0
  }

  // ============================================================
  // INLINE PROPERTY PATTERN TESTS
  // ============================================================

  it should "treat inline property patterns as equivalent to WHERE clause predicates" in {
    // These two queries should produce equivalent plans with predicate pushdown:
    // 1. MATCH (n {foo: "bar"}) RETURN id(n)   -- inline property syntax
    // 2. MATCH (n) WHERE n.foo = "bar" RETURN id(n)  -- WHERE clause syntax

    val inlinePlan = planQuery("""MATCH (n {foo: "bar"}) RETURN id(n)""")
    val wherePlan = planQuery("""MATCH (n) WHERE n.foo = "bar" RETURN id(n)""")

    // Count LocalProperty with Equal constraints (pushed-down predicates)
    def countPushedDownPredicates(p: QueryPlan): Int = p match {
      case QueryPlan.LocalProperty(_, _, PropertyConstraint.Equal(_)) => 1
      case QueryPlan.Anchor(_, onTarget) => countPushedDownPredicates(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countPushedDownPredicates).sum
      case QueryPlan.Sequence(first, andThen, _) =>
        countPushedDownPredicates(first) + countPushedDownPredicates(andThen)
      case QueryPlan.Project(_, _, input) => countPushedDownPredicates(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countPushedDownPredicates(onNeighbor)
      case QueryPlan.Distinct(input) => countPushedDownPredicates(input)
      case QueryPlan.Filter(_, input) => countPushedDownPredicates(input)
      case _ => 0
    }

    // The WHERE clause version should have 1 pushed-down predicate
    val wherePushedDown = countPushedDownPredicates(wherePlan)
    wherePushedDown shouldBe 1

    // The inline property version should ALSO have 1 pushed-down predicate
    // This is the feature we're testing - inline properties should be extracted
    // and pushed down just like WHERE clause predicates
    val inlinePushedDown = countPushedDownPredicates(inlinePlan)
    inlinePushedDown shouldBe 1
  }

  it should "push down inline property predicates on multiple nodes" in {
    // Test with inline properties on multiple nodes in a pattern
    val plan = planQuery("""MATCH (a {type: "person"})-[:KNOWS]->(b {type: "company"}) RETURN id(a), id(b)""")

    def countPushedDownPredicates(p: QueryPlan): Int = p match {
      case QueryPlan.LocalProperty(_, _, PropertyConstraint.Equal(_)) => 1
      case QueryPlan.Anchor(_, onTarget) => countPushedDownPredicates(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countPushedDownPredicates).sum
      case QueryPlan.Sequence(first, andThen, _) =>
        countPushedDownPredicates(first) + countPushedDownPredicates(andThen)
      case QueryPlan.Project(_, _, input) => countPushedDownPredicates(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countPushedDownPredicates(onNeighbor)
      case QueryPlan.Distinct(input) => countPushedDownPredicates(input)
      case QueryPlan.Filter(_, input) => countPushedDownPredicates(input)
      case _ => 0
    }

    // Should have 2 pushed-down predicates (one for a.type, one for b.type)
    val pushedDownCount = countPushedDownPredicates(plan)
    pushedDownCount shouldBe 2
  }

  it should "push down inline property predicates on anonymous nodes" in {
    // Test with inline properties on anonymous nodes - binding is auto-generated
    val plan = planQuery("""MATCH (n)-[:KNOWS]->({type: "target"}) RETURN id(n)""")

    def countPushedDownPredicates(p: QueryPlan): Int = p match {
      case QueryPlan.LocalProperty(_, _, PropertyConstraint.Equal(_)) => 1
      case QueryPlan.Anchor(_, onTarget) => countPushedDownPredicates(onTarget)
      case QueryPlan.CrossProduct(queries, _) => queries.map(countPushedDownPredicates).sum
      case QueryPlan.Sequence(first, andThen, _) =>
        countPushedDownPredicates(first) + countPushedDownPredicates(andThen)
      case QueryPlan.Project(_, _, input) => countPushedDownPredicates(input)
      case QueryPlan.Expand(_, _, onNeighbor) => countPushedDownPredicates(onNeighbor)
      case QueryPlan.Distinct(input) => countPushedDownPredicates(input)
      case QueryPlan.Filter(_, input) => countPushedDownPredicates(input)
      case _ => 0
    }

    // Should have 1 pushed-down predicate for the anonymous node's type property
    val pushedDownCount = countPushedDownPredicates(plan)
    pushedDownCount shouldBe 1
  }

  // ============================================================
  // MULTI-PATH PATTERN WITH SHARED NODES TESTS
  // ============================================================

  "Multi-path patterns with shared nodes" should "merge trees when nodes appear in multiple comma-separated patterns" in {
    // This pattern has two comma-separated paths that share nodes 'b' and 'c':
    // Pattern 1: (a)-[:R]->(b)<-[:R]-(c)
    // Pattern 2: (b)<-[:R]-(c)-[:R]->(d)
    // Shared nodes: b (appears in both), c (appears in both)
    //
    // The planner should merge these into a single tree rooted at a shared node.
    // This is similar to the APT detection output query pattern.
    val plan = planQuery("""
      MATCH (a)-[:R]->(b)<-[:R]-(c), (b)<-[:R]-(c)-[:R]->(d)
      WHERE id(b) = $param
      RETURN a, b, c, d
    """)

    // Should have exactly ONE computed anchor (for b, from the id(b) = $param constraint)
    // If tree merging works correctly, we shouldn't have multiple anchors for shared nodes
    val computedAnchors = countComputedAnchors(plan)
    computedAnchors shouldBe 1

    // Should NOT have any AllNodes anchors (which would indicate failed tree merging)
    containsOperator(
      plan,
      {
        case QueryPlan.Anchor(AnchorTarget.AllNodes, _) => true
        case _ => false
      },
    ) shouldBe false
  }

  it should "show plan structure for APT-like output query pattern" in {
    // Simplified version of the APT detection output query pattern:
    // MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
    //       (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:E]->(ip)
    // WHERE id(f) = $that.data.fileId
    //
    // Shared nodes: f (anchor), p2 (shared between both paths)
    val query = """
      MATCH (p1)-[:E]->(e1)-[:E]->(f)<-[:E]-(e2)<-[:E]-(p2),
            (f)<-[:E]-(e3)<-[:E]-(p2)-[:E]->(e4)-[:E]->(ip)
      WHERE id(f) = $fileId
      RETURN p1, e1, f, e2, p2, e3, e4, ip
    """

    val (parsedQuery, symbolTable) = parseCypher(query)

    val plan = QueryPlanner.plan(parsedQuery, symbolTable)

    // Should have exactly ONE computed anchor (for f)
    val computedAnchors = countComputedAnchors(plan)
    computedAnchors shouldBe 1

    // The key insight: p2 appears in BOTH paths.
    // After merging, the tree rooted at f should have p2 reachable via two paths:
    // 1. f <- e2 <- p2 (binding 5)
    // 2. f <- e3 <- p2 (renamed to fresh binding 10000)
    //
    // The planner should:
    // 1. Rename the second occurrence to a fresh binding
    // 2. Add a diamond join Filter that checks id(fresh) == id(original)

    // Verify there's a diamond join filter (Filter node for id equality)
    val hasFilter = containsOperator(
      plan,
      {
        case QueryPlan.Filter(_, _) => true
        case _ => false
      },
    )
    hasFilter shouldBe true

    // Verify the renamed binding exists (binding 10000 = first fresh binding)
    // Diamond join requires id(renamed) == id(original), so LocalId is emitted
    val hasRenamedBinding = containsOperator(
      plan,
      {
        case QueryPlan.LocalId(sym) if sym.name.contains("10000") => true
        case _ => false
      },
    )
    hasRenamedBinding shouldBe true
  }
}
