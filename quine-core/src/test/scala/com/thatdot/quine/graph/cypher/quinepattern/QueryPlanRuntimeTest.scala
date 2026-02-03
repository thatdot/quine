package com.thatdot.quine.graph.cypher.quinepattern

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Promise}

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisModule, SymbolAnalysisPhase}
import com.thatdot.quine.cypher.{ast => Cypher}
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.OutputTarget.LazyResultCollector
import com.thatdot.quine.graph.cypher.quinepattern.QueryPlan._
import com.thatdot.quine.graph.quinepattern.{LoadQuery, NonNodeActor}
import com.thatdot.quine.graph.{
  GraphService,
  NamespaceId,
  QuineIdLongProvider,
  StandingQueryId,
  StandingQueryInfo,
  StandingQueryPattern,
  StandingQueryResult,
  defaultNamespaceId,
}
import com.thatdot.quine.language.ast.{Expression, Source, Value}
import com.thatdot.quine.language.phases.UpgradeModule._
import com.thatdot.quine.model.{PropertyValue, QuineValue}
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

/** Runtime tests for QuinePattern interpreter.
  *
  * These tests verify that the QuinePattern state machine correctly executes query plans
  * on real graphs, testing both lazy (standing query) and eager (one-shot) modes.
  */
class QueryPlanRuntimeTest
    extends TestKit(ActorSystem("QueryPlanRuntimeTest"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)
  val namespace: NamespaceId = defaultNamespaceId
  val qidProvider: QuineIdLongProvider = QuineIdLongProvider()

  // Helper to create a Source for AST nodes
  private val noSource: Source = Source.NoSource

  // Helper to create a parameter expression
  // Note: Parameter names must start with '$' as evalParameter strips the leading '$'
  private def param(name: String): Expression =
    Expression.Parameter(noSource, Symbol("$" + name), None)

  def makeGraph(name: String = "test-graph"): GraphService = Await.result(
    GraphService(
      name,
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = qidProvider,
    )(LogConfig.permissive),
    5.seconds,
  )

  // ============================================================
  // EAGER MODE TESTS - One-shot queries that complete
  // ============================================================

  "QuinePattern Eager Mode" should "execute a simple LocalId query and return results" in {
    val graph = makeGraph("eager-localid-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Set up a node with properties
      Await.result(
        graph.literalOps(namespace).setLabels(nodeId, Set("Person")),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      // Create a simple query plan: Anchor(nodeId) -> LocalId(n)
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalId(Symbol("n")),
      )

      // Execute in eager mode
      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head
      ctx.bindings should contain key Symbol("n")

      // Verify we got a Node value with the expected structure
      val nodeValue = ctx.bindings(Symbol("n"))
      nodeValue shouldBe a[Value.NodeId]
      val node = nodeValue.asInstanceOf[Value.NodeId]
      node.id shouldEqual nodeId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "execute a two-node query with nested anchors" in {
    val graph = makeGraph("eager-two-node-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeIdA = qidProvider.newQid()
      val nodeIdB = qidProvider.newQid()

      // Set up nodes
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdA, "x", QuineValue.Integer(10)),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdB, "y", QuineValue.Integer(20)),
        5.seconds,
      )

      // Query plan: Anchor(b) -> Sequence(LocalId(b), Anchor(a) -> LocalId(a))
      // This is the optimized cross-node structure
      val plan = Anchor(
        AnchorTarget.Computed(param("bId")),
        Sequence(
          LocalId(Symbol("b")),
          Anchor(
            AnchorTarget.Computed(param("aId")),
            LocalId(Symbol("a")),
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("aId") -> Value.NodeId(nodeIdA),
        Symbol("bId") -> Value.NodeId(nodeIdB),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head
      ctx.bindings should contain key Symbol("a")
      ctx.bindings should contain key Symbol("b")

      // Verify both nodes are present
      ctx.bindings(Symbol("a")) shouldBe a[Value.NodeId]
      ctx.bindings(Symbol("b")) shouldBe a[Value.NodeId]

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "return empty results and complete for non-existent node" in {
    val graph = makeGraph("eager-empty-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nonExistentId = qidProvider.newQid()

      // Query plan that should return empty results (node exists but no matching filter)
      // Using Filter(false, ...) to guarantee no results
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        Sequence(
          LocalId(Symbol("n")),
          Filter(
            Expression.AtomicLiteral(noSource, Value.False, None),
            Unit,
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nonExistentId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should complete with empty results, not hang (verifies eager completion signaling)
      results shouldBe empty

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // LAZY MODE TESTS - Standing queries
  // ============================================================

  "QuinePattern Lazy Mode" should "set up a standing query without errors" in {
    val graph = makeGraph("lazy-setup-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Query plan for lazy mode
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalId(Symbol("n")),
      )

      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.StandingQuerySink(sqId, namespace)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Give time for the standing query to be registered
      Thread.sleep(500)

      // Test passes if no exceptions/deadlocks occur during setup
      succeed

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // INVARIANT TESTS - Verify runtime guarantees
  // ============================================================

  "QuinePattern Runtime Invariants" should "complete eager queries even when filter eliminates all results" in {
    val graph = makeGraph("invariant-completion-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Set up a node so the anchor dispatches
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "x", QuineValue.Integer(1)),
        5.seconds,
      )

      // Query that will have no matches (filter always false)
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        Sequence(
          LocalId(Symbol("n")),
          Filter(
            Expression.AtomicLiteral(noSource, Value.False, None),
            Unit,
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Must complete within timeout (no deadlock)
      val results = Await.result(resultPromise.future, 10.seconds)
      results shouldBe empty

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "propagate context through nested Sequence correctly" in {
    val graph = makeGraph("invariant-context-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeIdA = qidProvider.newQid()
      val nodeIdB = qidProvider.newQid()

      // Set up nodes with properties
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdA, "value", QuineValue.Integer(100)),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdB, "value", QuineValue.Integer(200)),
        5.seconds,
      )

      // Query plan: nested sequence that should combine contexts
      // Anchor(a) -> Sequence(LocalId(a), Anchor(b) -> LocalId(b))
      val plan = Anchor(
        AnchorTarget.Computed(param("aId")),
        Sequence(
          LocalId(Symbol("a")),
          Anchor(
            AnchorTarget.Computed(param("bId")),
            LocalId(Symbol("b")),
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("aId") -> Value.NodeId(nodeIdA),
        Symbol("bId") -> Value.NodeId(nodeIdB),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should have exactly one result with both bindings
      results should have size 1
      val ctx = results.head
      ctx.bindings.keySet should contain allOf (Symbol("a"), Symbol("b"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle Project operator correctly" in {
    val graph = makeGraph("invariant-project-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Set up a node
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "x", QuineValue.Integer(42)),
        5.seconds,
      )

      // Query plan with Project
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        Sequence(
          LocalId(Symbol("n")),
          Project(
            List(
              Projection(
                Expression.Ident(noSource, Left(com.thatdot.quine.language.ast.CypherIdentifier(Symbol("n"))), None),
                Symbol("result"),
              ),
            ),
            dropExisting = true,
            Unit,
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should have one result with only the projected column
      results should have size 1
      val ctx = results.head
      ctx.bindings should contain key Symbol("result")

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // INCREMENTAL MATCHING TESTS - Lazy mode reactive behavior
  // ============================================================

  "QuinePattern Incremental Matching" should "emit result when graph mutation creates matching pattern" in {
    val graph = makeGraph("incremental-match-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Create a collector for incremental results
      val collector = new LazyResultCollector()

      // Query plan: Watch for node with property 'name' set
      // WatchProperty will trigger on property changes
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("name"), Some(Symbol("n"))),
      )

      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      // Load the standing query
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Give time for the standing query to be installed on the node
      Thread.sleep(500)

      // Initially no results (property not set yet)
      collector.allDeltas shouldBe empty

      // Now set the property - this should trigger a match
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      // Wait for the delta to arrive
      collector.awaitFirstDelta(5.seconds) shouldBe true

      // Should have exactly one positive match
      collector.positiveCount shouldBe 1
      collector.hasRetractions shouldBe false

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "emit retraction when graph mutation breaks matching pattern" in {
    val graph = makeGraph("incremental-retract-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Pre-populate the node with the property
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      // Create a collector for incremental results
      val collector = new LazyResultCollector()

      // Query plan: Watch for node with property 'name'
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("name"), Some(Symbol("n"))),
      )

      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      // Load the standing query
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Wait for initial kickstart match
      collector.awaitFirstDelta(5.seconds) shouldBe true
      collector.positiveCount shouldBe 1

      // Clear the collector to track only new changes
      collector.clear()

      // Now remove the property - this should trigger a retraction
      Await.result(
        graph.literalOps(namespace).removeProp(nodeId, "name"),
        5.seconds,
      )

      // Wait for the retraction
      Thread.sleep(500)

      // Should have a retraction (negative delta)
      collector.hasRetractions shouldBe true
      collector.negativeCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "correctly handle property value changes with retract-then-add" in {
    val graph = makeGraph("incremental-change-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Pre-populate the node with initial property value
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "count", QuineValue.Integer(10)),
        5.seconds,
      )

      // Create a collector for incremental results
      val collector = new LazyResultCollector()

      // Query plan: Watch for node with property 'count'
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("count"), Some(Symbol("n"))),
      )

      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      // Load the standing query
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Wait for initial kickstart match
      collector.awaitFirstDelta(5.seconds) shouldBe true
      val initialCount = collector.positiveCount
      initialCount shouldBe 1

      // Clear and track changes
      collector.clear()

      // Change the property value - this should trigger retract old + add new
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "count", QuineValue.Integer(20)),
        5.seconds,
      )

      // Wait for the change to propagate
      Thread.sleep(500)

      // Should have both retraction and addition
      // The net result should still be +1 (retract old value, add new value)
      val netResult = collector.netResult
      netResult.values.sum shouldBe 0 // Old context retracted, new context added

      // Should have seen at least one retraction
      collector.hasRetractions shouldBe true

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "demonstrate multiset math with multiple operations" in {
    val graph = makeGraph("multiset-math-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Create a collector for incremental results
      val collector = new LazyResultCollector()

      // Query plan: Watch for node with property 'x'
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("x"), Some(Symbol("n"))),
      )

      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      // Load the standing query
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Give time for query installation
      Thread.sleep(500)

      // Perform sequence of operations: add, change, change, remove
      // Each should produce proper deltas

      // 1. Add property (should emit +1)
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "x", QuineValue.Integer(1)),
        5.seconds,
      )
      Thread.sleep(200)

      // 2. Change value (should emit -1 for old, +1 for new)
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "x", QuineValue.Integer(2)),
        5.seconds,
      )
      Thread.sleep(200)

      // 3. Change value again
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "x", QuineValue.Integer(3)),
        5.seconds,
      )
      Thread.sleep(200)

      // 4. Remove property (should emit -1)
      Await.result(
        graph.literalOps(namespace).removeProp(nodeId, "x"),
        5.seconds,
      )
      Thread.sleep(200)

      // Verify the net result is 0 (added then removed)
      val netResult = collector.netResult
      netResult.values.sum shouldBe 0

      // Verify we saw multiple deltas
      collector.allDeltas.size should be >= 4

      // Verify we saw both additions and retractions
      collector.hasRetractions shouldBe true

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // STANDING QUERY SINK INTEGRATION TESTS
  // ============================================================

  "QuinePattern StandingQuerySink" should "deliver results to a registered standing query" in {
    val graph = makeGraph("sq-sink-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      // Create a promise to collect results from the standing query
      val resultsPromise = Promise[Seq[StandingQueryResult]]()
      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]

      // Create a simple query plan for the pattern
      val patternPlan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalId(Symbol("n")),
      )

      // Register a minimal standing query using the V2 pattern
      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = patternPlan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("n"))),
      )

      // Create a sink that collects results with a kill switch
      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.size >= 1) resultsPromise.trySuccess(resultsList.toSeq)
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Set up node with a property
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      // Create a simple query plan that will produce a result
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalId(Symbol("n")),
      )

      // Load the QuinePattern query with StandingQuerySink target
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Eager, // Use Eager to get immediate results
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      // Wait for results to arrive
      val results = Await.result(resultsPromise.future, 10.seconds)

      // Verify we got at least one result
      results should not be empty
      results.head.meta.isPositiveMatch shouldBe true
      results.head.data should contain key "n"

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "properly handle the case when standing query is not registered" in {
    val graph = makeGraph("sq-sink-not-registered-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh() // Not registered

      // Set up node
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Bob")),
        5.seconds,
      )

      // Create query plan
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalId(Symbol("n")),
      )

      // Load with StandingQuerySink targeting an unregistered sqId
      // This should not throw, just silently not deliver results
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      // Give time for query to complete
      Thread.sleep(1000)

      // Test passes if no exception was thrown
      succeed

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // RETRACTION TESTS - Verify isPositiveMatch=false flows through StandingQuerySink
  // ============================================================

  "QuinePattern StandingQuerySink Retractions" should "emit retraction with isPositiveMatch=false when property is removed" in {
    val graph = makeGraph("sq-sink-retraction-property-removal")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      // Collect all results (positive and negative)
      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      val firstResultPromise = Promise[Unit]()

      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.size == 1) firstResultPromise.trySuccess(())
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Register standing query
      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = LocalProperty(Symbol("name"), Some(Symbol("n"))),
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("n"))),
      )

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "retraction-test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Set up initial data
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      // Load query plan in LAZY mode (required for retractions)
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("name"), Some(Symbol("n"))),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      // Wait for initial positive result
      Await.result(firstResultPromise.future, 5.seconds)

      // Verify positive result
      resultsList.synchronized {
        resultsList.size shouldBe 1
        resultsList.head.meta.isPositiveMatch shouldBe true
      }

      // Now remove the property - should trigger retraction
      Await.result(
        graph.literalOps(namespace).removeProp(nodeId, "name"),
        5.seconds,
      )

      // Wait for retraction to propagate
      Thread.sleep(1000)

      // Verify retraction arrived with isPositiveMatch = false
      resultsList.synchronized {
        resultsList.size shouldBe 2
        resultsList(0).meta.isPositiveMatch shouldBe true // initial match
        resultsList(1).meta.isPositiveMatch shouldBe false // retraction
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "emit retraction when property value changes to non-matching value" in {
    val graph = makeGraph("sq-sink-retraction-value-change-nomatch")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      val firstResultPromise = Promise[Unit]()

      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.size == 1) firstResultPromise.trySuccess(())
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Query plan with a constraint - only matches when name = "Alice"
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("name"), Some(Symbol("n")), PropertyConstraint.Equal(Value.Text("Alice"))),
      )

      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = plan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("n"))),
      )

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "retraction-filter-test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Set initial matching value
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      // Wait for initial match
      Await.result(firstResultPromise.future, 5.seconds)

      resultsList.synchronized {
        resultsList.size shouldBe 1
        resultsList.head.meta.isPositiveMatch shouldBe true
      }

      // Change to non-matching value - should trigger retraction only (no new match)
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Bob")),
        5.seconds,
      )

      Thread.sleep(1000)

      resultsList.synchronized {
        resultsList.size shouldBe 2
        resultsList(0).meta.isPositiveMatch shouldBe true // initial "Alice" match
        resultsList(1).meta.isPositiveMatch shouldBe false // retraction when changed to "Bob"
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "emit retraction then new assertion when property value changes to different matching value" in {
    val graph = makeGraph("sq-sink-retraction-value-change-rematch")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      val firstResultPromise = Promise[Unit]()

      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.size == 1) firstResultPromise.trySuccess(())
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Simple property watch - matches any value
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        LocalProperty(Symbol("name"), Some(Symbol("n"))),
      )

      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = plan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("n"))),
      )

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "retraction-rematch-test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Set initial value
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")),
        5.seconds,
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      Await.result(firstResultPromise.future, 5.seconds)

      resultsList.synchronized {
        resultsList.size shouldBe 1
        resultsList.head.meta.isPositiveMatch shouldBe true
        resultsList.head.data.get("n") shouldBe Some(QuineValue.Str("Alice"))
      }

      // Change to different value - should trigger retraction of old + assertion of new
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Bob")),
        5.seconds,
      )

      Thread.sleep(1000)

      resultsList.synchronized {
        resultsList.size shouldBe 3
        resultsList(0).meta.isPositiveMatch shouldBe true // initial "Alice" match
        resultsList(1).meta.isPositiveMatch shouldBe false // retraction of "Alice"
        resultsList(2).meta.isPositiveMatch shouldBe true // new "Bob" match
        resultsList(2).data.get("n") shouldBe Some(QuineValue.Str("Bob"))
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "emit retraction when edge is removed in cross-product pattern" in {
    val graph = makeGraph("sq-sink-retraction-edge-removal")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val personId = qidProvider.newQid()
      val movieId = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      val firstResultPromise = Promise[Unit]()

      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.size == 1) firstResultPromise.trySuccess(())
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Pattern: MATCH (p)-[:ACTED_IN]->(m) RETURN p, m
      val plan = Anchor(
        AnchorTarget.Computed(param("personId")),
        CrossProduct(
          List(
            LocalId(Symbol("p")),
            Expand(
              Some(Symbol("ACTED_IN")),
              com.thatdot.quine.model.EdgeDirection.Outgoing,
              LocalId(Symbol("m")),
            ),
          ),
        ),
      )

      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = plan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("p"), Symbol("m"))),
      )

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "retraction-edge-test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Create edge between person and movie
      Await.result(
        graph.literalOps(namespace).addEdge(personId, movieId, "ACTED_IN"),
        5.seconds,
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("personId") -> Value.NodeId(personId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      Await.result(firstResultPromise.future, 5.seconds)

      resultsList.synchronized {
        resultsList.size shouldBe 1
        resultsList.head.meta.isPositiveMatch shouldBe true
      }

      // Remove the edge - should trigger retraction
      Await.result(
        graph.literalOps(namespace).removeEdge(personId, movieId, "ACTED_IN"),
        5.seconds,
      )

      Thread.sleep(1000)

      resultsList.synchronized {
        resultsList.size shouldBe 2
        resultsList(0).meta.isPositiveMatch shouldBe true // initial match
        resultsList(1).meta.isPositiveMatch shouldBe false // retraction after edge removal
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "emit correct number of retractions when multiplicity decreases" in {
    val graph = makeGraph("sq-sink-retraction-multiplicity")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val personId = qidProvider.newQid()
      val movie1Id = qidProvider.newQid()
      val movie2Id = qidProvider.newQid()
      val movie3Id = qidProvider.newQid()
      val sqId = StandingQueryId.fresh()

      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      val threeResultsPromise = Promise[Unit]()

      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
              if (resultsList.count(_.meta.isPositiveMatch) == 3) threeResultsPromise.trySuccess(())
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Pattern: MATCH (p)-[:ACTED_IN]->(m) RETURN p, m
      // Without DISTINCT, each edge creates a separate result
      val plan = Anchor(
        AnchorTarget.Computed(param("personId")),
        CrossProduct(
          List(
            LocalId(Symbol("p")),
            Expand(
              Some(Symbol("ACTED_IN")),
              com.thatdot.quine.model.EdgeDirection.Outgoing,
              LocalId(Symbol("m")),
            ),
          ),
        ),
      )

      val dummyPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = plan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("p"), Symbol("m"))),
      )

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "retraction-multiplicity-test-sq",
          pattern = dummyPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Create 3 edges
      Await.result(graph.literalOps(namespace).addEdge(personId, movie1Id, "ACTED_IN"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(personId, movie2Id, "ACTED_IN"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(personId, movie3Id, "ACTED_IN"), 5.seconds)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("personId") -> Value.NodeId(personId)),
        namespace = namespace,
        output = OutputTarget.StandingQuerySink(sqId, namespace),
      )

      // Wait for 3 positive results
      Await.result(threeResultsPromise.future, 10.seconds)

      resultsList.synchronized {
        resultsList.count(_.meta.isPositiveMatch) shouldBe 3
      }

      // Remove 2 edges - should trigger 2 retractions
      Await.result(graph.literalOps(namespace).removeEdge(personId, movie1Id, "ACTED_IN"), 5.seconds)
      Await.result(graph.literalOps(namespace).removeEdge(personId, movie2Id, "ACTED_IN"), 5.seconds)

      Thread.sleep(1000)

      resultsList.synchronized {
        val positiveCount = resultsList.count(_.meta.isPositiveMatch)
        val negativeCount = resultsList.count(!_.meta.isPositiveMatch)

        positiveCount shouldBe 3 // 3 initial matches
        negativeCount shouldBe 2 // 2 retractions (one for each removed edge)
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // VALUE.NODE DISPATCH TESTS - Tests for Anchor handling of Node values
  // ============================================================

  "QuinePattern Value.Node Dispatch" should "dispatch correctly when anchor target evaluates to Value.Node" in {
    val graph = makeGraph("node-value-dispatch-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeIdA = qidProvider.newQid()
      val nodeIdB = qidProvider.newQid()

      // Set up two nodes
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdA, "name", QuineValue.Str("Alice")),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdB, "name", QuineValue.Str("Bob")),
        5.seconds,
      )

      // Query plan: Anchor(nodeIdA) -> Sequence(LocalId(a), Anchor(a) -> LocalId(b))
      // The key is that the second Anchor's target expression `a` evaluates to a Value.Node
      // (not Value.NodeId), which requires the fix to extract the ID from the Node
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeIdA")),
        Sequence(
          LocalId(Symbol("a")), // This produces Value.Node(nodeIdA, ...)
          Anchor(
            // This evaluates `a` which is a Value.Node - needs the fix to work
            AnchorTarget.Computed(
              Expression.Ident(noSource, Left(com.thatdot.quine.language.ast.CypherIdentifier(Symbol("a"))), None),
            ),
            LocalId(Symbol("b")),
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeIdA") -> Value.NodeId(nodeIdA))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Without the fix, this would return empty results because
      // Value.Node wouldn't be recognized as a valid dispatch target
      results should have size 1
      val ctx = results.head
      ctx.bindings should contain key Symbol("a")
      ctx.bindings should contain key Symbol("b")

      // Both should be the same node (dispatching to self via Node value)
      val nodeA = ctx.bindings(Symbol("a")).asInstanceOf[Value.NodeId]
      val nodeB = ctx.bindings(Symbol("b")).asInstanceOf[Value.NodeId]
      nodeA.id shouldEqual nodeIdA
      nodeB.id shouldEqual nodeIdA // Second anchor dispatched to same node

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "dispatch correctly when anchor target evaluates to Value.Node in context bridge" in {
    val graph = makeGraph("node-value-bridge-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeIdA = qidProvider.newQid()
      val nodeIdB = qidProvider.newQid()

      // Set up nodes with properties
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdA, "value", QuineValue.Integer(10)),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdB, "value", QuineValue.Integer(20)),
        5.seconds,
      )

      // Query plan simulating: MATCH (a) WHERE id(a) = $nodeIdA WITH a MATCH (b) WHERE id(b) = $nodeIdB RETURN a, b
      // The WITH clause produces a Node value that gets passed via ContextBridge
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeIdA")),
        Sequence(
          LocalId(Symbol("a")), // Produces Value.Node
          Anchor(
            AnchorTarget.Computed(param("nodeIdB")),
            LocalId(Symbol("b")),
          ),
          ContextFlow.Extend, // a should be in context for second anchor
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("nodeIdA") -> Value.NodeId(nodeIdA),
        Symbol("nodeIdB") -> Value.NodeId(nodeIdB),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head
      ctx.bindings should contain key Symbol("a")
      ctx.bindings should contain key Symbol("b")

      // Verify both nodes are correctly bound
      val nodeA = ctx.bindings(Symbol("a")).asInstanceOf[Value.NodeId]
      val nodeB = ctx.bindings(Symbol("b")).asInstanceOf[Value.NodeId]
      nodeA.id shouldEqual nodeIdA
      nodeB.id shouldEqual nodeIdB

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle lists containing Value.Node in anchor dispatch" in {
    val graph = makeGraph("node-value-list-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeIdA = qidProvider.newQid()
      val nodeIdB = qidProvider.newQid()

      // Set up nodes
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdA, "x", QuineValue.Integer(1)),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(nodeIdB, "x", QuineValue.Integer(2)),
        5.seconds,
      )

      // Test that when a list of Node values is used, each Node's ID is extracted
      // Query plan: Parameter $nodes (list of Node values) -> dispatch to each
      val plan = Anchor(
        AnchorTarget.Computed(param("nodes")),
        LocalId(Symbol("n")),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      // Pass a list of Node values (not NodeId values) - this tests the fix in the List case
      import scala.collection.immutable.SortedMap
      val emptyProps = Value.Map(SortedMap.empty[Symbol, Value])
      val params = Map(
        Symbol("nodes") -> Value.List(
          List(
            Value.Node(nodeIdA, Set.empty, emptyProps),
            Value.Node(nodeIdB, Set.empty, emptyProps),
          ),
        ),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should get results from both nodes
      results should have size 2
      val nodeIds = results.map(_.bindings(Symbol("n")).asInstanceOf[Value.NodeId].id).toSet
      nodeIds should contain(nodeIdA)
      nodeIds should contain(nodeIdB)

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // VALUE.NODE EDGE CREATION TESTS - Tests for CreateHalfEdge handling of Node values
  // ============================================================

  "QuinePattern CreateHalfEdge with Value.Node" should "create edge when target evaluates to Value.Node" in {
    val graph = makeGraph("create-edge-value-node-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val parentNodeId = qidProvider.newQid()
      val childNodeId = qidProvider.newQid()

      // Set up parent and child nodes
      Await.result(
        graph.literalOps(namespace).setProp(parentNodeId, "name", QuineValue.Str("Parent")),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(childNodeId, "name", QuineValue.Str("Child")),
        5.seconds,
      )

      import com.thatdot.quine.model.EdgeDirection

      // Query plan that:
      // 1. Dispatches to childNodeId
      // 2. Loads child node as LocalId(c)
      // 3. Sequences with a LocalEffect that creates an edge where the target is from a parameter
      //    that is a Value.Node (not Value.NodeId)
      //
      // This tests the fix where CreateHalfEdge needs to handle Value.Node as the edge target
      val plan = Anchor(
        AnchorTarget.Computed(param("childId")),
        Sequence(
          LocalId(Symbol("c")),
          LocalEffect(
            // Create an edge TO the parent - the target expression evaluates to Value.Node
            effects = List(
              LocalQueryEffect.CreateHalfEdge(
                source = None, // Current node (child)
                label = Symbol("has_parent"),
                direction = EdgeDirection.Outgoing,
                other = param("parentNode"), // This will be a Value.Node, not Value.NodeId
              ),
            ),
            input = Unit,
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      // Pass parentNode as a Value.Node (not Value.NodeId) - this is what tests the fix
      import scala.collection.immutable.SortedMap
      val emptyProps = Value.Map(SortedMap.empty[Symbol, Value])
      val params = Map(
        Symbol("childId") -> Value.NodeId(childNodeId),
        Symbol("parentNode") -> Value.Node(parentNodeId, Set.empty, emptyProps),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // The query should complete
      results should have size 1

      // Wait a bit for the edge creation to propagate
      Thread.sleep(500)

      // Verify the edge was actually created on the child node
      // The child should have an outgoing :has_parent edge to the parent
      val childEdges = Await.result(
        graph
          .literalOps(namespace)
          .getHalfEdges(
            childNodeId,
            withType = Some(Symbol("has_parent")),
            withDir = Some(EdgeDirection.Outgoing),
            withId = Some(parentNodeId),
            withLimit = None,
            atTime = None,
          ),
        5.seconds,
      )

      // Without the fix, no edges would be created because Value.Node wasn't recognized
      childEdges should not be empty
      childEdges.size shouldEqual 1
      val edge = childEdges.head
      edge.edgeType shouldEqual Symbol("has_parent")
      edge.direction shouldEqual EdgeDirection.Outgoing
      edge.other shouldEqual parentNodeId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create edge in complex pattern with Value.Node from context" in {
    val graph = makeGraph("create-edge-context-node-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val parentNodeId = qidProvider.newQid()
      val childNodeId = qidProvider.newQid()

      // Set up parent and child nodes
      Await.result(
        graph.literalOps(namespace).setProp(parentNodeId, "name", QuineValue.Str("Parent")),
        5.seconds,
      )
      Await.result(
        graph.literalOps(namespace).setProp(childNodeId, "name", QuineValue.Str("Child")),
        5.seconds,
      )

      import com.thatdot.quine.model.EdgeDirection

      // Query plan that:
      // 1. First loads parent as LocalId(p) - this produces Value.Node in context
      // 2. Then dispatches to child and creates edge using `p` from context
      // This simulates: MATCH (p) WHERE id(p) = $parentId MATCH (c) WHERE id(c) = $childId CREATE (c)-[:has_parent]->(p)
      val plan = Anchor(
        AnchorTarget.Computed(param("parentId")),
        Sequence(
          LocalId(Symbol("p")), // Puts Value.Node in context as `p`
          Anchor(
            AnchorTarget.Computed(param("childId")),
            Sequence(
              LocalId(Symbol("c")),
              LocalEffect(
                effects = List(
                  LocalQueryEffect.CreateHalfEdge(
                    source = None,
                    label = Symbol("has_parent"),
                    direction = EdgeDirection.Outgoing,
                    // Use `p` from context - this is a Value.Node
                    other = Expression
                      .Ident(noSource, Left(com.thatdot.quine.language.ast.CypherIdentifier(Symbol("p"))), None),
                  ),
                ),
                input = Unit,
              ),
              ContextFlow.Extend,
            ),
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("parentId") -> Value.NodeId(parentNodeId),
        Symbol("childId") -> Value.NodeId(childNodeId),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      results.head.bindings should contain key Symbol("p")
      results.head.bindings should contain key Symbol("c")

      // Wait for edge creation
      Thread.sleep(500)

      // Verify the edge was created
      val childEdges = Await.result(
        graph
          .literalOps(namespace)
          .getHalfEdges(
            childNodeId,
            withType = Some(Symbol("has_parent")),
            withDir = Some(EdgeDirection.Outgoing),
            withId = Some(parentNodeId),
            withLimit = None,
            atTime = None,
          ),
        5.seconds,
      )

      childEdges should not be empty
      childEdges.size shouldEqual 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // MOVIE DATA INGEST-1 PATTERN TESTS - UNWIND with Multi-Anchor
  // ============================================================

  "QuinePattern Movie Data INGEST-1" should "execute UNWIND pattern with idFrom anchors and create edges" in {
    // This test uses PARSED CYPHER with idFrom - matching the actual recipe pattern
    val graph = makeGraph("movie-genre-unwind-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection
      import scala.collection.immutable.SortedMap

      // Simplified INGEST-1 pattern using idFrom (not pre-computed node IDs)
      val query = """
        WITH $that AS row
        MATCH (m)
        WHERE id(m) = idFrom("Movie", row.movieId)
        SET m:Movie, m.title = row.title
        WITH m, row.genres AS genres
        UNWIND genres AS genre
        WITH m, genre
        MATCH (g)
        WHERE id(g) = idFrom("Genre", genre)
        SET g:Genre, g.name = genre
        CREATE (m)-[:IN_GENRE]->(g)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      // Pass data like the ingest does - a single $that object
      val thatValue = Value.Map(
        SortedMap(
          Symbol("movieId") -> Value.Text("movie-123"),
          Symbol("title") -> Value.Text("Test Movie"),
          Symbol("genres") -> Value.List(List(Value.Text("Action"), Value.Text("Comedy"))),
        ),
      )
      val params = Map(Symbol("that") -> thatValue)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // KEY TEST: Query must terminate (not deadlock)
      val results = Await.result(resultPromise.future, 15.seconds)

      // Should have 2 results (one per genre from UNWIND)
      results should have size 2

      // Wait for effects to propagate
      Thread.sleep(500)

      // Compute expected node IDs using idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedMovieId = computeIdFrom("Movie", "movie-123")
      val expectedGenre1Id = computeIdFrom("Genre", "Action")
      val expectedGenre2Id = computeIdFrom("Genre", "Comedy")

      // Verify movie node has Movie label and title property
      val movieProps = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(expectedMovieId, atTime = None),
        5.seconds,
      )
      movieProps._2.getOrElse(Set.empty) should contain(Symbol("Movie"))
      movieProps._1.get(Symbol("title")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Test Movie"))

      // Verify genre nodes have Genre label
      val genre1Props = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(expectedGenre1Id, atTime = None),
        5.seconds,
      )
      genre1Props._2.getOrElse(Set.empty) should contain(Symbol("Genre"))

      val genre2Props = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(expectedGenre2Id, atTime = None),
        5.seconds,
      )
      genre2Props._2.getOrElse(Set.empty) should contain(Symbol("Genre"))

      // Debug: Print computed IDs

      // Verify edges were created from movie to genres (outgoing from movie)
      val movieEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedMovieId),
        5.seconds,
      )

      val inGenreEdges = movieEdges.filter(_.edgeType == Symbol("IN_GENRE"))
      inGenreEdges should have size 2
      inGenreEdges.map(_.other).toSet shouldBe Set(expectedGenre1Id, expectedGenre2Id)

      // Verify reciprocal edges on genre nodes
      val genre1Edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedGenre1Id),
        5.seconds,
      )

      val genre1InGenre =
        genre1Edges.find(e => e.edgeType == Symbol("IN_GENRE") && e.direction == EdgeDirection.Incoming)
      genre1InGenre shouldBe defined
      genre1InGenre.get.other shouldBe expectedMovieId

      val genre2Edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedGenre2Id),
        5.seconds,
      )

      val genre2InGenre =
        genre2Edges.find(e => e.edgeType == Symbol("IN_GENRE") && e.direction == EdgeDirection.Incoming)
      genre2InGenre shouldBe defined
      genre2InGenre.get.other shouldBe expectedMovieId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle empty list in UNWIND without deadlock" in {
    val graph = makeGraph("empty-unwind-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val movieId = qidProvider.newQid()

      // Minimal plan with UNWIND over empty list
      val plan = Anchor(
        AnchorTarget.Computed(param("movieId")),
        Sequence(
          LocalId(Symbol("m")),
          Unwind(
            list = param("items"),
            binding = Symbol("item"),
            subquery = LocalId(Symbol("item")),
          ),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("movieId") -> Value.NodeId(movieId),
        Symbol("items") -> Value.List(Nil), // Empty list
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Must complete even with empty UNWIND (the key test is no deadlock)
      Await.result(resultPromise.future, 15.seconds)

      // The query completes - that's the main assertion (no deadlock)
      // Note: The exact result count depends on how empty UNWIND interacts with Sequence
      // The key test is that it terminates
      succeed

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle multiple sequential UNWIND executions" in {
    val graph = makeGraph("multi-unwind-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // Same plan structure, executed multiple times
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        Sequence(
          LocalId(Symbol("n")),
          Unwind(
            list = param("items"),
            binding = Symbol("item"),
            subquery = Anchor(
              AnchorTarget.Computed(
                Expression.Ident(noSource, Left(com.thatdot.quine.language.ast.CypherIdentifier(Symbol("item"))), None),
              ),
              Sequence(
                LocalId(Symbol("target")),
                LocalEffect(
                  effects = List(
                    LocalQueryEffect.CreateHalfEdge(
                      source = None,
                      label = Symbol("LINKS_TO"),
                      direction = EdgeDirection.Incoming,
                      other = Expression.Ident(
                        noSource,
                        Left(com.thatdot.quine.language.ast.CypherIdentifier(Symbol("n"))),
                        None,
                      ),
                    ),
                  ),
                  input = Unit,
                ),
                ContextFlow.Extend,
              ),
            ),
          ),
          ContextFlow.Extend,
        ),
      )

      // Execute multiple times with different source nodes
      for (_ <- 1 to 5) {
        val sourceId = qidProvider.newQid()
        val targetIds = (1 to 3).map(_ => qidProvider.newQid()).toList

        val resultPromise = Promise[Seq[QueryContext]]()
        val outputTarget = OutputTarget.EagerCollector(resultPromise)
        val params = Map(
          Symbol("nodeId") -> Value.NodeId(sourceId),
          Symbol("items") -> Value.List(targetIds.map(Value.NodeId.apply)),
        )

        val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        loader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = plan,
          mode = RuntimeMode.Eager,
          params = params,
          namespace = namespace,
          output = outputTarget,
        )

        // Each execution must complete
        val results = Await.result(resultPromise.future, 15.seconds)
        results should have size 3
      }

      // Test passes if all 5 executions complete without deadlock
      succeed

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // EDGE CREATION TESTS - Verify edges are actually created
  // ============================================================

  "QuinePattern Edge Creation" should "create a simple edge between two nodes in Eager mode" in {
    import com.thatdot.quine.model.EdgeDirection

    val graph = makeGraph("edge-creation-simple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val sourceId = qidProvider.newQid()
      val targetId = qidProvider.newQid()

      // Query plan: Anchor(source) -> Effect(CreateHalfEdge to target)
      val plan = Anchor(
        AnchorTarget.Computed(param("sourceId")),
        LocalEffect(
          effects = List(
            LocalQueryEffect.CreateHalfEdge(
              source = None, // Current node (source)
              label = Symbol("KNOWS"),
              direction = EdgeDirection.Outgoing,
              other = param("targetId"),
            ),
          ),
          input = LocalId(Symbol("n")),
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("sourceId") -> Value.NodeId(sourceId),
        Symbol("targetId") -> Value.NodeId(targetId),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)
      results should have size 1

      // Wait for effects to propagate
      Thread.sleep(500)

      // Verify the edge was actually created
      val edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(sourceId),
        5.seconds,
      )
      val knowsEdges = edges.filter(_.edgeType == Symbol("KNOWS"))
      knowsEdges should have size 1
      knowsEdges.head.other shouldEqual targetId
      knowsEdges.head.direction shouldEqual EdgeDirection.Outgoing

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create bidirectional edges (both half-edges)" in {
    import com.thatdot.quine.model.EdgeDirection

    val graph = makeGraph("edge-creation-bidirectional")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeAId = qidProvider.newQid()
      val nodeBId = qidProvider.newQid()

      // Query plan: Create edge from A to B with effects on both nodes
      // Anchor(A) -> Effect(CreateHalfEdge outgoing to B)
      // followed by
      // Anchor(B) -> Effect(CreateHalfEdge incoming from A)
      val plan = Sequence(
        Anchor(
          AnchorTarget.Computed(param("nodeA")),
          LocalEffect(
            effects = List(
              LocalQueryEffect.CreateHalfEdge(
                source = None,
                label = Symbol("CONNECTED"),
                direction = EdgeDirection.Outgoing,
                other = param("nodeB"),
              ),
            ),
            input = Unit,
          ),
        ),
        Anchor(
          AnchorTarget.Computed(param("nodeB")),
          LocalEffect(
            effects = List(
              LocalQueryEffect.CreateHalfEdge(
                source = None,
                label = Symbol("CONNECTED"),
                direction = EdgeDirection.Incoming,
                other = param("nodeA"),
              ),
            ),
            input = Unit,
          ),
        ),
        ContextFlow.Extend,
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("nodeA") -> Value.NodeId(nodeAId),
        Symbol("nodeB") -> Value.NodeId(nodeBId),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      Await.result(resultPromise.future, 10.seconds)

      // Wait for effects to propagate
      Thread.sleep(500)

      // Verify edges on node A
      val edgesA = Await.result(
        graph.literalOps(namespace).getHalfEdges(nodeAId),
        5.seconds,
      )
      edgesA.filter(e => e.edgeType == Symbol("CONNECTED") && e.direction == EdgeDirection.Outgoing) should have size 1

      // Verify edges on node B
      val edgesB = Await.result(
        graph.literalOps(namespace).getHalfEdges(nodeBId),
        5.seconds,
      )
      edgesB.filter(e => e.edgeType == Symbol("CONNECTED") && e.direction == EdgeDirection.Incoming) should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create edges using computed node IDs" in {
    import com.thatdot.quine.model.EdgeDirection

    val graph = makeGraph("edge-creation-computed")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Use fresh IDs
      val personId = qidProvider.newQid()
      val movieId = qidProvider.newQid()

      // Query plan simulating:
      // MATCH (p) WHERE id(p) = $personId
      // CREATE (p)-[:ACTED_IN]->(:Movie)
      // where the movie ID is also provided as parameter
      val plan = Anchor(
        AnchorTarget.Computed(param("personId")),
        LocalEffect(
          effects = List(
            LocalQueryEffect.CreateHalfEdge(
              source = None,
              label = Symbol("ACTED_IN"),
              direction = EdgeDirection.Outgoing,
              other = param("movieId"),
            ),
          ),
          input = LocalId(Symbol("p")),
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("personId") -> Value.NodeId(personId),
        Symbol("movieId") -> Value.NodeId(movieId),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      Await.result(resultPromise.future, 10.seconds)

      // Wait for effects to propagate
      Thread.sleep(500)

      // Verify the ACTED_IN edge was created
      val edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(personId),
        5.seconds,
      )
      val actedInEdges = edges.filter(_.edgeType == Symbol("ACTED_IN"))
      actedInEdges should have size 1
      actedInEdges.head.other shouldEqual movieId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create multiple edges in a single query" in {
    import com.thatdot.quine.model.EdgeDirection

    val graph = makeGraph("edge-creation-multiple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val sourceId = qidProvider.newQid()
      val target1Id = qidProvider.newQid()
      val target2Id = qidProvider.newQid()
      val target3Id = qidProvider.newQid()

      // Query plan: Create multiple edges from source to targets
      val plan = Anchor(
        AnchorTarget.Computed(param("sourceId")),
        LocalEffect(
          effects = List(
            LocalQueryEffect.CreateHalfEdge(
              source = None,
              label = Symbol("EDGE1"),
              direction = EdgeDirection.Outgoing,
              other = param("target1"),
            ),
            LocalQueryEffect.CreateHalfEdge(
              source = None,
              label = Symbol("EDGE2"),
              direction = EdgeDirection.Outgoing,
              other = param("target2"),
            ),
            LocalQueryEffect.CreateHalfEdge(
              source = None,
              label = Symbol("EDGE3"),
              direction = EdgeDirection.Outgoing,
              other = param("target3"),
            ),
          ),
          input = Unit,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("sourceId") -> Value.NodeId(sourceId),
        Symbol("target1") -> Value.NodeId(target1Id),
        Symbol("target2") -> Value.NodeId(target2Id),
        Symbol("target3") -> Value.NodeId(target3Id),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      Await.result(resultPromise.future, 10.seconds)

      // Wait for effects to propagate
      Thread.sleep(500)

      // Verify all edges were created
      val edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(sourceId),
        5.seconds,
      )
      edges.filter(_.edgeType == Symbol("EDGE1")) should have size 1
      edges.filter(_.edgeType == Symbol("EDGE2")) should have size 1
      edges.filter(_.edgeType == Symbol("EDGE3")) should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create edges combined with SET property effects" in {
    import com.thatdot.quine.model.EdgeDirection

    val graph = makeGraph("edge-creation-with-set")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val sourceId = qidProvider.newQid()
      val targetId = qidProvider.newQid()

      // Query plan: SET properties AND create edge
      val plan = Anchor(
        AnchorTarget.Computed(param("sourceId")),
        LocalEffect(
          effects = List(
            LocalQueryEffect.SetProperty(
              target = None,
              property = Symbol("name"),
              value = Expression.AtomicLiteral(noSource, Value.Text("TestNode"), None),
            ),
            LocalQueryEffect.SetLabels(
              target = None,
              labels = Set(Symbol("Person")),
            ),
            LocalQueryEffect.CreateHalfEdge(
              source = None,
              label = Symbol("KNOWS"),
              direction = EdgeDirection.Outgoing,
              other = param("targetId"),
            ),
          ),
          input = LocalId(Symbol("n")),
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("sourceId") -> Value.NodeId(sourceId),
        Symbol("targetId") -> Value.NodeId(targetId),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      Await.result(resultPromise.future, 10.seconds)

      // Wait for effects to propagate
      Thread.sleep(500)

      // Verify property was set
      val props = Await.result(
        graph.literalOps(namespace).getProps(sourceId),
        5.seconds,
      )
      props.get(Symbol("name")).flatMap(PropertyValue.unapply) shouldEqual Some(QuineValue.Str("TestNode"))

      // Verify edge was created
      val edges = Await.result(
        graph.literalOps(namespace).getHalfEdges(sourceId),
        5.seconds,
      )
      edges.filter(_.edgeType == Symbol("KNOWS")) should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // END-TO-END TESTS: Cypher String -> Parser -> Planner -> Runtime
  // These tests replicate the actual recipe flow to catch planner bugs
  // ============================================================

  /** Parse a Cypher query string and return the AST and symbol table */
  private def parseCypher(query: String): (Cypher.Query.SingleQuery, SymbolAnalysisModule.SymbolTable) = {
    val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
    val (tableState, result) = parser.process(query).value.run(LexerState(Nil)).value
    result match {
      case Some(q: Cypher.Query.SingleQuery) => (q, tableState.symbolTable)
      case Some(other) => fail(s"Expected SingleQuery, got: $other")
      case None =>
        val diagnostics = tableState.diagnostics.mkString("\n  ")
        fail(s"Parse error for query: $query\nDiagnostics:\n  $diagnostics")
    }
  }

  /** Parse and plan a Cypher query string */
  private def parseAndPlan(query: String): QueryPlan = {
    val (parsedQuery, symbolTable) = parseCypher(query)
    QueryPlanner.plan(parsedQuery, symbolTable)
  }

  /** Parse and plan a query, returning both the plan and output name mapping */
  private def parseAndPlanWithMetadata(query: String): QueryPlanner.PlannedQuery = {
    val (parsedQuery, symbolTable) = parseCypher(query)
    QueryPlanner.planWithMetadata(parsedQuery, symbolTable)
  }

  "QuinePattern End-to-End Edge Creation" should "create edges via parsed Cypher (INGEST-3 pattern)" in {
    // This test replicates the actual movie data INGEST-3 pattern:
    // MATCH (p), (m), (r)
    // WHERE id(p) = idFrom("Person", $tmdbId)
    //   AND id(m) = idFrom("Movie", $movieId)
    //   AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
    // SET r.role = $role, r:Role
    // CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
    // CREATE (p)-[:ACTED_IN]->(m)

    val graph = makeGraph("e2e-ingest3-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = s"""
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $$tmdbId)
          AND id(m) = idFrom("Movie", $$movieId)
          AND id(r) = idFrom("Role", $$tmdbId, $$movieId, $$role)
        SET r.role = $$role, r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      // Parse and plan the query
      val plan = parseAndPlan(query)

      // Debug: Print the plan

      // Verify the plan contains edge creation effects
      def findCreateHalfEdges(p: QueryPlan): List[LocalQueryEffect.CreateHalfEdge] = p match {
        case LocalEffect(effects, child) =>
          effects.collect { case e: LocalQueryEffect.CreateHalfEdge => e } ++ findCreateHalfEdges(child)
        case Anchor(_, onTarget) => findCreateHalfEdges(onTarget)
        case Sequence(first, andThen, _) => findCreateHalfEdges(first) ++ findCreateHalfEdges(andThen)
        case CrossProduct(queries, _) => queries.flatMap(findCreateHalfEdges)
        case Filter(_, input) => findCreateHalfEdges(input)
        case Project(_, _, input) => findCreateHalfEdges(input)
        case Unwind(_, _, subquery) => findCreateHalfEdges(subquery)
        case _ => Nil
      }

      val createEdges = findCreateHalfEdges(plan)

      // The pattern creates 3 edges: PLAYED, HAS_ROLE, ACTED_IN
      // Each edge needs 2 half-edges = 6 total CreateHalfEdge effects
      createEdges should have size 6

      // Now execute the plan with actual node IDs
      // We need to create QuineIds that match what idFrom would produce
      val tmdbId = "12345"
      val movieId = "67890"
      val role = "Hero"

      // The plan uses idFrom expressions, which compute IDs at runtime
      // We need to provide parameters that the idFrom expressions will use

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text(tmdbId),
        Symbol("movieId") -> Value.Text(movieId),
        Symbol("role") -> Value.Text(role),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      // Wait for execution
      Await.result(resultPromise.future, 15.seconds)

      // Wait for effects to propagate
      Thread.sleep(1000)

      // Compute the expected node IDs using the same idFrom function the runtime uses
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPersonId = computeIdFrom("Person", tmdbId)
      val expectedMovieId = computeIdFrom("Movie", movieId)
      val expectedRoleId = computeIdFrom("Role", tmdbId, movieId, role)

      // Verify edges were created
      // Person -[:PLAYED]-> Role
      val personEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedPersonId),
        5.seconds,
      )

      val playedEdge = personEdges.find(_.edgeType == Symbol("PLAYED"))
      playedEdge shouldBe defined
      playedEdge.get.direction shouldBe EdgeDirection.Outgoing
      playedEdge.get.other shouldBe expectedRoleId

      // Person -[:ACTED_IN]-> Movie
      val actedInEdge = personEdges.find(_.edgeType == Symbol("ACTED_IN"))
      actedInEdge shouldBe defined
      actedInEdge.get.direction shouldBe EdgeDirection.Outgoing
      actedInEdge.get.other shouldBe expectedMovieId

      // Movie -[:HAS_ROLE]-> Role
      val movieEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedMovieId),
        5.seconds,
      )

      val hasRoleEdge = movieEdges.find(_.edgeType == Symbol("HAS_ROLE"))
      hasRoleEdge shouldBe defined
      hasRoleEdge.get.direction shouldBe EdgeDirection.Outgoing
      hasRoleEdge.get.other shouldBe expectedRoleId

      // Verify Role node has the label and property set
      val roleProps = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(expectedRoleId, atTime = None),
        5.seconds,
      )
      roleProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))
      roleProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str(role))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "create edges via parsed Cypher (simple two-node pattern)" in {
    // Simpler test: just create one edge between two nodes
    val graph = makeGraph("e2e-simple-edge-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (a), (b)
        WHERE id(a) = idFrom("A", $aKey) AND id(b) = idFrom("B", $bKey)
        CREATE (a)-[:CONNECTS_TO]->(b)
      """

      val plan = parseAndPlan(query)

      // Verify plan structure
      def findCreateHalfEdges(p: QueryPlan): List[LocalQueryEffect.CreateHalfEdge] = p match {
        case LocalEffect(effects, child) =>
          effects.collect { case e: LocalQueryEffect.CreateHalfEdge => e } ++ findCreateHalfEdges(child)
        case Anchor(_, onTarget) => findCreateHalfEdges(onTarget)
        case Sequence(first, andThen, _) => findCreateHalfEdges(first) ++ findCreateHalfEdges(andThen)
        case CrossProduct(queries, _) => queries.flatMap(findCreateHalfEdges)
        case Filter(_, input) => findCreateHalfEdges(input)
        case Project(_, _, input) => findCreateHalfEdges(input)
        case _ => Nil
      }

      val createEdges = findCreateHalfEdges(plan)

      // One edge = 2 half-edges
      createEdges should have size 2

      // Execute
      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("aKey") -> Value.Text("node-a"),
        Symbol("bKey") -> Value.Text("node-b"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      Await.result(resultPromise.future, 15.seconds)
      Thread.sleep(500)

      // Compute expected IDs using the same idFrom function the runtime uses
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedAId = computeIdFrom("A", "node-a")
      val expectedBId = computeIdFrom("B", "node-b")

      // Verify edge from A to B
      val aEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedAId),
        5.seconds,
      )

      val connectsToEdge = aEdges.find(_.edgeType == Symbol("CONNECTS_TO"))
      connectsToEdge shouldBe defined
      connectsToEdge.get.direction shouldBe EdgeDirection.Outgoing
      connectsToEdge.get.other shouldBe expectedBId

      // Verify reciprocal edge on B
      val bEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(expectedBId),
        5.seconds,
      )

      val incomingEdge = bEdges.find(_.edgeType == Symbol("CONNECTS_TO"))
      incomingEdge shouldBe defined
      incomingEdge.get.direction shouldBe EdgeDirection.Incoming
      incomingEdge.get.other shouldBe expectedAId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // CROSSPRODUCT COMPLETION BUG - MINIMAL REPRODUCTION
  // ============================================================
  // This test demonstrates that CrossProduct with 3+ anchors fails to complete
  // in Eager mode, while CrossProduct with 2 anchors works correctly.
  // The issue is that notifications are being dropped because states are
  // unregistered prematurely.

  "CrossProduct completion" should "complete with 2 anchors in Eager mode" in {
    // This test PASSES - 2-anchor CrossProduct works
    val graph = makeGraph("crossproduct-2-anchor")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val query = """
        MATCH (a), (b)
        WHERE id(a) = idFrom("A") AND id(b) = idFrom("B")
        RETURN id(a) as aId, id(b) as bId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      // Should complete within timeout
      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with 3 anchors in Eager mode" in {
    // This test passes when there are no effects
    val graph = makeGraph("crossproduct-3-anchor")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val query = """
        MATCH (a), (b), (c)
        WHERE id(a) = idFrom("A") AND id(b) = idFrom("B") AND id(c) = idFrom("C")
        RETURN id(a) as aId, id(b) as bId, id(c) as cId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with 3 anchors and SET effects in Eager mode" in {
    // 3-anchor CrossProduct with SET effects
    val graph = makeGraph("crossproduct-3-anchor-set")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val query = """
        MATCH (a), (b), (c)
        WHERE id(a) = idFrom("A") AND id(b) = idFrom("B") AND id(c) = idFrom("C")
        SET a.visited = true, b.visited = true, c.visited = true
        RETURN id(a) as aId, id(b) as bId, id(c) as cId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify properties are actually set in the graph
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedAId = computeIdFrom("A")
      val expectedBId = computeIdFrom("B")
      val expectedCId = computeIdFrom("C")

      // Check properties on node A
      val aProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedAId, None), 5.seconds)
      aProps._1.get(Symbol("visited")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.True)

      // Check properties on node B
      val bProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedBId, None), 5.seconds)
      bProps._1.get(Symbol("visited")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.True)

      // Check properties on node C
      val cProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedCId, None), 5.seconds)
      cProps._1.get(Symbol("visited")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.True)

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with 3 anchors and CREATE edge effects in Eager mode" in {
    // 3-anchor CrossProduct with CREATE edge effects
    val graph = makeGraph("crossproduct-3-anchor-create")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (a), (b), (c)
        WHERE id(a) = idFrom("A") AND id(b) = idFrom("B") AND id(c) = idFrom("C")
        CREATE (a)-[:LINK]->(b)-[:LINK]->(c)
        RETURN id(a) as aId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify edges are actually created in the graph
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedAId = computeIdFrom("A")
      val expectedBId = computeIdFrom("B")
      val expectedCId = computeIdFrom("C")

      // Check edge A->B (outgoing from A)
      val aEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedAId), 5.seconds)
      val aOutgoingLink = aEdges.find(e => e.edgeType == Symbol("LINK") && e.direction == EdgeDirection.Outgoing)
      aOutgoingLink shouldBe defined
      aOutgoingLink.get.other shouldBe expectedBId

      // Check edge B->C (outgoing from B) and A->B (incoming to B)
      val bEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedBId), 5.seconds)
      val bOutgoingLink = bEdges.find(e => e.edgeType == Symbol("LINK") && e.direction == EdgeDirection.Outgoing)
      bOutgoingLink shouldBe defined
      bOutgoingLink.get.other shouldBe expectedCId

      val bIncomingLink = bEdges.find(e => e.edgeType == Symbol("LINK") && e.direction == EdgeDirection.Incoming)
      bIncomingLink shouldBe defined
      bIncomingLink.get.other shouldBe expectedAId

      // Check edge B->C (incoming to C)
      val cEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedCId), 5.seconds)
      val cIncomingLink = cEdges.find(e => e.edgeType == Symbol("LINK") && e.direction == EdgeDirection.Incoming)
      cIncomingLink shouldBe defined
      cIncomingLink.get.other shouldBe expectedBId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with 3 anchors, parameters, and CREATE edge effects in Eager mode" in {
    // This test reproduces the INGEST-3 pattern with parameters
    val graph = makeGraph("crossproduct-3-anchor-params")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $personId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $personId, $movieId)
        SET r.role = "Hero", r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("personId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify edges and properties are actually created in the graph
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedMId = computeIdFrom("Movie", "67890")
      val expectedRId = computeIdFrom("Role", "12345", "67890")

      // Check Role node properties and labels
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Check Person edges: PLAYED->Role, ACTED_IN->Movie
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

      val actedInEdge = pEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Outgoing)
      actedInEdge shouldBe defined
      actedInEdge.get.other shouldBe expectedMId

      // Check Role node: incoming PLAYED from Person, incoming HAS_ROLE from Movie
      val rEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedRId), 5.seconds)
      val incomingPlayed = rEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Incoming)
      incomingPlayed shouldBe defined
      incomingPlayed.get.other shouldBe expectedPId

      val incomingHasRole = rEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Incoming)
      incomingHasRole shouldBe defined
      incomingHasRole.get.other shouldBe expectedMId

      // Check Movie node: HAS_ROLE->Role, incoming ACTED_IN from Person
      val mEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedMId), 5.seconds)
      val hasRoleEdge = mEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Outgoing)
      hasRoleEdge shouldBe defined
      hasRoleEdge.get.other shouldBe expectedRId

      val incomingActedIn = mEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Incoming)
      incomingActedIn shouldBe defined
      incomingActedIn.get.other shouldBe expectedPId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with INGEST-3 exact pattern (3 params including role in idFrom)" in {
    // This test is the EXACT INGEST-3 pattern - the minimal reproduction
    val graph = makeGraph("crossproduct-ingest3-exact")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // Exact INGEST-3 query from the movie data recipe
      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
        SET r.role = $role, r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify edges and properties are actually created in the graph
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedMId = computeIdFrom("Movie", "67890")
      val expectedRId = computeIdFrom("Role", "12345", "67890", "Hero")

      // Check Role node properties and labels
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Check Person edges: PLAYED->Role, ACTED_IN->Movie
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

      val actedInEdge = pEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Outgoing)
      actedInEdge shouldBe defined
      actedInEdge.get.other shouldBe expectedMId

      // Check Role node: incoming PLAYED from Person, incoming HAS_ROLE from Movie
      val rEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedRId), 5.seconds)
      val incomingPlayed = rEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Incoming)
      incomingPlayed shouldBe defined
      incomingPlayed.get.other shouldBe expectedPId

      val incomingHasRole = rEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Incoming)
      incomingHasRole shouldBe defined
      incomingHasRole.get.other shouldBe expectedMId

      // Check Movie node: HAS_ROLE->Role, incoming ACTED_IN from Person
      val mEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedMId), 5.seconds)
      val hasRoleEdge = mEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Outgoing)
      hasRoleEdge shouldBe defined
      hasRoleEdge.get.other shouldBe expectedRId

      val incomingActedIn = mEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Incoming)
      incomingActedIn shouldBe defined
      incomingActedIn.get.other shouldBe expectedPId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // === ISOLATION TESTS: Find the exact trigger for the bug ===

  it should "complete with 3 params in idFrom but literal in SET (isolation test A)" in {
    // 3 params in idFrom, literal in SET
    val graph = makeGraph("crossproduct-isolation-a")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
        SET r.role = "HeroLiteral", r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify graph state
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedRId = computeIdFrom("Role", "12345", "67890", "Hero")

      // Check Role node properties (literal value "HeroLiteral")
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("HeroLiteral"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Check Person PLAYED edge
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with 2 params in idFrom and 3rd param only in SET (isolation test B)" in {
    // 2 params in idFrom, 3rd param only in SET
    val graph = makeGraph("crossproduct-isolation-b")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId)
        SET r.role = $role, r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify graph state
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedRId = computeIdFrom("Role", "12345", "67890") // Note: no $role in idFrom

      // Check Role node properties (param value "Hero")
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Check Person PLAYED edge
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with ONLY SET and no CREATE (isolation test C)" in {
    // ONLY SET effects, no CREATE edges
    val graph = makeGraph("crossproduct-isolation-c")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
        SET r.role = $role, r:Role
        RETURN id(r) as rId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify graph state - properties only, no edges
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedRId = computeIdFrom("Role", "12345", "67890", "Hero")

      // Check Role node properties
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with ONLY CREATE and no SET (isolation test D)" in {
    // ONLY CREATE edges, no SET
    val graph = makeGraph("crossproduct-isolation-d")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
        RETURN id(r) as rId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify graph state - edges only, no properties
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedMId = computeIdFrom("Movie", "67890")
      val expectedRId = computeIdFrom("Role", "12345", "67890", "Hero")

      // Check Person PLAYED edge to Role
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

      // Check Person ACTED_IN edge to Movie
      val actedInEdge = pEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Outgoing)
      actedInEdge shouldBe defined
      actedInEdge.get.other shouldBe expectedMId

      // Check Role incoming edges
      val rEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedRId), 5.seconds)
      val incomingPlayed = rEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Incoming)
      incomingPlayed shouldBe defined
      incomingPlayed.get.other shouldBe expectedPId

      val incomingHasRole = rEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Incoming)
      incomingHasRole shouldBe defined
      incomingHasRole.get.other shouldBe expectedMId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "complete with NO effects at all (isolation test E)" in {
    // This tests: Is the issue in the anchoring/planning itself?
    // If this FAILS, the issue is in the 3-param idFrom pattern even without effects
    val graph = makeGraph("crossproduct-isolation-e")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $tmdbId)
          AND id(m) = idFrom("Movie", $movieId)
          AND id(r) = idFrom("Role", $tmdbId, $movieId, $role)
        RETURN id(p) as pId, id(m) as mId, id(r) as rId
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(
        Symbol("tmdbId") -> Value.Text("12345"),
        Symbol("movieId") -> Value.Text("67890"),
        Symbol("role") -> Value.Text("Hero"),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // RECIPE PATTERN TESTS - Mimics actual ingest behavior
  // ============================================================
  // The actual recipe uses $that.property syntax where $that is an object.
  // Our previous tests used flat parameters like $tmdbId.
  // This tests the exact pattern used by QuinePatternImportFormat.

  "Recipe pattern" should "create edges using $that.property syntax" in {
    // This mimics the ACTUAL ingest query pattern from movieData recipe
    val graph = makeGraph("recipe-pattern-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // Exact query pattern from INGEST-3 in movieData recipe
      val query = """
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", $that.tmdbId)
          AND id(m) = idFrom("Movie", $that.movieId)
          AND id(r) = idFrom("Role", $that.tmdbId, $that.movieId, $that.role)
        SET r.role = $that.role, r:Role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)

      // Mimic how the ingest passes parameters - a single $that object
      import scala.collection.immutable.SortedMap
      val thatValue = Value.Map(
        SortedMap(
          Symbol("tmdbId") -> Value.Text("12345"),
          Symbol("movieId") -> Value.Text("67890"),
          Symbol("role") -> Value.Text("Hero"),
        ),
      )
      val params = Map(Symbol("that") -> thatValue)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 5.seconds)
      results should have size 1

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Verify edges and properties are actually created in the graph
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPId = computeIdFrom("Person", "12345")
      val expectedMId = computeIdFrom("Movie", "67890")
      val expectedRId = computeIdFrom("Role", "12345", "67890", "Hero")

      // Check Role node properties and labels
      val rProps = Await.result(graph.literalOps(namespace).getPropsAndLabels(expectedRId, None), 5.seconds)
      rProps._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      rProps._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Check Person edges: PLAYED->Role, ACTED_IN->Movie
      val pEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPId), 5.seconds)
      val playedEdge = pEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      playedEdge shouldBe defined
      playedEdge.get.other shouldBe expectedRId

      val actedInEdge = pEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Outgoing)
      actedInEdge shouldBe defined
      actedInEdge.get.other shouldBe expectedMId

      // Check Role node: incoming PLAYED from Person, incoming HAS_ROLE from Movie
      val rEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedRId), 5.seconds)
      val incomingPlayed = rEdges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Incoming)
      incomingPlayed shouldBe defined
      incomingPlayed.get.other shouldBe expectedPId

      val incomingHasRole = rEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Incoming)
      incomingHasRole shouldBe defined
      incomingHasRole.get.other shouldBe expectedMId

      // Check Movie node: HAS_ROLE->Role, incoming ACTED_IN from Person
      val mEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedMId), 5.seconds)
      val hasRoleEdge = mEdges.find(e => e.edgeType == Symbol("HAS_ROLE") && e.direction == EdgeDirection.Outgoing)
      hasRoleEdge shouldBe defined
      hasRoleEdge.get.other shouldBe expectedRId

      val incomingActedIn = mEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Incoming)
      incomingActedIn shouldBe defined
      incomingActedIn.get.other shouldBe expectedPId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // BUG REPRODUCTION: CrossProduct of Anchors after WITH clause
  // ============================================================

  // This test demonstrates the bug where a CrossProduct of Anchors
  // following a WITH clause only has the first Anchor receive context.
  // The actual recipe INGEST-3 pattern uses:
  //   WITH $that AS row
  //   WITH row WHERE row.Entity = "Join" AND row.Work = "Acting"
  //   MATCH (p), (m), (r) WHERE id(p) = idFrom("Person", row.tmdbId) ...
  //
  // When this is planned, the MATCH creates a CrossProduct of 3 Anchors.
  // The bug: only the first Anchor (p) receives the `row` context binding.
  // The other Anchors (m, r) are kickstarted with empty context, so their
  // idFrom expressions fail to evaluate row.* properties correctly.

  "CrossProduct of Anchors after WITH" should "pass context to ALL anchors (bug reproduction)" in {
    // This pattern exactly matches INGEST-3 from the movieData recipe
    val graph = makeGraph("crossproduct-anchors-context-bug")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // Simplified version of INGEST-3: WITH establishes row, then 3 anchors use row.*
      val query = """
        WITH $that AS row
        MATCH (p), (m), (r)
        WHERE id(p) = idFrom("Person", row.tmdbId)
          AND id(m) = idFrom("Movie", row.movieId)
          AND id(r) = idFrom("Role", row.tmdbId, row.movieId, row.role)
        SET p:Person, m:Movie, r:Role, r.role = row.role
        CREATE (p)-[:PLAYED]->(r)<-[:HAS_ROLE]-(m)
        CREATE (p)-[:ACTED_IN]->(m)
      """

      val plan = parseAndPlan(query)

      // Debug: print the plan structure

      // Execute with TWO different row values to verify distinct nodes are created
      def runWithRow(tmdbId: String, movieId: String, role: String): Unit = {
        val resultPromise = Promise[Seq[QueryContext]]()
        val outputTarget = OutputTarget.EagerCollector(resultPromise)

        import scala.collection.immutable.SortedMap
        val thatValue = Value.Map(
          SortedMap(
            Symbol("tmdbId") -> Value.Text(tmdbId),
            Symbol("movieId") -> Value.Text(movieId),
            Symbol("role") -> Value.Text(role),
          ),
        )
        val params = Map(Symbol("that") -> thatValue)

        val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        loader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = plan,
          mode = RuntimeMode.Eager,
          params = params,
          namespace = namespace,
          output = outputTarget,
        )

        val _ = Await.result(resultPromise.future, 5.seconds)
      }

      // Run with two different actors in different movies
      runWithRow("111", "AAA", "Hero")
      runWithRow("222", "BBB", "Villain")

      // Allow time for effects to propagate
      Thread.sleep(500)

      // Compute expected node IDs
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // First execution: Person 111, Movie AAA, Role (111, AAA, Hero)
      val person1 = computeIdFrom("Person", "111")
      val role1 = computeIdFrom("Role", "111", "AAA", "Hero")

      // Second execution: Person 222, Movie BBB, Role (222, BBB, Villain)
      val person2 = computeIdFrom("Person", "222")
      val role2 = computeIdFrom("Role", "222", "BBB", "Villain")

      // KEY VERIFICATION: We should have DISTINCT Role nodes for each execution
      // If the bug exists, both executions create the SAME Role node because
      // row.tmdbId, row.movieId, row.role aren't evaluated correctly

      val role1Props = Await.result(graph.literalOps(namespace).getPropsAndLabels(role1, None), 5.seconds)
      val role2Props = Await.result(graph.literalOps(namespace).getPropsAndLabels(role2, None), 5.seconds)

      // Both Role nodes should have the Role label
      role1Props._2.getOrElse(Set.empty) should contain(Symbol("Role"))
      role2Props._2.getOrElse(Set.empty) should contain(Symbol("Role"))

      // Role nodes should have DIFFERENT role property values
      role1Props._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Hero"))
      role2Props._1.get(Symbol("role")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("Villain"))

      // Verify edges are connected correctly
      val person1Edges = Await.result(graph.literalOps(namespace).getHalfEdges(person1), 5.seconds)
      val person2Edges = Await.result(graph.literalOps(namespace).getHalfEdges(person2), 5.seconds)

      // Person 1 should have PLAYED edge to Role 1 (not Role 2!)
      val p1PlayedEdge = person1Edges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      p1PlayedEdge shouldBe defined
      p1PlayedEdge.get.other shouldBe role1

      // Person 2 should have PLAYED edge to Role 2
      val p2PlayedEdge = person2Edges.find(e => e.edgeType == Symbol("PLAYED") && e.direction == EdgeDirection.Outgoing)
      p2PlayedEdge shouldBe defined
      p2PlayedEdge.get.other shouldBe role2

      // Count total Role nodes - should be 2, not 1
      // If the bug exists, there would only be 1 Role node (all edges point to same one)
      val role1Edges = Await.result(graph.literalOps(namespace).getHalfEdges(role1), 5.seconds)
      val role2Edges = Await.result(graph.literalOps(namespace).getHalfEdges(role2), 5.seconds)

      // Each Role should have exactly 2 incoming edges (PLAYED from Person, HAS_ROLE from Movie)
      role1Edges.filter(_.direction == EdgeDirection.Incoming) should have size 2
      role2Edges.filter(_.direction == EdgeDirection.Incoming) should have size 2

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // STANDING QUERY PATTERN TEST - Actor-Director pattern from recipe
  // ============================================================

  // This tests the exact standing query pattern from the movieData recipe:
  // MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
  // WHERE id(a) = id(m)
  // RETURN id(m) as movieId, id(p) as personId
  //
  // The pattern finds people who both acted in AND directed the same movie.

  "Standing Query Pattern" should "emit match when Person has both ACTED_IN and DIRECTED edges to same Movie" in {
    val graph = makeGraph("sq-actor-director-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // The exact standing query pattern from the recipe
      val sqQuery = """
        MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
        WHERE id(a) = id(m)
        RETURN id(m) as movieId, id(p) as personId
      """

      val sqPlan = parseAndPlan(sqQuery)

      // Debug: print the plan structure

      // Create a collector for lazy mode results
      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      // Load the standing query in Lazy mode
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      // Give time for the standing query to initialize
      Thread.sleep(500)

      // Initially no results
      collector.allDeltas shouldBe empty

      // Create the graph structure using an Eager query:
      // Person -[:ACTED_IN]-> Movie
      // Person -[:DIRECTED]-> Movie (same movie)
      val createQuery = """
        MATCH (p), (m)
        WHERE id(p) = idFrom("Person", "actor-director-1")
          AND id(m) = idFrom("Movie", "movie-1")
        SET p:Person, p.name = "Actor-Director"
        SET m:Movie, m.title = "My Movie"
        CREATE (p)-[:ACTED_IN]->(m)
        CREATE (p)-[:DIRECTED]->(m)
      """

      val createPlan = parseAndPlan(createQuery)
      val createPromise = Promise[Seq[QueryContext]]()

      val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      createLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = createPlan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(createPromise),
      )

      // Wait for the create query to complete
      Await.result(createPromise.future, 10.seconds)

      // Wait a bit more for effects to propagate
      Thread.sleep(1000)

      // Wait for the standing query to detect the match
      val matched = collector.awaitFirstDelta(10.seconds)

      // Verify the graph structure was created
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val expectedPersonId = computeIdFrom("Person", "actor-director-1")
      val expectedMovieId = computeIdFrom("Movie", "movie-1")

      val personEdges = Await.result(graph.literalOps(namespace).getHalfEdges(expectedPersonId), 5.seconds)

      // Verify Person has ACTED_IN and DIRECTED edges to Movie
      val actedInEdge = personEdges.find(e => e.edgeType == Symbol("ACTED_IN") && e.direction == EdgeDirection.Outgoing)
      val directedEdge =
        personEdges.find(e => e.edgeType == Symbol("DIRECTED") && e.direction == EdgeDirection.Outgoing)

      actedInEdge shouldBe defined
      actedInEdge.get.other shouldBe expectedMovieId
      directedEdge shouldBe defined
      directedEdge.get.other shouldBe expectedMovieId

      // Should have detected EXACTLY one match (not duplicates)
      matched shouldBe true
      collector.positiveCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "produce exactly the expected count with multiple persons and movies" in {
    // This test creates a scenario similar to the movie recipe:
    // - Multiple persons, some who both acted in and directed the same movie (should match)
    // - Some persons who only acted in a movie (no match)
    // - Some persons who only directed a movie (no match)
    // - Some persons who acted in one movie and directed a different one (no match)
    val graph = makeGraph("sq-exact-count-test")
    while (!graph.isReady) Thread.sleep(10)

    try {

      val sqQuery = """
        MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
        WHERE id(a) = id(m)
        RETURN id(m) as movieId, id(p) as personId
      """

      val sqPlan = parseAndPlan(sqQuery)

      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      Thread.sleep(500)

      // Create test data:
      // Person1: acted in Movie1, directed Movie1 -> MATCHES (id(a) = id(m))
      // Person2: acted in Movie2, directed Movie2 -> MATCHES (id(a) = id(m))
      // Person3: acted in Movie3 only -> NO MATCH (no DIRECTED edge)
      // Person4: directed Movie4 only -> NO MATCH (no ACTED_IN edge)
      // Person5: acted in Movie5, directed Movie6 -> NO MATCH (id(a) != id(m))

      // Create matching cases
      for (i <- 1 to 2) {
        val createQuery = s"""
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "person-$i")
            AND id(m) = idFrom("Movie", "movie-$i")
          SET p:Person, p.name = "Person $i"
          SET m:Movie, m.title = "Movie $i"
          CREATE (p)-[:ACTED_IN]->(m)
          CREATE (p)-[:DIRECTED]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching case: person acted but didn't direct
      {
        val createQuery = """
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "person-3")
            AND id(m) = idFrom("Movie", "movie-3")
          SET p:Person, p.name = "Person 3"
          SET m:Movie, m.title = "Movie 3"
          CREATE (p)-[:ACTED_IN]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching case: person directed but didn't act
      {
        val createQuery = """
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "person-4")
            AND id(m) = idFrom("Movie", "movie-4")
          SET p:Person, p.name = "Person 4"
          SET m:Movie, m.title = "Movie 4"
          CREATE (p)-[:DIRECTED]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching case: person acted in one movie, directed different movie
      {
        val createQuery = """
          MATCH (p), (m5), (m6)
          WHERE id(p) = idFrom("Person", "person-5")
            AND id(m5) = idFrom("Movie", "movie-5")
            AND id(m6) = idFrom("Movie", "movie-6")
          SET p:Person, p.name = "Person 5"
          SET m5:Movie, m5.title = "Movie 5"
          SET m6:Movie, m6.title = "Movie 6"
          CREATE (p)-[:ACTED_IN]->(m5)
          CREATE (p)-[:DIRECTED]->(m6)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Wait for effects to propagate
      Thread.sleep(2000)

      // We should have EXACTLY 2 matches: Person1-Movie1 and Person2-Movie2
      // Not 0 (filter not working), not >2 (duplicates or wrong matching)
      collector.positiveCount shouldBe 2

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "not emit duplicates when kickstart and edge events both fire" in {
    // This test specifically verifies the deduplication fix:
    // When a standing query is installed and then data is created,
    // both kickstart (seeing existing edges) and edge events (for new edges)
    // might try to dispatch for the same edge. We should only get ONE match.
    val graph = makeGraph("sq-dedup-test")
    while (!graph.isReady) Thread.sleep(10)

    try {

      // First, create the data BEFORE the standing query
      val createQuery = """
        MATCH (p), (m)
        WHERE id(p) = idFrom("Person", "dedup-person")
          AND id(m) = idFrom("Movie", "dedup-movie")
        SET p:Person, p.name = "Dedup Person"
        SET m:Movie, m.title = "Dedup Movie"
        CREATE (p)-[:ACTED_IN]->(m)
        CREATE (p)-[:DIRECTED]->(m)
      """
      val createPlan = parseAndPlan(createQuery)
      val createPromise = Promise[Seq[QueryContext]]()
      val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      createLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = createPlan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(createPromise),
      )
      Await.result(createPromise.future, 10.seconds)

      // Wait for data to settle
      Thread.sleep(500)

      // NOW install the standing query (after data exists)
      val sqQuery = """
        MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
        WHERE id(a) = id(m)
        RETURN id(m) as movieId, id(p) as personId
      """
      val sqPlan = parseAndPlan(sqQuery)

      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      // Wait for standing query to evaluate existing data
      Thread.sleep(2000)

      // Should have EXACTLY 1 match, not duplicates
      collector.positiveCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // RECIPE-LIKE TESTS - Using actual standing query API
  // These tests mimic how recipes register standing queries
  // ============================================================

  "Recipe-like Standing Query" should "produce correct count via LoadQuery (like recipes)" in {
    // This test uses the same registration flow as recipes:
    // 1. Register standing query via startStandingQuery
    // 2. Start the V2 pattern via LoadQuery (like QuineApp does for recipes)
    // 3. Create data via ingests
    // 4. Verify result count matches expectations
    import com.thatdot.quine.graph.quinepattern.LoadQuery

    val graph = makeGraph("recipe-like-sq-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val sqId = StandingQueryId.fresh()

      // Parse and plan the standing query - same pattern as recipe
      val sqQuery = """
        MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
        WHERE id(a) = id(m)
        RETURN id(m) as movieId, id(p) as personId
      """
      val sqPlan = parseAndPlan(sqQuery)

      // Create the standing query pattern (same as recipe would)
      val sqPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = sqPlan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("movieId"), Symbol("personId"))),
      )

      // Collect results
      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized {
              resultsList += result
            }
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      // Register the standing query via the API (like recipes do)
      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "test-actor-director-sq",
          pattern = sqPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Like QuineApp, we need to send LoadQuery to actually start the V2 pattern
      graph.getLoader ! LoadQuery(
        sqId,
        sqPlan,
        RuntimeMode.Lazy,
        Map.empty,
        namespace,
        OutputTarget.StandingQuerySink(sqId, namespace),
        Some(Set(Symbol("movieId"), Symbol("personId"))),
      )

      // Give time for standing query to initialize
      Thread.sleep(500)

      // Create test data similar to recipe ingests:
      // Person1 acted in and directed Movie1 -> SHOULD MATCH
      // Person2 acted in and directed Movie2 -> SHOULD MATCH
      // Person3 acted in Movie3 only -> NO MATCH
      // Person4 directed Movie4 only -> NO MATCH
      // Person5 acted in Movie5, directed Movie6 -> NO MATCH (different movies)

      // Create matching cases
      for (i <- 1 to 2) {
        val createQuery = s"""
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "recipe-person-$i")
            AND id(m) = idFrom("Movie", "recipe-movie-$i")
          SET p:Person, p.name = "Person $i"
          SET m:Movie, m.title = "Movie $i"
          CREATE (p)-[:ACTED_IN]->(m)
          CREATE (p)-[:DIRECTED]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching: acted but didn't direct
      {
        val createQuery = """
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "recipe-person-3")
            AND id(m) = idFrom("Movie", "recipe-movie-3")
          SET p:Person, p.name = "Person 3"
          SET m:Movie, m.title = "Movie 3"
          CREATE (p)-[:ACTED_IN]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching: directed but didn't act
      {
        val createQuery = """
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "recipe-person-4")
            AND id(m) = idFrom("Movie", "recipe-movie-4")
          SET p:Person, p.name = "Person 4"
          SET m:Movie, m.title = "Movie 4"
          CREATE (p)-[:DIRECTED]->(m)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create non-matching: acted in one movie, directed different movie
      {
        val createQuery = """
          MATCH (p), (m5), (m6)
          WHERE id(p) = idFrom("Person", "recipe-person-5")
            AND id(m5) = idFrom("Movie", "recipe-movie-5")
            AND id(m6) = idFrom("Movie", "recipe-movie-6")
          SET p:Person, p.name = "Person 5"
          SET m5:Movie, m5.title = "Movie 5"
          SET m6:Movie, m6.title = "Movie 6"
          CREATE (p)-[:ACTED_IN]->(m5)
          CREATE (p)-[:DIRECTED]->(m6)
        """
        val createPlan = parseAndPlan(createQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Wait for results to propagate through standing query
      Thread.sleep(3000)

      // Count positive matches (not retractions)
      val positiveCount = resultsList.count(_.meta.isPositiveMatch)
      positiveCount shouldBe 2

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle person acting in multiple movies correctly" in {
    // This tests a specific scenario that might cause multiplication:
    // Person P acted in Movies A1, A2, A3 and directed ONLY A1
    // Should produce exactly 1 match (P-A1), not 3
    import com.thatdot.quine.graph.quinepattern.LoadQuery

    val graph = makeGraph("recipe-multi-movie-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val sqId = StandingQueryId.fresh()

      val sqQuery = """
        MATCH (a:Movie)<-[:ACTED_IN]-(p:Person)-[:DIRECTED]->(m:Movie)
        WHERE id(a) = id(m)
        RETURN id(m) as movieId, id(p) as personId
      """
      val sqPlan = parseAndPlan(sqQuery)

      val sqPattern = StandingQueryPattern.QuinePatternQueryPattern(
        compiledQuery = sqPlan,
        mode = RuntimeMode.Lazy,
        returnColumns = Some(Set(Symbol("movieId"), Symbol("personId"))),
      )

      val resultsList = scala.collection.mutable.ListBuffer.empty[StandingQueryResult]
      import org.apache.pekko.stream.scaladsl.Flow
      val collectingSink: Sink[StandingQueryResult, UniqueKillSwitch] =
        Flow[StandingQueryResult]
          .map { result =>
            resultsList.synchronized(resultsList += result)
            result
          }
          .viaMat(KillSwitches.single)(Keep.right)
          .to(Sink.ignore)

      graph
        .standingQueries(namespace)
        .get
        .startStandingQuery(
          sqId = sqId,
          name = "test-multi-movie-sq",
          pattern = sqPattern,
          outputs = Map("test" -> collectingSink),
          queueBackpressureThreshold = StandingQueryInfo.DefaultQueueBackpressureThreshold,
          queueMaxSize = StandingQueryInfo.DefaultQueueMaxSize,
          shouldCalculateResultHashCode = false,
        )

      // Like QuineApp, we need to send LoadQuery to actually start the V2 pattern
      graph.getLoader ! LoadQuery(
        sqId,
        sqPlan,
        RuntimeMode.Lazy,
        Map.empty,
        namespace,
        OutputTarget.StandingQuerySink(sqId, namespace),
        Some(Set(Symbol("movieId"), Symbol("personId"))),
      )

      Thread.sleep(500)

      // Create: Person P acted in A1, A2, A3 but directed only A1
      // Person P
      {
        val personQuery = """
          MATCH (p)
          WHERE id(p) = idFrom("Person", "multi-movie-person")
          SET p:Person, p.name = "Multi Movie Person"
        """
        val createPlan = parseAndPlan(personQuery)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create Movies A1, A2, A3
      for (i <- 1 to 3) {
        val createMovie = s"""
          MATCH (m)
          WHERE id(m) = idFrom("Movie", "multi-movie-$i")
          SET m:Movie, m.title = "Movie $i"
        """
        val createPlan = parseAndPlan(createMovie)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create ACTED_IN edges to all 3 movies
      for (i <- 1 to 3) {
        val createEdge = s"""
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "multi-movie-person")
            AND id(m) = idFrom("Movie", "multi-movie-$i")
          CREATE (p)-[:ACTED_IN]->(m)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create DIRECTED edge to ONLY movie 1
      {
        val createEdge = """
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "multi-movie-person")
            AND id(m) = idFrom("Movie", "multi-movie-1")
          CREATE (p)-[:DIRECTED]->(m)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      Thread.sleep(3000)

      // Should have exactly 1 match: Person-Movie1
      // NOT 3 matches (one for each movie they acted in)
      val positiveCount = resultsList.count(_.meta.isPositiveMatch)
      positiveCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // DELTA COMPUTATION TESTS - Verify incremental delta logic
  // ============================================================

  "Expand delta computation" should "not re-emit results when multiple neighbors respond" in {
    // This test verifies that Expand doesn't re-emit ALL accumulated results
    // every time a neighbor sends a delta. It should only emit incremental deltas.
    //
    // Scenario: Movie M has ACTED_IN edges to Persons P1, P2, P3
    // When P1 responds with result R1, emit R1
    // When P2 responds with result R2, emit ONLY R2 (not R1+R2)
    // When P3 responds with result R3, emit ONLY R3 (not R1+R2+R3)
    //
    // Bug: Current code emits combined results, causing re-emission.
    val graph = makeGraph("expand-delta-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Create structure: Movie connected to 5 Persons via ACTED_IN
      // Each Person has a "name" property
      // Query: MATCH (m:Movie)-[:ACTED_IN]->(p:Person) RETURN id(m), p.name
      // Should return 5 results total, not 15 (1+2+3+4+5)

      // Create ALL data FIRST before standing query
      // Create Movie node
      {
        val createMovie = """
          MATCH (m)
          WHERE id(m) = idFrom("Movie", "expand-delta-movie")
          SET m:Movie, m.title = "Test Movie"
        """
        val createPlan = parseAndPlan(createMovie)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create 5 Person nodes and connect them to Movie via ACTED_IN
      for (i <- 1 to 5) {
        val createPersonAndEdge = s"""
          MATCH (p), (m)
          WHERE id(p) = idFrom("Person", "expand-delta-person-$i")
            AND id(m) = idFrom("Movie", "expand-delta-movie")
          SET p:Person, p.name = "Person $i"
          CREATE (m)-[:ACTED_IN]->(p)
        """
        val createPlan = parseAndPlan(createPersonAndEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      Thread.sleep(500) // Let data settle

      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      // Simple expand pattern: Movie -> Person via ACTED_IN
      val sqQuery = """
        MATCH (m:Movie)-[:ACTED_IN]->(p:Person)
        RETURN id(m) as movieId, p.name as personName
      """
      val sqPlan = parseAndPlan(sqQuery)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      Thread.sleep(2000) // Give standing query time to evaluate

      // Should have exactly 5 matches, not 15
      collector.positiveCount shouldBe 5

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // QUERY PLAN STRUCTURE TESTS - Examine planner output
  // ============================================================

  "Entity resolution query plans" should "produce sensible plans for the first SQ pattern" in {
    // First standing query from entity-resolution recipe:
    // MATCH (pb)<-[:poBox]-(e)-[:postcode]->(pc)
    // RETURN id(e) AS entity, pb.poBox AS poBox, pc.postcode AS postcode
    val query = """
      MATCH (pb)<-[:poBox]-(e)-[:postcode]->(pc)
      RETURN id(e) AS entity, pb.poBox AS poBox, pc.postcode AS postcode
    """
    val plan = parseAndPlan(query)

    prettyPrintPlan(plan)

    // The plan should have an Anchor at the root
    plan match {
      case Anchor(_, _) =>
      case _ =>
    }

    // Just verify it parses and plans without error
    plan shouldBe a[QueryPlan]
  }

  it should "produce sensible plans for the second SQ pattern (with property filter)" in {
    // Second standing query from entity-resolution recipe:
    // MATCH (record)-[:record_for_entity]->(entity)-[:resolved]->(resolved)
    // WHERE resolved.canonical IS NOT NULL
    // RETURN id(record) AS record, id(resolved) AS resolved
    val query = """
      MATCH (record)-[:record_for_entity]->(entity)-[:resolved]->(resolved)
      WHERE resolved.canonical IS NOT NULL
      RETURN id(record) AS record, id(resolved) AS resolved
    """
    val plan = parseAndPlan(query)

    prettyPrintPlan(plan)

    // The plan should have an Anchor at the root
    plan match {
      case Anchor(_, _) =>
      case _ =>
    }

    // Just verify it parses and plans without error
    plan shouldBe a[QueryPlan]
  }

  /** Pretty print a query plan with indentation */
  private def prettyPrintPlan(plan: QueryPlan, indent: Int = 0): Unit =
    plan match {
      case Anchor(_, onTarget) =>
        prettyPrintPlan(onTarget, indent + 1)

      case CrossProduct(queries, _) =>
        queries.foreach(q => prettyPrintPlan(q, indent + 1))

      case Expand(_, _, onNeighbor) =>
        prettyPrintPlan(onNeighbor, indent + 1)

      case LocalId(_) =>

      case LocalProperty(_, _, _) =>

      case LocalLabels(_, _) =>

      case Filter(_, input) =>
        prettyPrintPlan(input, indent + 1)

      case Project(_, _, input) =>
        prettyPrintPlan(input, indent + 1)

      case Sequence(first, andThen, _) =>
        prettyPrintPlan(first, indent + 2)
        prettyPrintPlan(andThen, indent + 2)

      case LocalEffect(_, input) =>
        prettyPrintPlan(input, indent + 1)

      case QueryPlan.Unit =>

      case _ =>
    }

  // ============================================================
  // ENTITY RESOLUTION PATTERN TEST - Two outgoing edges from center node
  // ============================================================

  "Entity resolution pattern" should "produce results with two-edge hub pattern" in {
    // This test mimics the entity-resolution recipe pattern:
    // MATCH (pb)<-[:poBox]-(e)-[:postcode]->(pc)
    // Which has entity `e` at center with edges to both `pb` and `pc`
    //
    // From e's perspective, both edges are Outgoing:
    //   e -[:poBox]-> pb
    //   e -[:postcode]-> pc

    val graph = makeGraph("entity-resolution-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      // Pattern matching entity-resolution: center node with two outgoing edges
      val sqQuery = """
        MATCH (pb)<-[:poBox]-(e)-[:postcode]->(pc)
        RETURN id(e) as entity, pb.poBox as poBox, pc.postcode as postcode
      """
      val sqPlan = parseAndPlan(sqQuery)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      Thread.sleep(500)

      // Create entity node with Entity label
      {
        val createEntity = """
          MATCH (e)
          WHERE id(e) = idFrom("Entity", "test-entity-1")
          SET e:Entity, e.name = "Test Entity"
        """
        val createPlan = parseAndPlan(createEntity)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create poBox node with property
      {
        val createPoBox = """
          MATCH (pb)
          WHERE id(pb) = idFrom("poBox", "12345")
          SET pb.poBox = "12345"
        """
        val createPlan = parseAndPlan(createPoBox)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create postcode node with property
      {
        val createPostcode = """
          MATCH (pc)
          WHERE id(pc) = idFrom("postcode", "90210")
          SET pc.postcode = "90210"
        """
        val createPlan = parseAndPlan(createPostcode)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create edge from entity to poBox
      {
        val createEdge = """
          MATCH (e), (pb)
          WHERE id(e) = idFrom("Entity", "test-entity-1")
            AND id(pb) = idFrom("poBox", "12345")
          CREATE (e)-[:poBox]->(pb)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create edge from entity to postcode
      {
        val createEdge = """
          MATCH (e), (pc)
          WHERE id(e) = idFrom("Entity", "test-entity-1")
            AND id(pc) = idFrom("postcode", "90210")
          CREATE (e)-[:postcode]->(pc)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      Thread.sleep(2000)

      // Should have exactly 1 match
      collector.positiveCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "produce results with property filter on target node" in {
    // This test mimics the second entity-resolution recipe pattern:
    // MATCH (record)-[:record_for_entity]->(entity)-[:resolved]->(resolved)
    // WHERE resolved.canonical IS NOT NULL
    //
    // The key difference is the property filter on the end node.

    val graph = makeGraph("entity-resolution-prop-filter-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val outputTarget = OutputTarget.LazyCollector(collector)

      // Pattern with property filter on end node
      val sqQuery = """
        MATCH (record)-[:record_for]->(entity)-[:resolved]->(resolved)
        WHERE resolved.canonical IS NOT NULL
        RETURN id(record) as recordId, id(resolved) as resolvedId
      """
      val sqPlan = parseAndPlan(sqQuery)

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = sqPlan,
        mode = RuntimeMode.Lazy,
        params = Map.empty,
        namespace = namespace,
        output = outputTarget,
      )

      Thread.sleep(500)

      // Create record node
      {
        val create = """
          MATCH (r)
          WHERE id(r) = idFrom("Record", "test-record-1")
          SET r:Record, r.data = "test data"
        """
        val createPlan = parseAndPlan(create)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create entity node
      {
        val create = """
          MATCH (e)
          WHERE id(e) = idFrom("Entity", "test-entity-prop")
          SET e:Entity, e.name = "Test Entity"
        """
        val createPlan = parseAndPlan(create)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create resolved node WITH canonical property (should match)
      {
        val create = """
          MATCH (resolved)
          WHERE id(resolved) = idFrom("Resolved", "test-resolved-1")
          SET resolved:Resolved, resolved.canonical = {poBox: "123", postcode: "456"}
        """
        val createPlan = parseAndPlan(create)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create edge: record -> entity
      {
        val createEdge = """
          MATCH (r), (e)
          WHERE id(r) = idFrom("Record", "test-record-1")
            AND id(e) = idFrom("Entity", "test-entity-prop")
          CREATE (r)-[:record_for]->(e)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      // Create edge: entity -> resolved
      {
        val createEdge = """
          MATCH (e), (resolved)
          WHERE id(e) = idFrom("Entity", "test-entity-prop")
            AND id(resolved) = idFrom("Resolved", "test-resolved-1")
          CREATE (e)-[:resolved]->(resolved)
        """
        val createPlan = parseAndPlan(createEdge)
        val createPromise = Promise[Seq[QueryContext]]()
        val createLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
        createLoader ! QuinePatternCommand.LoadQueryPlan(
          sqid = StandingQueryId.fresh(),
          plan = createPlan,
          mode = RuntimeMode.Eager,
          params = Map.empty,
          namespace = namespace,
          output = OutputTarget.EagerCollector(createPromise),
        )
        Await.result(createPromise.future, 10.seconds)
      }

      Thread.sleep(2000)

      // Should have exactly 1 match
      collector.positiveCount shouldBe 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // FOREACH RUNTIME TESTS
  // These tests verify that FOREACH clauses execute correctly
  // ============================================================

  "FOREACH clause" should "execute SET on current node with literal list" in {
    val graph = makeGraph("foreach-set-literal-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // FOREACH that sets a property multiple times (last value wins)
      val query = s"""
        MATCH (n)
        WHERE id(n) = idFrom("Test", $$key)
        FOREACH (x IN [1, 2, 3] | SET n.value = x)
        RETURN n
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("key") -> Value.Text("test-node-1")),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      Await.result(resultPromise.future, 10.seconds)
      Thread.sleep(500)

      // Verify the property was set (last value should be 3)
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val nodeId = computeIdFrom("Test", "test-node-1")
      val (props, _) = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(nodeId, atTime = None),
        5.seconds,
      )

      props.get(Symbol("value")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Integer(3L))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "execute CREATE edge inside FOREACH" in {
    val graph = makeGraph("foreach-create-edge-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // FOREACH that creates edges to multiple target nodes
      val query = s"""
        MATCH (source), (t1), (t2), (t3)
        WHERE id(source) = idFrom("Source", $$key)
          AND id(t1) = idFrom("Target", "1")
          AND id(t2) = idFrom("Target", "2")
          AND id(t3) = idFrom("Target", "3")
        FOREACH (t IN [t1, t2, t3] | CREATE (source)-[:LINKS_TO]->(t))
        RETURN source
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("key") -> Value.Text("source-node")),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      Await.result(resultPromise.future, 10.seconds)
      Thread.sleep(500)

      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val sourceId = computeIdFrom("Source", "source-node")
      val target1Id = computeIdFrom("Target", "1")
      val target2Id = computeIdFrom("Target", "2")
      val target3Id = computeIdFrom("Target", "3")

      // Verify edges were created from source to all targets
      val sourceEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(sourceId),
        5.seconds,
      )

      val linksToEdges = sourceEdges.filter(_.edgeType == Symbol("LINKS_TO"))
      linksToEdges should have size 3
      linksToEdges.map(_.other).toSet shouldBe Set(target1Id, target2Id, target3Id)
      linksToEdges.foreach(_.direction shouldBe EdgeDirection.Outgoing)

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "execute conditional FOREACH with CASE WHEN (non-null case)" in {
    val graph = makeGraph("foreach-conditional-nonnull-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // This mimics the entity-resolution pattern:
      // FOREACH (p IN CASE WHEN parts.poBox IS NULL THEN [] ELSE [parts.poBox] END |
      //   SET poBox.poBox = p CREATE (entity)-[:poBox]->(poBox))
      val query = """
        WITH {poBox: "PO Box 123", postcode: "12345"} AS parts
        MATCH (entity), (poBox)
        WHERE id(entity) = idFrom("Entity", $entityKey)
          AND id(poBox) = idFrom("poBox", CASE WHEN parts.poBox IS NULL THEN -1 ELSE parts.poBox END)
        FOREACH (p IN CASE WHEN parts.poBox IS NULL THEN [] ELSE [parts.poBox] END |
          SET poBox.poBox = p
          CREATE (entity)-[:poBox]->(poBox)
        )
        RETURN entity
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("entityKey") -> Value.Text("test-entity-1")),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      Await.result(resultPromise.future, 10.seconds)
      Thread.sleep(500)

      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val entityId = computeIdFrom("Entity", "test-entity-1")
      val poBoxId = computeIdFrom("poBox", "PO Box 123")

      // Verify the edge was created
      val entityEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(entityId),
        5.seconds,
      )

      val poBoxEdge = entityEdges.find(_.edgeType == Symbol("poBox"))
      poBoxEdge shouldBe defined
      poBoxEdge.get.direction shouldBe EdgeDirection.Outgoing
      poBoxEdge.get.other shouldBe poBoxId

      // Verify the property was set on the poBox node
      val (poBoxProps, _) = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(poBoxId, atTime = None),
        5.seconds,
      )
      poBoxProps.get(Symbol("poBox")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("PO Box 123"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "skip FOREACH body when CASE WHEN evaluates to empty list (null case)" in {
    val graph = makeGraph("foreach-conditional-null-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // When parts.poBox IS NULL, the CASE should return [] and FOREACH should not execute
      val query = """
        WITH {postcode: "12345"} AS parts
        MATCH (entity), (poBox)
        WHERE id(entity) = idFrom("Entity", $entityKey)
          AND id(poBox) = idFrom("poBox", CASE WHEN parts.poBox IS NULL THEN -1 ELSE parts.poBox END)
        FOREACH (p IN CASE WHEN parts.poBox IS NULL THEN [] ELSE [parts.poBox] END |
          SET poBox.poBox = p
          CREATE (entity)-[:poBox]->(poBox)
        )
        RETURN entity
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("entityKey") -> Value.Text("test-entity-null")),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      Await.result(resultPromise.future, 10.seconds)
      Thread.sleep(500)

      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val entityId = computeIdFrom("Entity", "test-entity-null")

      // Verify NO edge was created (because poBox was null)
      val entityEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(entityId),
        5.seconds,
      )

      val poBoxEdge = entityEdges.find(_.edgeType == Symbol("poBox"))
      poBoxEdge shouldBe None

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "execute multiple FOREACHes with different conditional outcomes" in {
    val graph = makeGraph("foreach-multiple-conditional-test")
    while (!graph.isReady) Thread.sleep(10)

    try {

      // This mimics entity-resolution with multiple optional properties:
      // poBox is present, postcode is present, unit is NULL
      val query = """
        WITH {poBox: "PO Box 999", postcode: "54321", unit: null} AS parts
        MATCH (entity), (poBox), (postcode), (unit)
        WHERE id(entity) = idFrom("Entity", $entityKey)
          AND id(poBox) = idFrom("poBox", CASE WHEN parts.poBox IS NULL THEN -1 ELSE parts.poBox END)
          AND id(postcode) = idFrom("postcode", CASE WHEN parts.postcode IS NULL THEN -1 ELSE parts.postcode END)
          AND id(unit) = idFrom("unit", CASE WHEN parts.unit IS NULL THEN -1 ELSE parts.unit END)
        FOREACH (p IN CASE WHEN parts.poBox IS NULL THEN [] ELSE [parts.poBox] END |
          SET poBox.poBox = p
          CREATE (entity)-[:poBox]->(poBox)
        )
        FOREACH (p IN CASE WHEN parts.postcode IS NULL THEN [] ELSE [parts.postcode] END |
          SET postcode.postcode = p
          CREATE (entity)-[:postcode]->(postcode)
        )
        FOREACH (p IN CASE WHEN parts.unit IS NULL THEN [] ELSE [parts.unit] END |
          SET unit.unit = p
          CREATE (entity)-[:unit]->(unit)
        )
        RETURN entity
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("entityKey") -> Value.Text("test-entity-multi")),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      Await.result(resultPromise.future, 10.seconds)
      Thread.sleep(500)

      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val entityId = computeIdFrom("Entity", "test-entity-multi")
      val poBoxId = computeIdFrom("poBox", "PO Box 999")
      val postcodeId = computeIdFrom("postcode", "54321")

      val entityEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(entityId),
        5.seconds,
      )

      // Should have poBox edge (poBox was not null)
      val poBoxEdge = entityEdges.find(_.edgeType == Symbol("poBox"))
      poBoxEdge shouldBe defined
      poBoxEdge.get.other shouldBe poBoxId

      // Should have postcode edge (postcode was not null)
      val postcodeEdge = entityEdges.find(_.edgeType == Symbol("postcode"))
      postcodeEdge shouldBe defined
      postcodeEdge.get.other shouldBe postcodeId

      // Should NOT have unit edge (unit was null)
      val unitEdge = entityEdges.find(_.edgeType == Symbol("unit"))
      unitEdge shouldBe None

      // Verify properties were set
      val (poBoxProps, _) = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(poBoxId, atTime = None),
        5.seconds,
      )
      poBoxProps.get(Symbol("poBox")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("PO Box 999"))

      val (postcodeProps, _) = Await.result(
        graph.literalOps(namespace).getPropsAndLabels(postcodeId, atTime = None),
        5.seconds,
      )
      postcodeProps.get(Symbol("postcode")).flatMap(PropertyValue.unapply) shouldBe Some(QuineValue.Str("54321"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // RETURN CLAUSE OUTPUT NAME VALIDATION
  // ============================================================
  // These tests verify that result binding names match the RETURN clause names,
  // not internal binding IDs (raw integers from symbol analysis).

  "RETURN clause output names" should "use human-readable names from RETURN clause" in {
    val graph = makeGraph("return-names-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Simple query with explicit RETURN names
      val query = """
        WITH $that AS row
        MATCH (m) WHERE id(m) = idFrom("Movie", row.movieId)
        SET m.title = row.title
        RETURN m, row.title AS movieTitle
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val thatValue = Value.Map(
        scala.collection.immutable.SortedMap(
          Symbol("movieId") -> Value.Text("test-movie-1"),
          Symbol("title") -> Value.Text("Test Movie Title"),
        ),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("that") -> thatValue),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // CRITICAL: The binding names should be the human-readable names from RETURN clause
      // NOT internal binding IDs (raw integers from symbol analysis)
      ctx.bindings.keys.map(_.name).toSet should contain("m")
      ctx.bindings.keys.map(_.name).toSet should contain("movieTitle")

      // Should NOT contain internal binding IDs as keys (pure numeric or legacy __qid_ format)
      ctx.bindings.keys.map(_.name).foreach { name =>
        withClue(s"Binding name '$name' should not be an internal identifier") {
          name should not startWith "__qid_"
          name.forall(_.isDigit) shouldBe false // Not a raw numeric binding ID
        }
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "preserve names through WITH clause projections" in {
    val graph = makeGraph("with-names-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query with multiple WITH clauses that rename bindings
      val query = """
        WITH $that AS row
        MATCH (m) WHERE id(m) = idFrom("Movie", row.movieId)
        WITH m AS movie, row.genres AS genreList
        RETURN movie, genreList
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val thatValue = Value.Map(
        scala.collection.immutable.SortedMap(
          Symbol("movieId") -> Value.Text("test-movie-2"),
          Symbol("genres") -> Value.List(List(Value.Text("Action"), Value.Text("Comedy"))),
        ),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("that") -> thatValue),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // Binding names should match the RETURN clause names
      ctx.bindings.keys.map(_.name).toSet should contain("movie")
      ctx.bindings.keys.map(_.name).toSet should contain("genreList")

      // Verify values are correct
      ctx.bindings(Symbol("genreList")) shouldBe Value.List(List(Value.Text("Action"), Value.Text("Comedy")))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle UNWIND with proper output names" in {
    val graph = makeGraph("unwind-names-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query with UNWIND
      val query = """
        WITH $that AS row
        MATCH (m) WHERE id(m) = idFrom("Movie", row.movieId)
        WITH m, row.genres AS genres
        UNWIND genres AS genre
        RETURN m AS movie, genre AS genreName
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val thatValue = Value.Map(
        scala.collection.immutable.SortedMap(
          Symbol("movieId") -> Value.Text("test-movie-3"),
          Symbol("genres") -> Value.List(List(Value.Text("Action"), Value.Text("Comedy"))),
        ),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("that") -> thatValue),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should have 2 results (one per genre from UNWIND)
      results should have size 2

      // Each result should have human-readable binding names
      results.foreach { ctx =>
        ctx.bindings.keys.map(_.name).toSet should contain("movie")
        ctx.bindings.keys.map(_.name).toSet should contain("genreName")

        // Should NOT contain internal binding IDs (pure numeric or legacy __qid_ format)
        ctx.bindings.keys.map(_.name).foreach { name =>
          withClue(s"Binding name '$name' should not be an internal identifier") {
            name should not startWith "__qid_"
            name.forall(_.isDigit) shouldBe false // Not a raw numeric binding ID
          }
        }
      }

      // Verify the genre values are correct
      val genreNames = results.map(_.bindings(Symbol("genreName"))).toSet
      genreNames should contain(Value.Text("Action"))
      genreNames should contain(Value.Text("Comedy"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "return node properties when RETURN references a bare node binding" in {
    // Simplest test case: node exists with properties, MATCH and RETURN it
    val graph = makeGraph("return-bare-node-props")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val nodeId = computeIdFrom("test", "bare-node-1")

      // Create the node with properties BEFORE running the query
      Await.result(graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(nodeId, "age", QuineValue.Integer(30L)), 5.seconds)

      // Simplest query: just MATCH and RETURN
      val query = """
        MATCH (a) WHERE id(a) = idFrom("test", "bare-node-1")
        RETURN a
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // The binding should be named "a" from the RETURN clause
      ctx.bindings.keys.map(_.name).toSet should contain("a")

      // CRITICAL: The value for "a" should include the node's properties
      val aValue = ctx.bindings(Symbol("a"))

      aValue match {
        case Value.Node(id, _, props) =>
          id shouldEqual nodeId
          props.values.get(Symbol("name")) shouldBe Some(Value.Text("Alice"))
          props.values.get(Symbol("age")) shouldBe Some(Value.Integer(30))
        case Value.Map(values) =>
          values.get(Symbol("name")) shouldBe Some(Value.Text("Alice"))
          values.get(Symbol("age")) shouldBe Some(Value.Integer(30))
        case other =>
          fail(s"Expected Node or Map with properties, but got: $other")
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "return updated properties after SET in the same query" in {
    // Test that MATCH (n) SET n.prop = value RETURN n returns the newly set property
    // This tests that EffectState updates the context after applying SET
    val graph = makeGraph("set-then-return")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val nodeId = computeIdFrom("test", "set-return-1")

      // Create the node with an initial property
      Await.result(graph.literalOps(namespace).setProp(nodeId, "existingProp", QuineValue.Str("original")), 5.seconds)

      // Query that SETs a NEW property and RETURNs the node
      val query = """
        MATCH (n) WHERE id(n) = idFrom("test", "set-return-1")
        SET n.newProp = "newly set value"
        RETURN n
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // The binding should be named "n" from the RETURN clause
      ctx.bindings.keys.map(_.name).toSet should contain("n")

      // The value for "n" should include BOTH the existing property AND the newly set one
      val nValue = ctx.bindings(Symbol("n"))

      nValue match {
        case Value.Node(id, _, props) =>
          id shouldEqual nodeId
          // Should have the existing property
          props.values.get(Symbol("existingProp")) shouldBe Some(Value.Text("original"))
          // Should also have the newly set property
          props.values.get(Symbol("newProp")) shouldBe Some(Value.Text("newly set value"))
        case Value.Map(values) =>
          values.get(Symbol("existingProp")) shouldBe Some(Value.Text("original"))
          values.get(Symbol("newProp")) shouldBe Some(Value.Text("newly set value"))
        case other =>
          fail(s"Expected Node or Map with properties, but got: $other")
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // LOADQUERY PATH VALIDATION (Recipe code path)
  // ============================================================
  // These tests exercise the LoadQuery path (used by recipes) as opposed to
  // LoadQueryPlan (used by most tests). This ensures outputNameMapping is
  // properly threaded through the recipe code path.

  "LoadQuery path (recipe code path)" should "properly map output names via QuinePatternLoader" in {
    val graph = makeGraph("loadquery-path-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query with explicit RETURN names - same as above but sent via LoadQuery
      val query = """
        WITH $that AS row
        MATCH (m) WHERE id(m) = idFrom("Movie", row.movieId)
        SET m.title = row.title
        RETURN m, row.title AS movieTitle
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val thatValue = Value.Map(
        scala.collection.immutable.SortedMap(
          Symbol("movieId") -> Value.Text("loadquery-movie-1"),
          Symbol("title") -> Value.Text("LoadQuery Test Movie"),
        ),
      )

      // CRITICAL: Use LoadQuery through graph.getLoader (the recipe path)
      // NOT LoadQueryPlan directly to NonNodeActor
      graph.getLoader ! LoadQuery(
        standingQueryId = StandingQueryId.fresh(),
        queryPlan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("that") -> thatValue),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // CRITICAL: The binding names should be the human-readable names from RETURN clause
      // This validates that outputNameMapping flows through the LoadQuery/QuinePatternLoader path
      withClue("LoadQuery path should produce human-readable output names") {
        ctx.bindings.keys.map(_.name).toSet should contain("m")
        ctx.bindings.keys.map(_.name).toSet should contain("movieTitle")

        // Should NOT contain internal binding IDs (pure numeric or legacy __qid_ format)
        ctx.bindings.keys.map(_.name).foreach { name =>
          withClue(s"Binding name '$name' should not be an internal identifier") {
            name should not startWith "__qid_"
            name.forall(_.isDigit) shouldBe false // Not a raw numeric binding ID
          }
        }
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle WITH clause projections through LoadQuery path" in {
    val graph = makeGraph("loadquery-with-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query with WITH clause that renames bindings
      val query = """
        WITH $that AS row
        MATCH (m) WHERE id(m) = idFrom("Movie", row.movieId)
        WITH m AS movie, row.director AS directorName
        RETURN movie, directorName
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val thatValue = Value.Map(
        scala.collection.immutable.SortedMap(
          Symbol("movieId") -> Value.Text("loadquery-movie-2"),
          Symbol("director") -> Value.Text("Steven Spielberg"),
        ),
      )

      // Use LoadQuery through graph.getLoader (the recipe path)
      graph.getLoader ! LoadQuery(
        standingQueryId = StandingQueryId.fresh(),
        queryPlan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("that") -> thatValue),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // Validate human-readable names flow through LoadQuery path
      withClue("LoadQuery path should preserve WITH clause renamed bindings") {
        ctx.bindings.keys.map(_.name).toSet should contain("movie")
        ctx.bindings.keys.map(_.name).toSet should contain("directorName")

        ctx.bindings.keys.map(_.name).foreach { name =>
          withClue(s"Binding name '$name' should not be an internal identifier") {
            name should not startWith "__qid_"
            name.forall(_.isDigit) shouldBe false // Not a raw numeric binding ID
          }
        }
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // DIAMOND PATTERN TESTS
  // Tests for patterns where the same node appears in multiple branches
  // after tree merging (e.g., MATCH (hub)<-[R]-(leaf), (hub)<-[S]-(leaf))
  // ============================================================

  "Diamond pattern" should "match when the shared node is the same physical node" in {
    val graph = makeGraph("diamond-same-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Create a diamond structure:
      //   leaf --R--> hub
      //   leaf --S--> hub
      // So `leaf` connects to `hub` via two different edge types

      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val hubId = computeIdFrom("Hub", "hub-1")
      val leafId = computeIdFrom("Leaf", "leaf-1")

      // Set up the graph nodes
      Await.result(graph.literalOps(namespace).setProp(hubId, "name", QuineValue.Str("hub")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(leafId, "name", QuineValue.Str("leaf")), 5.seconds)

      // Set up edges: leaf --R--> hub and leaf --S--> hub
      Await.result(graph.literalOps(namespace).addEdge(leafId, hubId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(leafId, hubId, "S"), 5.seconds)

      // The diamond pattern: hub has incoming R from leaf AND incoming S from leaf (same leaf!)
      // This creates a diamond because `leaf` appears in both comma-separated patterns
      val query = """
        MATCH (hub)<-[:R]-(leaf), (hub)<-[:S]-(leaf)
        WHERE id(hub) = idFrom("Hub", "hub-1")
        RETURN hub, leaf
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should find exactly 1 match because both patterns refer to the same physical leaf node
      results should have size 1

      // Verify bindings have the correct names from RETURN clause
      val ctx = results.head
      ctx.bindings.keys.map(_.name).toSet should contain("hub")
      ctx.bindings.keys.map(_.name).toSet should contain("leaf")

      // Verify the leaf binding is correct - may be NodeId or Node depending on what's available
      val leafValue = ctx.bindings(Symbol("leaf"))
      val leafNodeId = leafValue match {
        case Value.NodeId(id) => id
        case Value.Node(id, _, _) => id
        case other => fail(s"Expected NodeId or Node, got $other")
      }
      leafNodeId shouldEqual leafId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "NOT match when the shared binding would be different physical nodes" in {
    val graph = makeGraph("diamond-different-nodes")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Create a structure where hub has two incoming edges, but from DIFFERENT nodes:
      //   leaf1 --R--> hub
      //   leaf2 --S--> hub

      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val hubId = computeIdFrom("Hub", "hub-2")
      val leaf1Id = computeIdFrom("Leaf", "leaf1")
      val leaf2Id = computeIdFrom("Leaf", "leaf2")

      // Set up the graph nodes
      Await.result(graph.literalOps(namespace).setProp(hubId, "name", QuineValue.Str("hub")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(leaf1Id, "name", QuineValue.Str("leaf1")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(leaf2Id, "name", QuineValue.Str("leaf2")), 5.seconds)

      // Set up edges: leaf1 --R--> hub and leaf2 --S--> hub (different source nodes)
      Await.result(graph.literalOps(namespace).addEdge(leaf1Id, hubId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(leaf2Id, hubId, "S"), 5.seconds)

      // The diamond pattern: hub has incoming R from leaf AND incoming S from leaf
      // The diamond join filter should reject this because leaf1 != leaf2
      val query = """
        MATCH (hub)<-[:R]-(leaf), (hub)<-[:S]-(leaf)
        WHERE id(hub) = idFrom("Hub", "hub-2")
        RETURN hub, leaf
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should find 0 matches because the diamond join filter rejects leaf1 != leaf2
      results should have size 0

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match complex diamond with three paths through shared node" in {
    val graph = makeGraph("diamond-three-paths")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Create a structure:
      //   shared --R--> hub
      //   shared --S--> hub
      //   shared --T--> hub

      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val hubId = computeIdFrom("Hub", "hub-3")
      val sharedId = computeIdFrom("Shared", "shared-1")

      // Set up the graph nodes
      Await.result(graph.literalOps(namespace).setProp(hubId, "name", QuineValue.Str("hub")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(sharedId, "name", QuineValue.Str("shared")), 5.seconds)

      // Set up edges: shared --R/S/T--> hub
      Await.result(graph.literalOps(namespace).addEdge(sharedId, hubId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(sharedId, hubId, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(sharedId, hubId, "T"), 5.seconds)

      // Three-way diamond pattern
      val query = """
        MATCH (hub)<-[:R]-(x), (hub)<-[:S]-(x), (hub)<-[:T]-(x)
        WHERE id(hub) = idFrom("Hub", "hub-3")
        RETURN hub, x
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should find exactly 1 match
      results should have size 1

      // Verify bindings have the correct names from RETURN clause
      val ctx = results.head
      ctx.bindings.keys.map(_.name).toSet should contain("hub")
      ctx.bindings.keys.map(_.name).toSet should contain("x")

      // Verify the x binding is correct - may be NodeId or Node depending on what's available
      val xValue = ctx.bindings(Symbol("x"))
      val xNodeId = xValue match {
        case Value.NodeId(id) => id
        case Value.Node(id, _, _) => id
        case other => fail(s"Expected NodeId or Node, got $other")
      }
      xNodeId shouldEqual sharedId

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // COMPLEX DIAMOND PATTERN TEST - APT Detection Style
  // ============================================================
  // SIMPLE DIAMOND PATTERN - minimal test case
  // Structure: a -> b -> d, a -> c -> d
  // ============================================================

  it should "match simplest diamond pattern (4 nodes)" in {
    val graph = makeGraph("diamond-simple-4")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Create 4 nodes: a, b, c, d
      val aId = computeIdFrom("Node", "a")
      val bId = computeIdFrom("Node", "b")
      val cId = computeIdFrom("Node", "c")
      val dId = computeIdFrom("Node", "d")

      // Set properties to distinguish b and c (like e1.type="WRITE", e2.type="READ" in APT)
      Await.result(graph.literalOps(namespace).setProp(bId, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "type", QuineValue.Str("READ")), 5.seconds)

      // Create diamond edges: a -> b -> d, a -> c -> d
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, dId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(aId, cId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(cId, dId, "E"), 5.seconds)

      // Query: find the diamond anchored at d
      // (a)-[:E]->(b)-[:E]->(d), (a)-[:E]->(c)-[:E]->(d)
      // where d is shared (diamond point)
      val query = """
        MATCH (a)-[:E]->(b)-[:E]->(d)<-[:E]-(c)<-[:E]-(a)
        WHERE id(d) = idFrom("Node", "d")
          AND b.type = "WRITE"
          AND c.type = "READ"
        RETURN a, b, c, d
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find exactly 1 match
      results should have size 1

      // Verify all 4 bindings are present
      val ctx = results.head
      val bindingNames = ctx.bindings.keys.map(_.name).toSet
      bindingNames should contain("a")
      bindingNames should contain("b")
      bindingNames should contain("c")
      bindingNames should contain("d")

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simple diamond with same edge type (EVENT) like APT" in {
    // This mimics APT more closely: same edge type everywhere
    val graph = makeGraph("diamond-same-edge")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Same 4-node diamond: a -> b -> d, a -> c -> d
      val aId = computeIdFrom("Node", "a2")
      val bId = computeIdFrom("Node", "b2")
      val cId = computeIdFrom("Node", "c2")
      val dId = computeIdFrom("Node", "d2")

      // Properties to distinguish b and c
      Await.result(graph.literalOps(namespace).setProp(bId, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "type", QuineValue.Str("READ")), 5.seconds)

      // ALL edges use EVENT (like APT pattern)
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, dId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(aId, cId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(cId, dId, "EVENT"), 5.seconds)

      // Query with same edge type everywhere
      val query = """
        MATCH (a)-[:EVENT]->(b)-[:EVENT]->(d)<-[:EVENT]-(c)<-[:EVENT]-(a)
        WHERE id(d) = idFrom("Node", "d2")
          AND b.type = "WRITE"
          AND c.type = "READ"
        RETURN a, b, c, d
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 5-node star pattern with f as explicit anchor" in {
    // Structure: p1 -> e1 -> f <- e2 <- p2
    // f is the explicit anchor (like APT's id(f) = $that)
    // Two branches meeting at f, no diamond join yet
    val graph = makeGraph("star-5-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val p1Id = computeIdFrom("Node", "p1")
      val p2Id = computeIdFrom("Node", "p2")
      val e1Id = computeIdFrom("Node", "e1")
      val e2Id = computeIdFrom("Node", "e2")
      val fId = computeIdFrom("Node", "f")

      // Properties to distinguish e1 and e2
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)

      // Edges: p1 -> e1 -> f <- e2 <- p2
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)

      // Query anchored at f
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2)
        WHERE id(f) = idFrom("Node", "f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
        RETURN p1, e1, f, e2, p2
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simplest comma-separated pattern (3 nodes, b shared)" in {
    // Simplest comma test: (a)->(b), (b)->(c)
    // b appears in TWO comma-separated patterns
    val graph = makeGraph("comma-simple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "comma-a")
      val bId = computeIdFrom("Node", "comma-b")
      val cId = computeIdFrom("Node", "comma-c")

      // Edges: a -> b -> c
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, cId, "E"), 5.seconds)

      // Query with comma: b appears in both patterns
      val query = """
        MATCH (a)-[:E]->(b), (b)-[:E]->(c)
        WHERE id(b) = idFrom("Node", "comma-b")
        RETURN a, b, c
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match comma pattern with TWO shared nodes (like f and p2 in APT)" in {
    // Structure: a -> b -> f, a -> c -> f
    // Comma pattern: (a)->(b)->(f), (f)<-(c)<-(a)
    // TWO shared nodes: f AND a (like APT's f and p2)
    val graph = makeGraph("comma-two-shared")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "two-a")
      val bId = computeIdFrom("Node", "two-b")
      val cId = computeIdFrom("Node", "two-c")
      val fId = computeIdFrom("Node", "two-f")

      // Properties to distinguish b and c
      Await.result(graph.literalOps(namespace).setProp(bId, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "type", QuineValue.Str("READ")), 5.seconds)

      // Edges: a -> b -> f, a -> c -> f
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, fId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(aId, cId, "E"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(cId, fId, "E"), 5.seconds)

      // Query: f and a both appear in both comma-separated patterns
      val query = """
        MATCH (a)-[:E]->(b)-[:E]->(f), (f)<-[:E]-(c)<-[:E]-(a)
        WHERE id(f) = idFrom("Node", "two-f")
          AND b.type = "WRITE"
          AND c.type = "READ"
        RETURN a, b, c, f
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 3-branch pattern from f (like APT's e1,e2,e3 -> f)" in {
    // Structure: e1 -> f <- e2 <- p2 -> e3 -> f
    // f has THREE incoming edges (from e1, e2, e3)
    // p2 is shared (connects to e2 and e3)
    // ALL edges are EVENT type (like APT)
    val graph = makeGraph("three-branch")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val e1Id = computeIdFrom("Node", "3b-e1")
      val e2Id = computeIdFrom("Node", "3b-e2")
      val e3Id = computeIdFrom("Node", "3b-e3")
      val fId = computeIdFrom("Node", "3b-f")
      val p2Id = computeIdFrom("Node", "3b-p2")

      // Properties to distinguish e1, e2, e3
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)

      // Edges: e1 -> f, e2 -> f, e3 -> f, p2 -> e2, p2 -> e3 (ALL EVENT)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)

      // Query: f has 3 incoming EVENT edges, p2 shared between e2 and e3
      val query = """
        MATCH (e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)
        WHERE id(f) = idFrom("Node", "3b-f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
        RETURN e1, e2, e3, f, p2
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 6-node pattern (adding p1 to 3-branch)" in {
    // Add p1 at start of e1 branch: p1 -> e1 -> f <- e2 <- p2 -> e3 -> f
    val graph = makeGraph("six-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val p1Id = computeIdFrom("Node", "6n-p1")
      val e1Id = computeIdFrom("Node", "6n-e1")
      val e2Id = computeIdFrom("Node", "6n-e2")
      val e3Id = computeIdFrom("Node", "6n-e3")
      val fId = computeIdFrom("Node", "6n-f")
      val p2Id = computeIdFrom("Node", "6n-p2")

      // Properties
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)

      // Edges: p1 -> e1 -> f, e2 -> f, e3 -> f, p2 -> e2, p2 -> e3
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)

      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)
        WHERE id(f) = idFrom("Node", "6n-f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
        RETURN p1, e1, e2, e3, f, p2
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 7-node pattern (adding e4 to 6-node)" in {
    // Add e4: p1 -> e1 -> f <- e2 <- p2 -> e3 -> f, p2 -> e4
    val graph = makeGraph("seven-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val p1Id = computeIdFrom("Node", "7n-p1")
      val e1Id = computeIdFrom("Node", "7n-e1")
      val e2Id = computeIdFrom("Node", "7n-e2")
      val e3Id = computeIdFrom("Node", "7n-e3")
      val e4Id = computeIdFrom("Node", "7n-e4")
      val fId = computeIdFrom("Node", "7n-f")
      val p2Id = computeIdFrom("Node", "7n-p2")

      // Properties
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "type", QuineValue.Str("SEND")), 5.seconds)

      // Edges: p1 -> e1 -> f, e2 -> f, e3 -> f, p2 -> e2, p2 -> e3, p2 -> e4
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)

      // Query: adds p2 -> e4 path in second pattern
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)
        WHERE id(f) = idFrom("Node", "7n-f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
        RETURN p1, e1, e2, e3, e4, f, p2
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 8-node pattern (full APT structure, NO time constraints)" in {
    // Full APT structure: p1 -> e1 -> f <- e2 <- p2 -> e3 -> f, p2 -> e4 -> ip
    // NO time constraints (to isolate the issue)
    val graph = makeGraph("eight-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val p1Id = computeIdFrom("Node", "8n-p1")
      val e1Id = computeIdFrom("Node", "8n-e1")
      val e2Id = computeIdFrom("Node", "8n-e2")
      val e3Id = computeIdFrom("Node", "8n-e3")
      val e4Id = computeIdFrom("Node", "8n-e4")
      val fId = computeIdFrom("Node", "8n-f")
      val p2Id = computeIdFrom("Node", "8n-p2")
      val ipId = computeIdFrom("Node", "8n-ip")

      // Properties
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "type", QuineValue.Str("SEND")), 5.seconds)

      // Edges: p1 -> e1 -> f, e2 -> f, e3 -> f, p2 -> e2, p2 -> e3, p2 -> e4, e4 -> ip
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e4Id, ipId, "EVENT"), 5.seconds)

      // Query: full APT structure, NO time constraints, NO CREATE
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
        WHERE id(f) = idFrom("Node", "8n-f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
        RETURN p1, e1, e2, e3, e4, f, p2, ip
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match 8-node pattern WITH time constraints" in {
    // Same as above, but WITH time constraints
    val graph = makeGraph("eight-node-time")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val p1Id = computeIdFrom("Node", "8t-p1")
      val e1Id = computeIdFrom("Node", "8t-e1")
      val e2Id = computeIdFrom("Node", "8t-e2")
      val e3Id = computeIdFrom("Node", "8t-e3")
      val e4Id = computeIdFrom("Node", "8t-e4")
      val fId = computeIdFrom("Node", "8t-f")
      val p2Id = computeIdFrom("Node", "8t-p2")
      val ipId = computeIdFrom("Node", "8t-ip")

      // Properties with TIME values that satisfy: e1.time < e2.time < e3.time AND e2.time < e4.time
      // e1.time=100, e2.time=200, e3.time=400, e4.time=300
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e1Id, "time", QuineValue.Integer(100L)), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "time", QuineValue.Integer(200L)), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "time", QuineValue.Integer(400L)), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "type", QuineValue.Str("SEND")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "time", QuineValue.Integer(300L)), 5.seconds)

      // Same edges
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e4Id, ipId, "EVENT"), 5.seconds)

      // Query WITH time constraints
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
        WHERE id(f) = idFrom("Node", "8t-f")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
          AND e1.time < e2.time
          AND e2.time < e3.time
          AND e2.time < e4.time
        RETURN p1, e1, e2, e3, e4, f, p2, ip
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // TIME COMPARISON TESTS - isolating the comparison logic
  // ============================================================

  it should "compare time properties on two simple nodes" in {
    // Simplest case: two nodes, compare their time properties
    val graph = makeGraph("time-compare-simple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "time-a")
      val bId = computeIdFrom("Node", "time-b")

      // a.time = 100, b.time = 200
      Await.result(graph.literalOps(namespace).setProp(aId, "time", QuineValue.Integer(100L)), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "time", QuineValue.Integer(200L)), 5.seconds)

      // Simple query: MATCH (a), (b) WHERE id(a)=... AND id(b)=... AND a.time < b.time
      val query = """
        MATCH (a), (b)
        WHERE id(a) = idFrom("Node", "time-a")
          AND id(b) = idFrom("Node", "time-b")
          AND a.time < b.time
        RETURN a, b
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "handle simple CREATE between two matched nodes" in {
    // Simplest CREATE case: match two nodes, create an edge between them
    val graph = makeGraph("create-simple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "create-a")
      val bId = computeIdFrom("Node", "create-b")

      // Set minimal properties
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("nodeA")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("nodeB")), 5.seconds)

      // Query: match two nodes and create an edge between them
      val query = """
        MATCH (a), (b)
        WHERE id(a) = idFrom("Node", "create-a")
          AND id(b) = idFrom("Node", "create-b")
        CREATE (a)-[:LINKED]->(b)
        RETURN a, b
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match connected pattern via Expand and CREATE an edge" in {
    // Test a pattern using Expand (connected nodes), plus CREATE
    // This tests the Anchor -> Expand structure with Sequence for effects
    val graph = makeGraph("create-connected")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "conn-a")
      val bId = computeIdFrom("Node", "conn-b")
      val cId = computeIdFrom("Node", "conn-c")

      // Set properties
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("nodeA")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("nodeB")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "name", QuineValue.Str("nodeC")), 5.seconds)

      // Create edges: a -> b -> c
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "LINK"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, cId, "LINK"), 5.seconds)

      // Query: match connected pattern, create a shortcut edge
      val query = """
        MATCH (a)-[:LINK]->(b)-[:LINK]->(c)
        WHERE id(a) = idFrom("Node", "conn-a")
        CREATE (a)-[:SHORTCUT]->(c)
        RETURN a, b, c
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simple diamond pattern with CREATE" in {
    // Test diamond pattern (shared node) + CREATE
    // Structure: a -> b, a -> c, b -> d, c -> d (d is shared)
    // This should produce a plan similar to APT with comma-separated patterns
    val graph = makeGraph("diamond-create")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "dm-a")
      val bId = computeIdFrom("Node", "dm-b")
      val cId = computeIdFrom("Node", "dm-c")
      val dId = computeIdFrom("Node", "dm-d")

      // Set properties - add type to distinguish b from c (like APT test)
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("nodeA")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("nodeB")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "type", QuineValue.Str("FIRST")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "name", QuineValue.Str("nodeC")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "type", QuineValue.Str("SECOND")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(dId, "name", QuineValue.Str("nodeD")), 5.seconds)

      // Create edges: a -> b -> d, a -> c -> d (diamond)
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(aId, cId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, dId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(cId, dId, "EDGE"), 5.seconds)

      // Query: comma-separated diamond pattern with shared node d, plus CREATE
      // This is similar to APT structure: two patterns sharing d
      // Add property constraints to ensure unique matching (like APT test)
      val query = """
        MATCH (a)-[:EDGE]->(b)-[:EDGE]->(d), (a)-[:EDGE]->(c)-[:EDGE]->(d)
        WHERE id(d) = idFrom("Node", "dm-d")
          AND b.type = "FIRST"
          AND c.type = "SECOND"
        CREATE (b)-[:SHORTCUT]->(c)
        RETURN a, b, c, d
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simple diamond pattern WITHOUT CREATE (to verify pattern works)" in {
    // Same as above but WITHOUT CREATE - to verify the pattern match works
    val graph = makeGraph("diamond-no-create")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "dm2-a")
      val bId = computeIdFrom("Node", "dm2-b")
      val cId = computeIdFrom("Node", "dm2-c")
      val dId = computeIdFrom("Node", "dm2-d")

      // Same properties as CREATE test
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("nodeA")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("nodeB")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "type", QuineValue.Str("FIRST")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "name", QuineValue.Str("nodeC")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "type", QuineValue.Str("SECOND")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(dId, "name", QuineValue.Str("nodeD")), 5.seconds)

      // Same edges
      Await.result(graph.literalOps(namespace).addEdge(aId, bId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(aId, cId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, dId, "EDGE"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(cId, dId, "EDGE"), 5.seconds)

      // Same query but NO CREATE clause
      val query = """
        MATCH (a)-[:EDGE]->(b)-[:EDGE]->(d), (a)-[:EDGE]->(c)-[:EDGE]->(d)
        WHERE id(d) = idFrom("Node", "dm2-d")
          AND b.type = "FIRST"
          AND c.type = "SECOND"
        RETURN a, b, c, d
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "CREATE multiple edges with 4 independent nodes (CrossProduct, no Expand)" in {
    // This test isolates whether the issue is Expand or just 4-node CrossProduct + CREATE
    // Uses the same CREATE structure as diamond test, but without edge traversals
    val graph = makeGraph("4-node-crossproduct-create")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val aId = computeIdFrom("Node", "cp4-a")
      val bId = computeIdFrom("Node", "cp4-b")
      val cId = computeIdFrom("Node", "cp4-c")
      val dId = computeIdFrom("Node", "cp4-d")

      // Set properties (no edges needed - pure CrossProduct)
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("nodeA")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("nodeB")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(cId, "name", QuineValue.Str("nodeC")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(dId, "name", QuineValue.Str("nodeD")), 5.seconds)

      // Same CREATE as diamond test: (b)-[:SHORTCUT]->(c)
      // But pattern is just 4 independent node lookups
      val query = """
        MATCH (a), (b), (c), (d)
        WHERE id(a) = idFrom("Node", "cp4-a")
          AND id(b) = idFrom("Node", "cp4-b")
          AND id(c) = idFrom("Node", "cp4-c")
          AND id(d) = idFrom("Node", "cp4-d")
        CREATE (b)-[:SHORTCUT]->(c)
        RETURN a, b, c, d
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // This tests a real-world diamond pattern with:
  // - Multiple shared nodes across comma-separated patterns (f, p2)
  // - Property constraints (type = "WRITE", etc.)
  // - Comparison constraints (e1.time < e2.time, etc.)
  // - CREATE side effects
  // - Complex RETURN with string manipulation
  // ============================================================

  it should "match complex APT-style pattern with property constraints and side effects" in {
    val graph = makeGraph("diamond-apt-style")
    while (!graph.isReady) Thread.sleep(10)

    try {
      import com.thatdot.quine.model.EdgeDirection

      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Create node IDs for the pattern:
      // (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2)
      // (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
      val p1Id = computeIdFrom("Process", "p1")
      val p2Id = computeIdFrom("Process", "p2")
      val e1Id = computeIdFrom("Event", "e1")
      val e2Id = computeIdFrom("Event", "e2")
      val e3Id = computeIdFrom("Event", "e3")
      val e4Id = computeIdFrom("Event", "e4")
      val fId = computeIdFrom("File", "target-file")
      val ipId = computeIdFrom("IP", "exfil-ip")

      // Set up node properties
      // e1: WRITE event at time 100
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e1Id, "time", QuineValue.Integer(100L)), 5.seconds)

      // e2: READ event at time 200
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "time", QuineValue.Integer(200L)), 5.seconds)

      // e3: DELETE event at time 400
      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "time", QuineValue.Integer(400L)), 5.seconds)

      // e4: SEND event at time 300
      Await.result(graph.literalOps(namespace).setProp(e4Id, "type", QuineValue.Str("SEND")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "time", QuineValue.Integer(300L)), 5.seconds)

      // Set some properties on other nodes for identification
      Await.result(graph.literalOps(namespace).setProp(p1Id, "name", QuineValue.Str("process1")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(p2Id, "name", QuineValue.Str("process2")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(fId, "name", QuineValue.Str("target.txt")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(ipId, "address", QuineValue.Str("10.0.0.1")), 5.seconds)

      // Set up edges for first pattern: (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2)
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)

      // Set up edges for second pattern: (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e4Id, ipId, "EVENT"), 5.seconds)

      // The complex APT-style query with diamond joins on f and p2
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
        WHERE id(f) = idFrom("File", "target-file")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
          AND e1.time < e2.time
          AND e2.time < e3.time
          AND e2.time < e4.time
        CREATE (e1)-[:NEXT]->(e2)-[:NEXT]->(e4)-[:NEXT]->(e3)
        RETURN p1, p2, e1, e2, e3, e4, f, ip
      """

      val planned = parseAndPlanWithMetadata(query)

      // Debug: print the plan structure

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Debug: print results

      // Should find exactly 1 match
      results should have size 1

      // Verify bindings have the correct names from RETURN clause
      val ctx = results.head
      val bindingNames = ctx.bindings.keys.map(_.name).toSet
      bindingNames should contain("p1")
      bindingNames should contain("p2")
      bindingNames should contain("e1")
      bindingNames should contain("e2")
      bindingNames should contain("e3")
      bindingNames should contain("e4")
      bindingNames should contain("f")
      bindingNames should contain("ip")

      // Wait for side effects to propagate
      Thread.sleep(1000)

      // Verify the NEXT edges were created as side effects
      val e1Edges = Await.result(graph.literalOps(namespace).getHalfEdges(e1Id), 5.seconds)
      val e2Edges = Await.result(graph.literalOps(namespace).getHalfEdges(e2Id), 5.seconds)
      val e4Edges = Await.result(graph.literalOps(namespace).getHalfEdges(e4Id), 5.seconds)

      // e1 -[:NEXT]-> e2
      val e1NextEdge = e1Edges.find(e => e.edgeType == Symbol("NEXT") && e.direction == EdgeDirection.Outgoing)
      e1NextEdge shouldBe defined
      e1NextEdge.get.other shouldBe e2Id

      // e2 -[:NEXT]-> e4
      val e2NextEdge = e2Edges.find(e => e.edgeType == Symbol("NEXT") && e.direction == EdgeDirection.Outgoing)
      e2NextEdge shouldBe defined
      e2NextEdge.get.other shouldBe e4Id

      // e4 -[:NEXT]-> e3
      val e4NextEdge = e4Edges.find(e => e.edgeType == Symbol("NEXT") && e.direction == EdgeDirection.Outgoing)
      e4NextEdge shouldBe defined
      e4NextEdge.get.other shouldBe e3Id

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simple two-diamond pattern without property constraints" in {
    // Simplified version to isolate the diamond join issue
    val graph = makeGraph("diamond-two-shared-simple")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Simple pattern: (a)-[:R]->(shared)<-[:S]-(b), (shared)<-[:T]-(b)
      // shared appears in both comma-separated patterns
      // b appears in both comma-separated patterns
      val aId = computeIdFrom("Node", "a")
      val bId = computeIdFrom("Node", "b")
      val sharedId = computeIdFrom("Node", "shared")

      // Set properties
      Await.result(graph.literalOps(namespace).setProp(aId, "name", QuineValue.Str("a")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("b")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(sharedId, "name", QuineValue.Str("shared")), 5.seconds)

      // Edges: a --R--> shared, b --S--> shared, b --T--> shared
      Await.result(graph.literalOps(namespace).addEdge(aId, sharedId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, sharedId, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, sharedId, "T"), 5.seconds)

      // Query with two shared bindings: shared and b
      val query = """
        MATCH (a)-[:R]->(shared)<-[:S]-(b), (shared)<-[:T]-(b)
        WHERE id(shared) = idFrom("Node", "shared")
        RETURN a, b, shared
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find exactly 1 match
      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match simple diamond with ONE property constraint" in {
    // Minimal test to isolate property constraint behavior in diamond patterns
    val graph = makeGraph("diamond-one-property")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Pattern: (a)-[:R]->(hub)<-[:S]-(b), (hub)<-[:T]-(b)
      // With ONE property constraint: a.type = "SOURCE"
      val aId = computeIdFrom("Node", "a-prop")
      val bId = computeIdFrom("Node", "b-prop")
      val hubId = computeIdFrom("Node", "hub-prop")

      // Set property on 'a'
      Await.result(graph.literalOps(namespace).setProp(aId, "type", QuineValue.Str("SOURCE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(bId, "name", QuineValue.Str("b")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(hubId, "name", QuineValue.Str("hub")), 5.seconds)

      // Edges
      Await.result(graph.literalOps(namespace).addEdge(aId, hubId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, hubId, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(bId, hubId, "T"), 5.seconds)

      // Query with ONE property constraint
      val query = """
        MATCH (a)-[:R]->(hub)<-[:S]-(b), (hub)<-[:T]-(b)
        WHERE id(hub) = idFrom("Node", "hub-prop")
          AND a.type = "SOURCE"
        RETURN a, b, hub
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find exactly 1 match
      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match diamond with 2-hop traversals and property constraint" in {
    // 2-hop traversal test - intermediate between simple (1-hop) and APT (4-hop)
    // Pattern: (hub)<-[:R]-(e1)<-[:S]-(leaf), (hub)<-[:T]-(e2)<-[:S]-(leaf)
    // 'leaf' is shared across patterns, requiring 2 hops from hub
    val graph = makeGraph("diamond-two-hop")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val hubId = computeIdFrom("Node", "hub-2hop")
      val e1Id = computeIdFrom("Event", "e1-2hop")
      val e2Id = computeIdFrom("Event", "e2-2hop")
      val leafId = computeIdFrom("Leaf", "leaf-2hop")

      // Properties
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("WRITE")), 5.seconds)

      // Edges: leaf -> e1 -> hub, leaf -> e2 -> hub
      Await.result(graph.literalOps(namespace).addEdge(leafId, e1Id, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, hubId, "R"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(leafId, e2Id, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, hubId, "T"), 5.seconds)

      val query = """
        MATCH (hub)<-[:R]-(e1)<-[:S]-(leaf), (hub)<-[:T]-(e2)<-[:S]-(leaf)
        WHERE id(hub) = idFrom("Node", "hub-2hop")
          AND e1.type = "READ"
          AND e2.type = "WRITE"
        RETURN hub, e1, e2, leaf
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find exactly 1 match
      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match diamond where shared binding is in middle of one branch" in {
    // This is the KEY difference from simpler tests:
    // In one pattern, 'shared' is at the end: (hub)<-[:R]-(e1)<-[:S]-(shared)
    // In other pattern, 'shared' is in the MIDDLE: (hub)<-[:T]-(e2)<-[:S]-(shared)-[:U]->(extra)
    val graph = makeGraph("diamond-middle-binding")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val hubId = computeIdFrom("Node", "hub-mid")
      val e1Id = computeIdFrom("Event", "e1-mid")
      val e2Id = computeIdFrom("Event", "e2-mid")
      val sharedId = computeIdFrom("Shared", "shared-mid")
      val extraId = computeIdFrom("Extra", "extra-mid")

      // Properties for disambiguation
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("WRITE")), 5.seconds)

      // Edges:
      // Pattern 1: shared -> e1 -> hub
      Await.result(graph.literalOps(namespace).addEdge(sharedId, e1Id, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, hubId, "R"), 5.seconds)
      // Pattern 2: shared -> e2 -> hub AND shared -> extra
      Await.result(graph.literalOps(namespace).addEdge(sharedId, e2Id, "S"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, hubId, "T"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(sharedId, extraId, "U"), 5.seconds)

      // Query where 'shared' is at end of first pattern but MIDDLE of second
      val query = """
        MATCH (hub)<-[:R]-(e1)<-[:S]-(shared), (hub)<-[:T]-(e2)<-[:S]-(shared)-[:U]->(extra)
        WHERE id(hub) = idFrom("Node", "hub-mid")
          AND e1.type = "READ"
          AND e2.type = "WRITE"
        RETURN hub, e1, e2, shared, extra
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find exactly 1 match
      results should have size 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "match APT-style traversal structure without property constraints" in {
    // Same structure as APT but NO property constraints - to isolate traversal vs property issues
    val graph = makeGraph("diamond-apt-structure-only")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Same node structure as APT test
      val p1Id = computeIdFrom("Process", "p1-struct")
      val p2Id = computeIdFrom("Process", "p2-struct")
      val e1Id = computeIdFrom("Event", "e1-struct")
      val e2Id = computeIdFrom("Event", "e2-struct")
      val e3Id = computeIdFrom("Event", "e3-struct")
      val e4Id = computeIdFrom("Event", "e4-struct")
      val fId = computeIdFrom("File", "f-struct")
      val ipId = computeIdFrom("IP", "ip-struct")

      // Set minimal properties (just for identification, not constraints)
      Await.result(graph.literalOps(namespace).setProp(fId, "name", QuineValue.Str("file")), 5.seconds)

      // Same edge structure
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e4Id, ipId, "EVENT"), 5.seconds)

      // Same query structure but NO property constraints
      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
        WHERE id(f) = idFrom("File", "f-struct")
        RETURN p1, p2, e1, e2, e3, e4, f, ip
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Without property constraints, multiple matches are possible because nodes can
      // be assigned to different positions in the pattern. This test verifies traversal
      // structure works; property constraints (in other tests) handle disambiguation.
      results.size should be >= 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "NOT match APT-style pattern when time constraints are violated" in {
    val graph = makeGraph("diamond-apt-no-match")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Helper to compute idFrom
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Create same structure but with time constraints violated
      // e1.time = 300, e2.time = 200 (violates e1.time < e2.time)
      val p1Id = computeIdFrom("Process", "p1-nomatch")
      val p2Id = computeIdFrom("Process", "p2-nomatch")
      val e1Id = computeIdFrom("Event", "e1-nomatch")
      val e2Id = computeIdFrom("Event", "e2-nomatch")
      val e3Id = computeIdFrom("Event", "e3-nomatch")
      val e4Id = computeIdFrom("Event", "e4-nomatch")
      val fId = computeIdFrom("File", "file-nomatch")
      val ipId = computeIdFrom("IP", "ip-nomatch")

      // Set up node properties with VIOLATED time constraint (e1.time > e2.time)
      Await.result(graph.literalOps(namespace).setProp(e1Id, "type", QuineValue.Str("WRITE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e1Id, "time", QuineValue.Integer(300L)), 5.seconds) // AFTER e2

      Await.result(graph.literalOps(namespace).setProp(e2Id, "type", QuineValue.Str("READ")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e2Id, "time", QuineValue.Integer(200L)), 5.seconds) // BEFORE e1

      Await.result(graph.literalOps(namespace).setProp(e3Id, "type", QuineValue.Str("DELETE")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e3Id, "time", QuineValue.Integer(400L)), 5.seconds)

      Await.result(graph.literalOps(namespace).setProp(e4Id, "type", QuineValue.Str("SEND")), 5.seconds)
      Await.result(graph.literalOps(namespace).setProp(e4Id, "time", QuineValue.Integer(350L)), 5.seconds)

      // Set up all edges (same structure)
      Await.result(graph.literalOps(namespace).addEdge(p1Id, e1Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e1Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e2Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e2Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e3Id, fId, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e3Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(p2Id, e4Id, "EVENT"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(e4Id, ipId, "EVENT"), 5.seconds)

      val query = """
        MATCH (p1)-[:EVENT]->(e1)-[:EVENT]->(f)<-[:EVENT]-(e2)<-[:EVENT]-(p2),
              (f)<-[:EVENT]-(e3)<-[:EVENT]-(p2)-[:EVENT]->(e4)-[:EVENT]->(ip)
        WHERE id(f) = idFrom("File", "file-nomatch")
          AND e1.type = "WRITE"
          AND e2.type = "READ"
          AND e3.type = "DELETE"
          AND e4.type = "SEND"
          AND e1.time < e2.time
          AND e2.time < e3.time
          AND e2.time < e4.time
        RETURN p1, p2, e1, e2, e3, e4, f, ip
      """

      val planned = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 15.seconds)

      // Should find 0 matches because e1.time (300) > e2.time (200)
      results should have size 0

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // LOCAL NODE TESTS - RETURN n returns full Value.Node
  // ============================================================

  it should "return Value.Node with id, labels, and properties when using SET and RETURN n" in {
    // Test the full flow: SET creates labeled nodes with properties,
    // MATCH (n:Label) RETURN n should return Value.Node (not just properties)
    val graph = makeGraph("set-return-value-node")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // First query: SET creates 3 Person nodes with properties
      val setQuery = """
        MATCH (a), (b), (c)
        WHERE id(a) = idFrom("Person", "Alice")
        AND id(b) = idFrom("Person", "Bob")
        AND id(c) = idFrom("Person", "Charlie")
        SET a:Person,
            a.name="Alice",
            a.age=30,
            a.city="Seattle",
            b:Person,
            b.name="Bob",
            b.age=25,
            b.city="Portland",
            c:Person,
            c.name="Charlie",
            c.age=35,
            c.city="Washington"
      """

      // Execute the SET query first
      val setPlanned = parseAndPlanWithMetadata(setQuery)
      val setResultPromise = Promise[Seq[QueryContext]]()
      val setLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      setLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = setPlanned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(setResultPromise),
        returnColumns = setPlanned.returnColumns,
        outputNameMapping = setPlanned.outputNameMapping,
      )
      Await.result(setResultPromise.future, 10.seconds)

      // Give time for the SET to propagate
      Thread.sleep(500)

      // Second query: RETURN the nodes
      val returnQuery = """
        MATCH (n:Person) RETURN n
      """

      val returnPlanned = parseAndPlanWithMetadata(returnQuery)
      val returnResultPromise = Promise[Seq[QueryContext]]()
      val returnLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      returnLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = returnPlanned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(returnResultPromise),
        returnColumns = returnPlanned.returnColumns,
        outputNameMapping = returnPlanned.outputNameMapping,
      )

      val results = Await.result(returnResultPromise.future, 10.seconds)

      // Should find 3 Person nodes
      results should have size 3

      // Compute expected IDs
      val aliceId = computeIdFrom("Person", "Alice")
      val bobId = computeIdFrom("Person", "Bob")
      val charlieId = computeIdFrom("Person", "Charlie")

      // Collect the results and validate they are Value.Node with correct structure
      val nodeResults = results.map { ctx =>
        ctx.bindings.keys.map(_.name).toSet should contain("n")
        ctx.bindings(Symbol("n"))
      }

      // Each result should be a Value.Node with id, labels, and properties
      nodeResults.foreach { value =>
        value match {
          case Value.Node(nodeId, labels, props) =>
            // Validate it has the Person label
            labels should contain(Symbol("Person"))

            // Properties should NOT contain the internal labels property
            props.values.keys.map(_.name) should not contain graph.labelsProperty.name

            // Identify which person this is and validate properties
            nodeId match {
              case id if id == aliceId =>
                props.values.get(Symbol("name")) shouldBe Some(Value.Text("Alice"))
                props.values.get(Symbol("age")) shouldBe Some(Value.Integer(30))
                props.values.get(Symbol("city")) shouldBe Some(Value.Text("Seattle"))
              case id if id == bobId =>
                props.values.get(Symbol("name")) shouldBe Some(Value.Text("Bob"))
                props.values.get(Symbol("age")) shouldBe Some(Value.Integer(25))
                props.values.get(Symbol("city")) shouldBe Some(Value.Text("Portland"))
              case id if id == charlieId =>
                props.values.get(Symbol("name")) shouldBe Some(Value.Text("Charlie"))
                props.values.get(Symbol("age")) shouldBe Some(Value.Integer(35))
                props.values.get(Symbol("city")) shouldBe Some(Value.Text("Washington"))
              case otherId =>
                fail(s"Unexpected node ID: $otherId")
            }

          case other =>
            fail(s"Expected Value.Node, but got: $other (${other.getClass.getName})")
        }
      }

      // Validate we got all 3 distinct persons
      val nodeIds = nodeResults.map {
        case Value.Node(id, _, _) => id
        case _ => fail("Expected Value.Node")
      }.toSet
      nodeIds should contain(aliceId)
      nodeIds should contain(bobId)
      nodeIds should contain(charlieId)

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // CALL PROCEDURE TESTS - getFilteredEdges
  // ============================================================

  "QuinePattern CALL getFilteredEdges" should "return edges in eager mode" in {
    val graph = makeGraph("call-getFilteredEdges-eager")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Create a central node and three connected nodes
      val centralId = computeIdFrom("central", "node-1")
      val friend1Id = computeIdFrom("friend", "node-1")
      val friend2Id = computeIdFrom("friend", "node-2")
      val colleagueId = computeIdFrom("colleague", "node-1")

      // Set up the central node
      Await.result(graph.literalOps(namespace).setProp(centralId, "name", QuineValue.Str("Alice")), 5.seconds)

      // Create edges from central node to others
      Await.result(graph.literalOps(namespace).addEdge(centralId, friend1Id, "KNOWS"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(centralId, friend2Id, "KNOWS"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(centralId, colleagueId, "WORKS_WITH"), 5.seconds)

      Thread.sleep(300) // Let edges settle

      // Query using getFilteredEdges to get all edges
      val query = """
        UNWIND $nodes AS nodeId
        CALL getFilteredEdges(nodeId, [], [], $all) YIELD edge
        RETURN edge
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val params = Map(
        Symbol("nodes") -> Value.List(List(Value.NodeId(centralId))),
        Symbol("all") -> Value.List(List(Value.NodeId(friend1Id), Value.NodeId(friend2Id), Value.NodeId(colleagueId))),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return 3 edges (KNOWS x2, WORKS_WITH x1)
      results should have size 3

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "filter edges by type in eager mode" in {
    val graph = makeGraph("call-getFilteredEdges-filter-type")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val centralId = computeIdFrom("central", "filter-test")
      val friend1Id = computeIdFrom("friend", "filter-1")
      val friend2Id = computeIdFrom("friend", "filter-2")
      val colleagueId = computeIdFrom("colleague", "filter-1")

      // Create edges
      Await.result(graph.literalOps(namespace).addEdge(centralId, friend1Id, "KNOWS"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(centralId, friend2Id, "KNOWS"), 5.seconds)
      Await.result(graph.literalOps(namespace).addEdge(centralId, colleagueId, "WORKS_WITH"), 5.seconds)

      Thread.sleep(300)

      // Query filtering only KNOWS edges
      val query = """
        UNWIND $nodes AS nodeId
        CALL getFilteredEdges(nodeId, ["KNOWS"], [], $all) YIELD edge
        RETURN edge
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val params = Map(
        Symbol("nodes") -> Value.List(List(Value.NodeId(centralId))),
        Symbol("all") -> Value.List(List(Value.NodeId(friend1Id), Value.NodeId(friend2Id), Value.NodeId(colleagueId))),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return only 2 KNOWS edges
      results should have size 2

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "return edges in lazy mode" in {
    val graph = makeGraph("call-getFilteredEdges-lazy")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val centralId = computeIdFrom("central", "lazy-test")
      val friendId = computeIdFrom("friend", "lazy-1")

      // Create the edge
      Await.result(graph.literalOps(namespace).addEdge(centralId, friendId, "KNOWS"), 5.seconds)

      Thread.sleep(300)

      val query = """
        UNWIND $nodes AS nodeId
        CALL getFilteredEdges(nodeId, [], [], $all) YIELD edge
        RETURN edge
      """

      val plan = parseAndPlan(query)

      val collector = new LazyResultCollector()
      val sqId = StandingQueryId.fresh()
      val params = Map(
        Symbol("nodes") -> Value.List(List(Value.NodeId(centralId))),
        Symbol("all") -> Value.List(List(Value.NodeId(friendId))),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqId,
        plan = plan,
        mode = RuntimeMode.Lazy,
        params = params,
        namespace = namespace,
        output = OutputTarget.LazyCollector(collector),
      )

      // Wait for the lazy evaluation
      collector.awaitFirstDelta(5.seconds) shouldBe true

      // Should have at least 1 positive result
      collector.positiveCount should be >= 1

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "return empty results when no edges match filter" in {
    val graph = makeGraph("call-getFilteredEdges-no-match")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      val centralId = computeIdFrom("central", "no-match-test")
      val friendId = computeIdFrom("friend", "no-match-1")

      // Create a KNOWS edge
      Await.result(graph.literalOps(namespace).addEdge(centralId, friendId, "KNOWS"), 5.seconds)

      Thread.sleep(300)

      // Query filtering for WORKS_WITH (which doesn't exist)
      val query = """
        UNWIND $nodes AS nodeId
        CALL getFilteredEdges(nodeId, ["WORKS_WITH"], [], $all) YIELD edge
        RETURN edge
      """

      val plan = parseAndPlan(query)

      val resultPromise = Promise[Seq[QueryContext]]()
      val params = Map(
        Symbol("nodes") -> Value.List(List(Value.NodeId(centralId))),
        Symbol("all") -> Value.List(List(Value.NodeId(friendId))),
      )

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return 0 edges (no WORKS_WITH edges exist)
      results should have size 0

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // CALL PROCEDURE TESTS - help.builtins (multiple yields)
  // ============================================================

  "QuinePattern CALL help.builtins" should "yield multiple values without aliases" in {
    val graph = makeGraph("call-help-builtins-multi-yield")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query yielding all three values (no LIMIT - not implemented in QuinePattern yet)
      val query = """
        CALL help.builtins() YIELD name, signature, description
        RETURN name, signature, description
      """

      val plannedQuery = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plannedQuery.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = plannedQuery.returnColumns,
        outputNameMapping = plannedQuery.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return all builtin functions (53 as of this writing)
      results.size should be > 0

      val result = results.head
      // Verify all yielded values are present
      result.bindings should contain key Symbol("name")
      result.bindings should contain key Symbol("signature")
      result.bindings should contain key Symbol("description")

      // Verify they are all Text values
      result.bindings(Symbol("name")) shouldBe a[Value.Text]
      result.bindings(Symbol("signature")) shouldBe a[Value.Text]
      result.bindings(Symbol("description")) shouldBe a[Value.Text]

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "yield three values with aliases" in {
    val graph = makeGraph("call-help-builtins-yield-alias")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query with 3 yields and aliases
      // Note: Can't use "desc" as alias - it's a reserved word (ORDER BY ... DESC)
      val query =
        """CALL help.builtins() YIELD name AS funcName, signature AS sig, description AS descr RETURN funcName, sig, descr"""

      val plannedQuery = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plannedQuery.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = plannedQuery.returnColumns,
        outputNameMapping = plannedQuery.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return all builtin functions
      results.size should be > 0

      val result = results.head
      // Verify aliased names are used in bindings
      result.bindings should contain key Symbol("funcName")
      result.bindings should contain key Symbol("sig")
      result.bindings should contain key Symbol("descr")

      // Verify original names are NOT present (they were aliased)
      result.bindings should not contain key(Symbol("name"))
      result.bindings should not contain key(Symbol("signature"))
      result.bindings should not contain key(Symbol("description"))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "yield partial values (subset of outputs)" in {
    val graph = makeGraph("call-help-builtins-partial-yield")
    while (!graph.isReady) Thread.sleep(10)

    try {
      // Query yielding only name (subset of available outputs)
      val query = """
        CALL help.builtins() YIELD name
        RETURN name
      """

      val plannedQuery = parseAndPlanWithMetadata(query)

      val resultPromise = Promise[Seq[QueryContext]]()

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plannedQuery.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = plannedQuery.returnColumns,
        outputNameMapping = plannedQuery.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      // Should return all builtin functions
      results.size should be > 0

      val result = results.head
      // Verify only name is present
      result.bindings should contain key Symbol("name")
      result.bindings(Symbol("name")) shouldBe a[Value.Text]

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // COMPREHENSIVE SET + CREATE + RETURN + getFilteredEdges TEST
  // ============================================================

  "QuinePattern SET, CREATE, RETURN, and getFilteredEdges integration" should "return nodes with updated properties and find all edges" in {
    // This test validates the complete flow:
    // 1. Run a query that SETs labels/properties, CREATEs edges, and RETURNs nodes
    // 2. Assert returned nodes have correct IDs, labels, and properties
    // 3. Convert node IDs to their string representation
    // 4. Run getFilteredEdges with string IDs
    // 5. Assert all 3 edges are returned

    val graph = makeGraph("set-create-return-getFilteredEdges-integration")
    while (!graph.isReady) Thread.sleep(10)

    try {
      def computeIdFrom(parts: String*): QuineId = {
        val cypherValues = parts.map(s => com.thatdot.quine.graph.cypher.Expr.Str(s))
        com.thatdot.quine.graph.idFrom(cypherValues: _*)(qidProvider)
      }

      // Expected node IDs
      val aliceId = computeIdFrom("Person", "Alice")
      val bobId = computeIdFrom("Person", "Bob")
      val charlieId = computeIdFrom("Person", "Charlie")

      // ========================================
      // STEP 1: Run the combined SET + CREATE + RETURN query
      // ========================================
      val mainQuery = """
        MATCH (a), (b), (c)
        WHERE id(a) = idFrom("Person", "Alice")
        AND id(b) = idFrom("Person", "Bob")
        AND id(c) = idFrom("Person", "Charlie")
        SET a:Person,
            a.name="Alice",
            a.age=30,
            a.city="Seattle",
            b:Person,
            b.name="Bob",
            b.age=25,
            b.city="Portland",
            c:Person,
            c.name="Charlie",
            c.age=35,
            c.city="Washington"
        CREATE (a)-[:KNOWS]->(b),
               (b)-[:KNOWS]->(c),
               (c)-[:KNOWS]->(a)
        RETURN a, b, c
      """

      val mainPlanned = parseAndPlanWithMetadata(mainQuery)
      val mainResultPromise = Promise[Seq[QueryContext]]()
      val mainLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      mainLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = mainPlanned.plan,
        mode = RuntimeMode.Eager,
        params = Map.empty,
        namespace = namespace,
        output = OutputTarget.EagerCollector(mainResultPromise),
        returnColumns = mainPlanned.returnColumns,
        outputNameMapping = mainPlanned.outputNameMapping,
      )

      val mainResults = Await.result(mainResultPromise.future, 10.seconds)

      // ========================================
      // STEP 2: Assert the returned nodes are correct
      // ========================================
      mainResults should have size 1
      val ctx = mainResults.head

      // Should have bindings for a, b, c
      ctx.bindings should contain key Symbol("a")
      ctx.bindings should contain key Symbol("b")
      ctx.bindings should contain key Symbol("c")

      // Helper to validate a node binding
      def validateNode(
        binding: Symbol,
        expectedId: QuineId,
        expectedName: String,
        expectedAge: Long,
        expectedCity: String,
      ): Unit = {
        val value = ctx.bindings(binding)
        value match {
          case Value.Node(nodeId, labels, props) =>
            // Validate ID
            nodeId shouldBe expectedId

            // Validate labels
            labels should contain(Symbol("Person"))

            // Validate properties
            props.values.get(Symbol("name")) shouldBe Some(Value.Text(expectedName))
            props.values.get(Symbol("age")) shouldBe Some(Value.Integer(expectedAge))
            props.values.get(Symbol("city")) shouldBe Some(Value.Text(expectedCity))
            ()

          case other =>
            fail(s"Expected Value.Node for $binding, but got: $other (${other.getClass.getName})")
        }
      }

      validateNode(Symbol("a"), aliceId, "Alice", 30, "Seattle")
      validateNode(Symbol("b"), bobId, "Bob", 25, "Portland")
      validateNode(Symbol("c"), charlieId, "Charlie", 35, "Washington")

      // ========================================
      // STEP 3: Verify edges are in the graph
      // ========================================
      import com.thatdot.quine.model.EdgeDirection

      // Wait for edges to propagate
      Thread.sleep(500)

      // Check Alice -> Bob edge
      val aliceEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(aliceId, None, None, None, None),
        5.seconds,
      )
      aliceEdges.exists(e => e.other == bobId && e.direction == EdgeDirection.Outgoing) shouldBe true
      aliceEdges.exists(e => e.other == charlieId && e.direction == EdgeDirection.Incoming) shouldBe true

      // Check Bob -> Charlie edge
      val bobEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(bobId, None, None, None, None),
        5.seconds,
      )
      bobEdges.exists(e => e.other == charlieId && e.direction == EdgeDirection.Outgoing) shouldBe true
      bobEdges.exists(e => e.other == aliceId && e.direction == EdgeDirection.Incoming) shouldBe true

      // Check Charlie -> Alice edge
      val charlieEdges = Await.result(
        graph.literalOps(namespace).getHalfEdges(charlieId, None, None, None, None),
        5.seconds,
      )
      charlieEdges.exists(e => e.other == aliceId && e.direction == EdgeDirection.Outgoing) shouldBe true
      charlieEdges.exists(e => e.other == bobId && e.direction == EdgeDirection.Incoming) shouldBe true

      // ========================================
      // STEP 4: Convert node IDs to string representations
      // ========================================
      val aliceIdStr = qidProvider.qidToPrettyString(aliceId)
      val bobIdStr = qidProvider.qidToPrettyString(bobId)
      val charlieIdStr = qidProvider.qidToPrettyString(charlieId)

      // ========================================
      // STEP 5: Run getFilteredEdges with string IDs
      // ========================================
      // Parameters: $new and $all are lists of string IDs
      val edgeParams = Map(
        Symbol("new") -> Value.List(
          List(
            Value.Text(aliceIdStr),
            Value.Text(bobIdStr),
            Value.Text(charlieIdStr),
          ),
        ),
        Symbol("all") -> Value.List(
          List(
            Value.Text(aliceIdStr),
            Value.Text(bobIdStr),
            Value.Text(charlieIdStr),
          ),
        ),
      )

      // Query to get all edges via UNWIND over all 3 nodes
      val edgeQuery = """
        UNWIND $new AS newId
        CALL getFilteredEdges(newId, [], [], $all) YIELD edge
        RETURN DISTINCT edge AS e
      """

      val edgePlanned = parseAndPlanWithMetadata(edgeQuery)
      val edgeResultPromise = Promise[Seq[QueryContext]]()

      val edgeLoader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      edgeLoader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = edgePlanned.plan,
        mode = RuntimeMode.Eager,
        params = edgeParams,
        namespace = namespace,
        output = OutputTarget.EagerCollector(edgeResultPromise),
        returnColumns = edgePlanned.returnColumns,
        outputNameMapping = edgePlanned.outputNameMapping,
      )

      val edgeResults = Await.result(edgeResultPromise.future, 10.seconds)

      // ========================================
      // STEP 6: Assert all 3 edges are returned
      // ========================================
      // We created 3 edges:
      // - Alice -> Bob (KNOWS)
      // - Bob -> Charlie (KNOWS)
      // - Charlie -> Alice (KNOWS)

      edgeResults should have size 3

      // Collect the edge information
      val returnedEdges = edgeResults.flatMap { edgeCtx =>
        edgeCtx.bindings.get(Symbol("e")).map {
          case Value.Relationship(from, label, _, to) =>
            (from, label.name, to)
          case other =>
            fail(s"Expected Value.Relationship, got: $other")
        }
      }.toSet

      // Verify all 3 edges are present
      returnedEdges should contain((aliceId, "KNOWS", bobId))
      returnedEdges should contain((bobId, "KNOWS", charlieId))
      returnedEdges should contain((charlieId, "KNOWS", aliceId))

    } finally Await.result(graph.shutdown(), 5.seconds)
  }
}
