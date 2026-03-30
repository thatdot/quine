package com.thatdot.quine.graph.cypher.quinepattern

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Promise}

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.OutputTarget.LazyResultCollector
import com.thatdot.quine.graph.quinepattern.NonNodeActor
import com.thatdot.quine.graph.{GraphService, NamespaceId, QuineIdLongProvider, StandingQueryId, defaultNamespaceId}
import com.thatdot.quine.language.ast.Value
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

/** Regression tests for bugs identified in PR #3981 code review.
  *
  * Each test guards against a specific class of bug re-emerging:
  *   Bug 1 - UnionState eager mode must not re-emit after both sides have reported.
  *   Bug 2 - OptionalState lazy mode must handle repeated match/no-match cycles,
  *            emitting atomic combined deltas at each transition.
  *   Bug 3 - Mixed UNION / UNION ALL must use left-associative evaluation so that
  *            DISTINCT applies to the correct sub-trees.
  *   Bug 5 - OptionalState lazy mode must retract real results atomically (in a single
  *            combined delta) when transitioning from matches back to no matches.
  */
class PR3981BugRegressionTest
    extends TestKit(ActorSystem("PR3981BugRegressionTest"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)
  val namespace: NamespaceId = defaultNamespaceId
  val qidProvider: QuineIdLongProvider = QuineIdLongProvider()

  private def makeCtx(bindings: (Symbol, Value)*): QueryContext =
    QueryContext(bindings.toMap)

  private def singletonDelta(bindings: (Symbol, Value)*): Delta.T =
    Map(makeCtx(bindings: _*) -> 1)

  /** Collect only QueryUpdate messages from the probe, discarding housekeeping messages
    * (e.g., UnregisterState) that the actor system sends as side effects.
    */
  private def expectQueryUpdates(
    probe: TestProbe,
    count: Int,
    max: FiniteDuration,
  ): Seq[QuinePatternCommand.QueryUpdate] =
    (1 to count).map { _ =>
      probe
        .fishForMessage(max) {
          case _: QuinePatternCommand.QueryUpdate => true
          case _ => false
        }
        .asInstanceOf[QuinePatternCommand.QueryUpdate]
    }

  private def expectNoQueryUpdate(probe: TestProbe, within: FiniteDuration): Unit = {
    val deadline = within.fromNow
    while (deadline.hasTimeLeft())
      probe.receiveOne(deadline.timeLeft.max(1.millis)) match {
        case null => return // timeout — no message
        case _: QuinePatternCommand.QueryUpdate =>
          fail("Received unexpected QueryUpdate")
        case _ => () // ignore housekeeping messages
      }
  }

  def makeGraph(name: String): GraphService = Await.result(
    GraphService(
      name,
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = qidProvider,
    )(LogConfig.permissive),
    5.seconds,
  )

  // ============================================================
  // Bug 1: UnionState eager mode relies on PublishingState.emit's `hasEmitted`
  // guard to prevent re-emission after both sides have reported.  This test
  // verifies that guard holds when LHS sends a second notification.
  // ============================================================

  "UnionState (Bug 1 — hasEmitted guard)" should "not re-emit when LHS notifies a second time after both sides have reported" in {
    val probe = TestProbe()
    val parentId = StandingQueryId.fresh()
    val lhsId = StandingQueryId.fresh()
    val rhsId = StandingQueryId.fresh()
    val stateId = StandingQueryId.fresh()

    val state = new UnionState(stateId, parentId, RuntimeMode.Eager, lhsId, rhsId)

    val lhsRow1 = singletonDelta(Symbol("x") -> Value.Integer(1))
    val rhsRow1 = singletonDelta(Symbol("x") -> Value.Integer(2))

    // LHS notifies first — RHS not yet reported, no emit
    state.notify(lhsRow1, lhsId, probe.ref)
    expectNoQueryUpdate(probe, 100.millis)

    // RHS notifies — both reported; expect exactly one combined emit
    state.notify(rhsRow1, rhsId, probe.ref)
    val firstEmit = expectQueryUpdates(probe, 1, 1.second).head
    firstEmit.delta should have size 2
    expectNoQueryUpdate(probe, 100.millis)

    // LHS sends a second notification — hasEmitted guard should block a second emit
    val lhsRow2 = singletonDelta(Symbol("x") -> Value.Integer(3))
    state.notify(lhsRow2, lhsId, probe.ref)

    // Drain all messages for 300ms; no QueryUpdate should arrive
    val allAfterStep3 = probe.receiveWhile(300.millis, 100.millis, 20) { case any => any }
    val extraUpdates = allAfterStep3.collect { case msg: QuinePatternCommand.QueryUpdate => msg }
    extraUpdates shouldBe empty
  }

  // ============================================================
  // Bug 2: OptionalState lazy mode must correctly cycle through the full
  // lifecycle: no matches → has matches → no matches → has matches again.
  // Each transition emits a single atomic delta combining the retraction and
  // assertion so downstream states never see an inconsistent intermediate.
  // ============================================================

  "OptionalState (Bug 2)" should "handle repeated edge add/remove cycles in a lazy OPTIONAL MATCH" in {
    val graph = makeGraph("optional-lazy-cycle-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()
      val friendId = qidProvider.newQid()
      val collector = new LazyResultCollector()

      // Ensure the anchor node exists so the initial MATCH fires
      Await.result(
        graph.literalOps(namespace).setProp(nodeId, "exists", QuineValue.True),
        5.seconds,
      )

      // OPTIONAL MATCH introduces a new binding `friend` — when no edge exists,
      // it is null-padded; when an edge exists, the real friend ID is emitted.
      val query =
        s"""
          MATCH (n) WHERE id(n) = $$nodeId
          OPTIONAL MATCH (n)-[:KNOWS]->(friend)
          RETURN id(n) AS nId, id(friend) AS friendId
        """
      val planned = QueryPlanner.planFromString(query) match {
        case Right(p) => p
        case Left(err) => fail(s"Failed to plan: $err")
      }

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Lazy,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.LazyCollector(collector),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      // Initially no KNOWS edge → null-padded default emitted (friendId = null)
      collector.awaitFirstDelta(5.seconds) shouldBe true
      collector.netResult.values.sum shouldBe 1
      collector.clear()

      // Add edge → transition: retract null-padded, assert real result
      Await.result(
        graph.literalOps(namespace).addEdge(nodeId, friendId, "KNOWS"),
        5.seconds,
      )
      Thread.sleep(500)
      collector.netResult.values.sum shouldBe 0 // one retraction + one assertion
      collector.hasRetractions shouldBe true
      collector.clear()

      // Remove edge → transition: retract real result, re-assert null-padded
      Await.result(
        graph.literalOps(namespace).removeEdge(nodeId, friendId, "KNOWS"),
        5.seconds,
      )
      Thread.sleep(500)
      collector.netResult.values.sum shouldBe 0
      collector.hasRetractions shouldBe true
      collector.clear()

      // Add edge again → verifies the full cycle works a second time
      Await.result(
        graph.literalOps(namespace).addEdge(nodeId, friendId, "KNOWS"),
        5.seconds,
      )
      Thread.sleep(500)
      collector.netResult.values.sum shouldBe 0
      collector.hasRetractions shouldBe true

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // Bug 3: Mixed UNION / UNION ALL sequences are right-associative in the
  //        planner, which applies the Distinct wrapper to the wrong sub-tree.
  //
  // Root cause: RegularQueryVisitor.visitOC_RegularQuery builds a right-
  // recursive tree.  For A UNION B UNION ALL C the tree is:
  //   Union(all=false, A, Union(all=true, B, C))
  // The planner wraps the outer (false) union with Distinct, giving:
  //   Distinct(Union(A, Union(B, C)))         ← deduplicates everything together
  //
  // Correct left-associative semantics require:
  //   Union(all=true, Union(all=false, A, B), C)
  // which the planner would render as:
  //   Union(Distinct(Union(A, B)), C)          ← only deduplicates A ∪ B, then appends C
  //
  // Observable difference: with A = B = C = {row}, left-associative gives 2 rows,
  // right-associative (bug) gives 1 row.
  // ============================================================

  "UNION execution (Bug 3)" should "produce 2 rows for A UNION B UNION ALL C when all three return the same row" in {
    val graph = makeGraph("union-associativity-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // All three sub-queries return the same row ({val: 1}).
      // Left-associative evaluation (correct):
      //   (A UNION B) = deduplicate({1}, {1}) = {1}   (1 row)
      //   result UNION ALL C = {1} ++ {1}              (2 rows)
      val query =
        s"""
          MATCH (n) WHERE id(n) = $$nodeId RETURN 1 AS val
          UNION
          MATCH (n) WHERE id(n) = $$nodeId RETURN 1 AS val
          UNION ALL
          MATCH (n) WHERE id(n) = $$nodeId RETURN 1 AS val
        """

      val planned = QueryPlanner.planFromString(query) match {
        case Right(p) => p
        case Left(err) => fail(s"Failed to plan: $err")
      }

      val resultPromise = Promise[Seq[QueryContext]]()
      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = Map(Symbol("nodeId") -> Value.NodeId(nodeId)),
        namespace = namespace,
        output = OutputTarget.EagerCollector(resultPromise),
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 2
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  // ============================================================
  // Bug 5: When OptionalState transitions from >0 inner matches back to 0,
  // it must both retract the real results and re-emit the null-padded default
  // (the row where inner-only bindings are Null, preserving the LEFT JOIN
  // invariant).  These are combined into a single atomic delta so downstream
  // states never see a transient intermediate state.
  // ============================================================

  "OptionalState (Bug 5)" should "retract real results and re-emit null-padded default atomically on matches→no-matches transition" in {
    val graph = makeGraph("PR3981BugRegressionTest-Bug5")
    try {
      val probe = TestProbe()
      val parentId = StandingQueryId.fresh()
      val contextSenderId = StandingQueryId.fresh()
      val stateId = StandingQueryId.fresh()

      val nullBindings = Set(Symbol("friend"))
      // Inner plan is a simple Unit - in this unit test we simulate its results manually
      val state = new OptionalState(
        id = stateId,
        publishTo = parentId,
        mode = RuntimeMode.Lazy,
        innerPlan = QueryPlan.Unit,
        nullBindings = nullBindings,
        namespace = namespace,
        params = Map.empty,
        atTime = None,
      )

      val contextRow = makeCtx(Symbol("p") -> Value.Integer(42))
      val contextDelta: Delta.T = Map(contextRow -> 1)

      val innerRow = makeCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))
      val nullPaddedRow = makeCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Null)
      val innerAdd: Delta.T = Map(innerRow -> 1)
      val innerRetract: Delta.T = Map(innerRow -> -1)

      // Step 1: context arrives → LoadQueryPlan is sent (not QueryUpdate) + null-padded QueryUpdate to parent
      state.notify(contextDelta, contextSenderId, probe.ref)

      // Capture the LoadQueryPlan to get the inner sqid
      val loadPlanMsg = probe
        .fishForMessage(1.second) {
          case _: QuinePatternCommand.LoadQueryPlan => true
          case _ => false
        }
        .asInstanceOf[QuinePatternCommand.LoadQueryPlan]
      val innerSqid = loadPlanMsg.sqid

      // Also expect the null-padded default emission (QueryUpdate to parent)
      val nullPaddedMsg = expectQueryUpdates(probe, 1, 1.second).head
      nullPaddedMsg.delta should contain(nullPaddedRow -> 1)
      expectNoQueryUpdate(probe, 100.millis)

      // Step 2: inner match arrives (0 → 1) via the inner sqid → single atomic delta: retract null-padded + emit real result
      state.notify(innerAdd, innerSqid, probe.ref)
      val addMsg = expectQueryUpdates(probe, 1, 1.second).head
      // The delta should retract the null-padded default and emit the real result
      addMsg.delta should contain(nullPaddedRow -> -1)
      addMsg.delta should contain(innerRow -> 1)
      expectNoQueryUpdate(probe, 100.millis)

      // Step 3: inner retracts (1 → 0) — the combined delta must contain both
      // the retraction of innerRow and the assertion of the null-padded default
      state.notify(innerRetract, innerSqid, probe.ref)

      val transitionMsg = expectQueryUpdates(probe, 1, 1.second).head
      expectNoQueryUpdate(probe, 100.millis)

      // The single delta must contain the retraction of the real result
      transitionMsg.delta should contain(innerRow -> -1)

      // The single delta must contain the null-padded default assertion
      transitionMsg.delta should contain(nullPaddedRow -> 1)
    } finally Await.result(graph.shutdown(), 5.seconds)
  }

}
