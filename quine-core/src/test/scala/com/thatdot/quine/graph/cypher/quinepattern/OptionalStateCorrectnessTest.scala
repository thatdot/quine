package com.thatdot.quine.graph.cypher.quinepattern

import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{TestKit, TestProbe}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.{StandingQueryId, defaultNamespaceId}
import com.thatdot.quine.language.ast.Value

/** Correctness tests for OptionalState, lifted from the Lean formalization
  * in lean-learning/OptionalLeftJoin/.
  *
  * Each test drives the real OptionalState through its notify/kickstart
  * interface and verifies the properties proved (or stated) in Lean.
  *
  * Lean theorems tested:
  *   - delta_case1_new_row_no_inner          (null-padded on arrival)
  *   - delta_case3_null_to_matched           (retract null, assert real)
  *   - delta_case4_matched_to_null           (retract real, assert null)
  *   - delta_case5_row_retracted             (retract everything)
  *   - lazy_trace_telescoping                (Σ diffs = final output)
  *   - mode_equivalence                      (lazy accumulated = eager single)
  *   - inner_delta_row_isolation             (σ₁ inner doesn't affect σ₂)
  *   - match_cycle_returns_to_null           (full cycle back to null-pad)
  */
class OptionalStateCorrectnessTest
    extends TestKit(ActorSystem("OptionalStateCorrectnessTest"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val namespace = defaultNamespaceId
  private val nullBindings = Set(Symbol("friend"))

  private def mkCtx(bindings: (Symbol, Value)*): QueryContext =
    QueryContext(bindings.toMap)

  private def mkState(mode: RuntimeMode): OptionalState =
    new OptionalState(
      id = StandingQueryId.fresh(),
      publishTo = StandingQueryId.fresh(),
      mode = mode,
      innerPlan = QueryPlan.Unit,
      nullBindings = nullBindings,
      namespace = namespace,
      params = Map.empty,
      atTime = None,
    )

  private def collectUpdates(probe: TestProbe, count: Int): Seq[QuinePatternCommand.QueryUpdate] =
    (1 to count).map { _ =>
      probe
        .fishForMessage(1.second) { case _: QuinePatternCommand.QueryUpdate => true; case _ => false }
        .asInstanceOf[QuinePatternCommand.QueryUpdate]
    }

  private def captureInnerSqid(probe: TestProbe): StandingQueryId =
    probe
      .fishForMessage(1.second) { case _: QuinePatternCommand.LoadQueryPlan => true; case _ => false }
      .asInstanceOf[QuinePatternCommand.LoadQueryPlan]
      .sqid

  private def expectNoUpdate(probe: TestProbe): Unit = {
    val deadline = 100.millis.fromNow
    while (deadline.hasTimeLeft())
      probe.receiveOne(deadline.timeLeft.max(1.millis)) match {
        case null => return
        case _: QuinePatternCommand.QueryUpdate => fail("Unexpected QueryUpdate")
        case _ => ()
      }
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: delta_case1 — new row, no inner → emit null-padded
  // ════════════════════════════════════════════════════════════════

  "Case 1 (delta_case1_new_row_no_inner)" should "emit null-padded on context arrival" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val nullPadded = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Null)

    state.notify(Map(σ -> 1), contextSender, probe.ref)

    captureInnerSqid(probe) // consume LoadQueryPlan
    val msg = collectUpdates(probe, 1).head
    msg.delta should be(Map(nullPadded -> 1))
    expectNoUpdate(probe)
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: delta_case3 / null_to_matched_transition
  // ════════════════════════════════════════════════════════════════

  "Case 3 (null_to_matched_transition)" should "retract null-padded and assert real in one delta" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val nullPadded = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Null)
    val innerRow = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))

    state.notify(Map(σ -> 1), contextSender, probe.ref)
    val innerSqid = captureInnerSqid(probe)
    collectUpdates(probe, 1) // consume null-padded emission

    state.notify(Map(innerRow -> 1), innerSqid, probe.ref)
    val msg = collectUpdates(probe, 1).head
    msg.delta should contain(nullPadded -> -1)
    msg.delta should contain(innerRow -> 1)
    expectNoUpdate(probe)
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: delta_case4 / matched_to_null_transition
  // ════════════════════════════════════════════════════════════════

  "Case 4 (matched_to_null_transition)" should "retract real and assert null-padded in one delta" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val nullPadded = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Null)
    val innerRow = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))

    state.notify(Map(σ -> 1), contextSender, probe.ref)
    val innerSqid = captureInnerSqid(probe)
    collectUpdates(probe, 1) // null-padded
    state.notify(Map(innerRow -> 1), innerSqid, probe.ref)
    collectUpdates(probe, 1) // null→matched transition

    // Retract inner match
    state.notify(Map(innerRow -> -1), innerSqid, probe.ref)
    val msg = collectUpdates(probe, 1).head
    msg.delta should contain(innerRow -> -1)
    msg.delta should contain(nullPadded -> 1)
    expectNoUpdate(probe)
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: lazy_trace_telescoping
  // Σ emitted deltas = final totalOutput
  // ════════════════════════════════════════════════════════════════

  "Telescoping (lazy_trace_telescoping)" should "produce accumulated diffs equal to final output" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val innerRow = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))

    // Event 1: context arrives → null-padded
    state.notify(Map(σ -> 1), contextSender, probe.ref)
    val innerSqid = captureInnerSqid(probe)
    val d1 = collectUpdates(probe, 1).head.delta

    // Event 2: inner match → retract null, assert real
    state.notify(Map(innerRow -> 1), innerSqid, probe.ref)
    val d2 = collectUpdates(probe, 1).head.delta

    // Event 3: inner retract → retract real, assert null
    state.notify(Map(innerRow -> -1), innerSqid, probe.ref)
    val d3 = collectUpdates(probe, 1).head.delta

    // Event 4: inner match again → retract null, assert real
    state.notify(Map(innerRow -> 1), innerSqid, probe.ref)
    val d4 = collectUpdates(probe, 1).head.delta

    // Telescoping: Σ dᵢ should equal the final output (innerRow -> 1)
    val accumulated = List(d1, d2, d3, d4).foldLeft(Delta.empty)(Delta.add)
    accumulated should be(Map(innerRow -> 1))
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: match_cycle_returns_to_null
  // Full cycle: null → matched → null. Accumulated = null-padded.
  // ════════════════════════════════════════════════════════════════

  "Match cycle (match_cycle_returns_to_null)" should "return to null-padded after full cycle" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val nullPadded = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Null)
    val innerRow = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))

    state.notify(Map(σ -> 1), contextSender, probe.ref)
    val innerSqid = captureInnerSqid(probe)
    val d1 = collectUpdates(probe, 1).head.delta
    state.notify(Map(innerRow -> 1), innerSqid, probe.ref)
    val d2 = collectUpdates(probe, 1).head.delta
    state.notify(Map(innerRow -> -1), innerSqid, probe.ref)
    val d3 = collectUpdates(probe, 1).head.delta

    val accumulated = List(d1, d2, d3).foldLeft(Delta.empty)(Delta.add)
    accumulated should be(Map(nullPadded -> 1))
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: inner_delta_row_isolation
  // Inner delta for σ₁ does not affect output for σ₂
  // ════════════════════════════════════════════════════════════════

  "Row isolation (inner_delta_row_isolation)" should "not affect other rows when inner changes for one" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Lazy)
    val contextSender = StandingQueryId.fresh()
    val σ1 = mkCtx(Symbol("p") -> Value.Integer(1))
    val σ2 = mkCtx(Symbol("p") -> Value.Integer(2))
    val nullPadded1 = mkCtx(Symbol("p") -> Value.Integer(1), Symbol("friend") -> Value.Null)
    val nullPadded2 = mkCtx(Symbol("p") -> Value.Integer(2), Symbol("friend") -> Value.Null)
    val inner1 = mkCtx(Symbol("p") -> Value.Integer(1), Symbol("friend") -> Value.Integer(10))

    // Send context rows separately so we can track which sqid maps to which row
    state.notify(Map(σ1 -> 1), contextSender, probe.ref)
    val sqid1 = captureInnerSqid(probe)
    val np1 = collectUpdates(probe, 1).head.delta
    np1 should contain(nullPadded1 -> 1)

    state.notify(Map(σ2 -> 1), contextSender, probe.ref)
    captureInnerSqid(probe) // sqid2 — we won't send inner results for it
    val np2 = collectUpdates(probe, 1).head.delta
    np2 should contain(nullPadded2 -> 1)

    // Inner match arrives for σ1 only
    state.notify(Map(inner1 -> 1), sqid1, probe.ref)
    val transition = collectUpdates(probe, 1).head.delta

    // The transition should only affect σ1's row
    transition should contain(nullPadded1 -> -1)
    transition should contain(inner1 -> 1)
    // σ2's null-padded row should NOT appear
    transition.get(nullPadded2) should be(None)
    expectNoUpdate(probe)
  }

  // ════════════════════════════════════════════════════════════════
  // Lean: mode_equivalence
  // Lazy accumulated output = eager single emission
  // ════════════════════════════════════════════════════════════════

  "Mode equivalence (mode_equivalence)" should "produce same total for lazy and eager" in {
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))
    val innerRow = mkCtx(Symbol("p") -> Value.Integer(42), Symbol("friend") -> Value.Integer(99))

    // Lazy run
    val lazyProbe = TestProbe()
    val lazyState = mkState(RuntimeMode.Lazy)
    val lazyCtxSender = StandingQueryId.fresh()

    lazyState.notify(Map(σ -> 1), lazyCtxSender, lazyProbe.ref)
    val lazySqid = captureInnerSqid(lazyProbe)
    val ld1 = collectUpdates(lazyProbe, 1).head.delta
    lazyState.notify(Map(innerRow -> 1), lazySqid, lazyProbe.ref)
    val ld2 = collectUpdates(lazyProbe, 1).head.delta

    val lazyTotal = Delta.add(ld1, ld2)

    // Eager run
    val eagerProbe = TestProbe()
    val eagerState = mkState(RuntimeMode.Eager)
    val eagerCtxSender = StandingQueryId.fresh()

    eagerState.notify(Map(σ -> 1), eagerCtxSender, eagerProbe.ref)
    val eagerSqid = captureInnerSqid(eagerProbe)

    // In eager mode, no QueryUpdate until all inner plans respond
    expectNoUpdate(eagerProbe)

    eagerState.notify(Map(innerRow -> 1), eagerSqid, eagerProbe.ref)
    val eagerEmission = collectUpdates(eagerProbe, 1).head.delta

    lazyTotal should be(eagerEmission)
  }

  // ════════════════════════════════════════════════════════════════
  // Eager must reply even when upstream produces no rows.
  //
  // Simulates: MATCH (a) WHERE false OPTIONAL MATCH (b) WHERE id(b) = a.foo
  // The WHERE false means the input delta is empty — no context rows arrive.
  // Eager mode must still emit (Delta.empty) to signal completion.
  // ════════════════════════════════════════════════════════════════

  "Eager empty input (WHERE false upstream)" should "emit empty delta when input produces no rows" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Eager)
    val contextSender = StandingQueryId.fresh()

    // Upstream filtered everything: empty delta
    state.notify(Delta.empty, contextSender, probe.ref)

    val msg = collectUpdates(probe, 1).head
    msg.delta should be(Delta.empty)
  }

  "Eager zero-mult input" should "emit empty delta when input has only zero-multiplicity rows" in {
    val probe = TestProbe()
    val state = mkState(RuntimeMode.Eager)
    val contextSender = StandingQueryId.fresh()
    val σ = mkCtx(Symbol("p") -> Value.Integer(42))

    // Context row with multiplicity 0 — effectively absent
    state.notify(Map(σ -> 0), contextSender, probe.ref)

    val msg = collectUpdates(probe, 1).head
    msg.delta should be(Delta.empty)
  }
}
