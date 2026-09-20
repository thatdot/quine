package com.thatdot.quine.graph

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.DomainNodeIndex.DomainIndexResult
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.{DomainNodeIndex, NodeParentIndex}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainGraphBranch,
  DomainGraphNodePackage,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  PropertyComparisonFunctions,
  SingleBranch,
}

/** [[NodeParentIndex]]: which patterns rooted on a node care about which child pattern, so a peer's answer can be
  * routed to the evaluations that depend on it.
  *
  * The index is derived rather than journaled or snapshotted -- it is rebuilt at every wake by [[reconstruct]] --
  * so what matters is the two claims the rest of the bookkeeping makes about it. The first is stated on the
  * operators: a child is known exactly while some parent lists it, so `parentNodesOf(child).isEmpty` is a sound
  * test for "no pattern here wants this answer", which is what `cancelSubscription` and
  * `dropAnswersNoLiveQueryNeeds` both turn on. The second is stated on `reconstruct`: it may over-approximate,
  * never under-approximate, because a missing link drops an update that mattered while a spurious one only costs
  * a wasted evaluation.
  */
class NodeParentIndexTest extends AnyFunSuite with Matchers {

  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val peer: QuineId = idProvider.customIdToQid(1L)
  private val otherPeer: QuineId = idProvider.customIdToQid(2L)

  private val childX: DomainGraphNodeId = 10L
  private val childY: DomainGraphNodeId = 11L
  private val parentP: DomainGraphNodeId = 20L
  private val parentQ: DomainGraphNodeId = 21L

  // -- the operators --------------------------------------------------------------------------------------------

  test("an empty index knows no children and no parents") {
    val index = NodeParentIndex()
    index.knownChildren shouldBe empty
    index.parentNodesOf(childX) shouldBe empty
  }

  test("adding the same link twice leaves one parent") {
    val index = NodeParentIndex() + ((childX, parentP)) + ((childX, parentP))
    index.parentNodesOf(childX) shouldBe Set(parentP)
  }

  test("a child shared by two patterns lists both") {
    val index = NodeParentIndex() + ((childX, parentP)) + ((childX, parentQ))
    index.parentNodesOf(childX) shouldBe Set(parentP, parentQ)
    index.knownChildren.toSet shouldBe Set(childX)
  }

  test("removing one parent of a shared child leaves the child known to the other") {
    // The case `cancelSubscription` turns on: a child is content-addressed and shared, so one pattern's
    // teardown must not take the answers the other still depends on.
    val index = NodeParentIndex() + ((childX, parentP)) + ((childX, parentQ)) - ((childX, parentP))
    index.parentNodesOf(childX) shouldBe Set(parentQ)
    withClue("and the child is still known, so its answers are still wanted: ")(
      index.parentNodesOf(childX) should not be empty,
    )
  }

  test("removing the last parent of a child forgets the child entirely") {
    val index = NodeParentIndex() + ((childX, parentP)) - ((childX, parentP))
    index.parentNodesOf(childX) shouldBe empty
    withClue("no empty parent set is left behind, or the child would read as known: ")(
      index.knownChildren shouldBe empty,
    )
  }

  test("removing a link that is not there changes nothing") {
    val index = NodeParentIndex() + ((childX, parentP))
    (index - ((childX, parentQ))).knownParents shouldBe index.knownParents
    (index - ((childY, parentP))).knownParents shouldBe index.knownParents
  }

  test("forgetting a child drops every parent of it at once") {
    val index = NodeParentIndex() + ((childX, parentP)) + ((childX, parentQ)) + ((childY, parentP))
    val after = index -- childX
    after.parentNodesOf(childX) shouldBe empty
    withClue("the other child is untouched: ")(after.parentNodesOf(childY) shouldBe Set(parentP))
  }

  test("every operator leaves the index immutable for its caller") {
    val index = NodeParentIndex() + ((childX, parentP))
    val _ = index + ((childY, parentQ))
    val _ = index - ((childX, parentP))
    val _ = index -- childX
    index.knownParents shouldBe Map(childX -> Set(parentP))
  }

  // -- reconstruct ----------------------------------------------------------------------------------------------

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private def hop(prop: String, child: DomainGraphBranch): SingleBranch = SingleBranch(
    hasProperty(prop),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, child)),
  )

  /** `kind -> region` and `other -> region`: two roots over one content-addressed child, which is the shape
    * every sharing case in the DistinctId bookkeeping is built from.
    */
  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val kindToRegion: SingleBranch = hop("kind", regionBranch)
  private val otherToRegion: SingleBranch = hop("other", regionBranch)

  private class Registry {
    val registry: DomainGraphNodeRegistry = new DomainGraphNodeRegistry(
      registerGaugeDomainGraphNodeCount = _ => (),
      persistDomainGraphNodes = _ => Future.unit,
      removeDomainGraphNodes = _ => Future.unit,
    )
    def register(branch: DomainGraphBranch): DomainGraphNodeId = {
      val pkg = branch.toDomainGraphNodePackage
      Await.result(
        registry.registerAndPersistDomainGraphNodePackage(pkg, StandingQueryId.fresh(), skipPersistor = true),
        5.seconds,
      )
      pkg.dgnId
    }
    def unregister(dgnId: DomainGraphNodeId, sqId: StandingQueryId): Unit = {
      val pkg = DomainGraphNodePackage(dgnId, registry.getDomainGraphNode(_))
      Await.result(registry.unregisterDomainGraphNodePackage(pkg, sqId), 5.seconds)
    }
  }

  private def indexHolding(entries: (QuineId, DomainGraphNodeId)*): DomainNodeIndex = {
    val index = new DomainNodeIndex()
    entries.foreach { case (qid, child) =>
      index.index.getOrElseUpdate(qid, mutable.Map.empty) += (child -> DomainIndexResult(Some(true), Set.empty))
    }
    index
  }

  test("reconstruct links each child this node holds an answer for to every pattern here that has it") {
    val reg = new Registry
    val kindRoot = reg.register(kindToRegion)
    val otherRoot = reg.register(otherToRegion)
    val regionChild = regionBranch.toDomainGraphNodePackage.dgnId

    val (rebuilt, missing) = NodeParentIndex.reconstruct(
      indexHolding(peer -> regionChild),
      nodesRootedHere = List(kindRoot, otherRoot),
      reg.registry,
    )
    withClue("both patterns have the child, so both are recorded as parents: ")(
      rebuilt.parentNodesOf(regionChild) shouldBe Set(kindRoot, otherRoot),
    )
    missing shouldBe empty
  }

  test("reconstruct records nothing for a child no answer is held for") {
    val reg = new Registry
    val kindRoot = reg.register(kindToRegion)
    val regionChild = regionBranch.toDomainGraphNodePackage.dgnId

    val (rebuilt, missing) = NodeParentIndex.reconstruct(
      new DomainNodeIndex(),
      nodesRootedHere = List(kindRoot),
      reg.registry,
    )
    withClue("the index is what says which children this node is waiting on: ")(
      rebuilt.parentNodesOf(regionChild) shouldBe empty,
    )
    withClue("the pattern itself is registered, so it is not reported missing: ")(missing shouldBe empty)
  }

  test("reconstruct reports a subscribed pattern the registry no longer defines") {
    val reg = new Registry
    val sqId = StandingQueryId.fresh()
    val pkg = kindToRegion.toDomainGraphNodePackage
    Await.result(reg.registry.registerAndPersistDomainGraphNodePackage(pkg, sqId, skipPersistor = true), 5.seconds)
    val regionChild = regionBranch.toDomainGraphNodePackage.dgnId
    reg.unregister(pkg.dgnId, sqId)

    val (rebuilt, missing) = NodeParentIndex.reconstruct(
      indexHolding(peer -> regionChild),
      nodesRootedHere = List(pkg.dgnId),
      reg.registry,
    )
    withClue("a pattern that cannot be resolved contributes no links: ")(
      rebuilt.parentNodesOf(regionChild) shouldBe empty,
    )
    withClue("and is named, so the caller can see what it could not rebuild: ")(
      missing.toList shouldBe List(pkg.dgnId),
    )
  }

  test("reconstruct is at least as complete as a thoroughgoing index, which is the invariant it promises") {
    // The example from the doc comment on `reconstruct`, made concrete. Both patterns are rooted here and both
    // have `region` as a child, but only one of them ever asked -- the other was answered from what was already
    // held. A thoroughgoing index may therefore record one parent; the rebuilt one records both. What must never
    // happen is the other way round, because a missing link is an answer that reaches no evaluation.
    val reg = new Registry
    val kindRoot = reg.register(kindToRegion)
    val otherRoot = reg.register(otherToRegion)
    val regionChild = regionBranch.toDomainGraphNodePackage.dgnId

    val thoroughgoing = NodeParentIndex() + ((regionChild, kindRoot))
    val (rebuilt, _) = NodeParentIndex.reconstruct(
      indexHolding(peer -> regionChild),
      nodesRootedHere = List(kindRoot, otherRoot),
      reg.registry,
    )
    thoroughgoing.knownChildren.foreach { child =>
      withClue(s"child $child lost a parent in the rebuild: ")(
        rebuilt.parentNodesOf(child) should contain allElementsOf thoroughgoing.parentNodesOf(child),
      )
    }
    withClue("and this rebuild is strictly more complete, which is allowed: ")(
      rebuilt.parentNodesOf(regionChild) shouldBe Set(kindRoot, otherRoot),
    )
  }

  test("reconstruct does not care which peer an answer came from, only which pattern it is about") {
    val reg = new Registry
    val kindRoot = reg.register(kindToRegion)
    val regionChild = regionBranch.toDomainGraphNodePackage.dgnId

    val (rebuilt, _) = NodeParentIndex.reconstruct(
      indexHolding(peer -> regionChild, otherPeer -> regionChild),
      nodesRootedHere = List(kindRoot),
      reg.registry,
    )
    rebuilt.parentNodesOf(regionChild) shouldBe Set(kindRoot)
    rebuilt.knownChildren.toSet shouldBe Set(regionChild)
  }
}
