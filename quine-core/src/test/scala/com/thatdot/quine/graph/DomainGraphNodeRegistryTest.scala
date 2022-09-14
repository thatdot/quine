package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.Generators.generate1
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphBranch, DomainGraphNode, DomainGraphNodePackage, Not}

class DomainGraphNodeRegistryTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with BeforeAndAfter
    with ArbitraryInstances {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 1000)

  var dgnReg: DomainGraphNodeRegistry = _

  var persistDomainGraphNodesCalls: Set[DomainGraphNodeId] = _
  var deleteDomainGraphNodesCalls: Set[DomainGraphNodeId] = _

  before {
    persistDomainGraphNodesCalls = Set.empty
    deleteDomainGraphNodesCalls = Set.empty
    def persistDomainGraphNodes(z: Map[DomainGraphNodeId, DomainGraphNode]): Future[Unit] = {
      persistDomainGraphNodesCalls ++= z.keys
      Future.unit
    }
    def removeDomainGraphNodes(z: Set[DomainGraphNodeId]): Future[Unit] = {
      deleteDomainGraphNodesCalls ++= z
      Future.unit
    }
    dgnReg = new DomainGraphNodeRegistry(
      registerGaugeDomainGraphNodeCount = _ => (),
      persistDomainGraphNodes,
      removeDomainGraphNodes
    )
  }

  private def registerDomainGraphBranch(dgb: DomainGraphBranch, sqId: StandingQueryId): DomainGraphNodeId = {
    val dgnPackage = dgb.toDomainGraphNodePackage
    Await.result(dgnReg.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId), 1 second)
    dgnPackage.dgnId
  }

  private def unregisterDomainGraphNode(dgnId: DomainGraphNodeId, sqId: StandingQueryId): Unit = {
    val dgnPackage = DomainGraphNodePackage(dgnId, dgnReg.getDomainGraphNode(_))
    Await.result(dgnReg.unregisterDomainGraphNodePackage(dgnPackage, sqId), 1 second)
  }

  private def getDomainGraphBranch(dgnId: DomainGraphNodeId): DomainGraphBranch =
    dgnReg.getDomainGraphBranch(dgnId).get

  test("getDomainGraphNode ~ id does not exist") {
    assert(dgnReg.getDomainGraphNode(1L).isEmpty)
  }

  test("getDomainGraphBranch ~ id does not exist") {
    assert(dgnReg.getDomainGraphBranch(1L).isEmpty)
  }

  test("getDomainGraphNode ~ arbitrary data") {
    forAll { dgb: DomainGraphBranch =>
      val sqId = StandingQueryId.fresh()
      val dgnId = registerDomainGraphBranch(dgb, sqId)
      assert(dgnReg.getDomainGraphNode(dgnId).nonEmpty)
    }
  }

  test("getDomainGraphBranch ~ arbitrary data") {
    forAll { dgb: DomainGraphBranch =>
      val sqId = StandingQueryId.fresh()
      val dgnId = registerDomainGraphBranch(dgb, sqId)
      assert(dgb === getDomainGraphBranch(dgnId))
    }
  }

  test("standing query references ~ variation 1") {
    val dgn = generate1[DomainGraphNode](100)
    val id = DomainGraphNode.id(dgn)
    val sqId = StandingQueryId.fresh()
    assert(dgnReg.put(id, dgn, sqId) === true)
    assert(dgnReg.getDomainGraphNode(id).nonEmpty)
    assert(dgnReg.size === 1)
    assert(dgnReg.remove(id, sqId) === true)
    assert(dgnReg.getDomainGraphNode(id).isEmpty)
    assert(dgnReg.size === 0)
    assert(dgnReg.remove(id, sqId) === false)
  }

  test("standing query references ~ variation 2") {
    val dgn = generate1[DomainGraphNode](100)
    val id = DomainGraphNode.id(dgn)
    val sqId1 = StandingQueryId.fresh()
    val sqId2 = StandingQueryId.fresh()
    assert(dgnReg.put(id, dgn, sqId1) === true)
    assert(dgnReg.put(id, dgn, sqId2) === false)
    assert(dgnReg.getDomainGraphNode(id) === Some(dgn))
    assert(dgnReg.size === 1)
    assert(dgnReg.remove(id, sqId1) === false)
    assert(dgnReg.getDomainGraphNode(id).nonEmpty)
    assert(dgnReg.size === 1)
    assert(dgnReg.remove(id, sqId2) === true)
    assert(dgnReg.getDomainGraphNode(id).isEmpty)
    assert(dgnReg.size === 0)
  }

  test("register/unregister ~ variation 1") {
    val dgb = generate1[DomainGraphBranch](10)
    val sqId = StandingQueryId.fresh()
    val id = registerDomainGraphBranch(dgb, sqId)
    assert(dgnReg.getDomainGraphNode(id).nonEmpty)
    val generatedIds = DomainGraphNodePackage(id, dgnReg.getDomainGraphNode(_)).population.keySet
    assert(dgnReg.size === generatedIds.size)
    assert(dgnReg.referenceCount === generatedIds.size)
    assert(persistDomainGraphNodesCalls === generatedIds)
    unregisterDomainGraphNode(id, sqId)
    assert(dgnReg.getDomainGraphNode(id).isEmpty)
    assert(dgnReg.size === 0)
    assert(deleteDomainGraphNodesCalls === generatedIds)
  }

  test("register/unregister ~ variation 2 subsection A") {
    val dgb = generate1[DomainGraphBranch](10)
    val sqId = StandingQueryId.fresh()
    val id = registerDomainGraphBranch(dgb, sqId)
    val generatedIds = DomainGraphNodePackage(id, dgnReg.getDomainGraphNode(_)).population.keySet
    assert(dgnReg.size === generatedIds.size)
    assert(dgnReg.referenceCount === generatedIds.size)
    assert(persistDomainGraphNodesCalls === generatedIds)
    persistDomainGraphNodesCalls = Set.empty
    val notDgb = Not(dgb)
    val notSqId = StandingQueryId.fresh()
    val notId = registerDomainGraphBranch(notDgb, notSqId)
    assert(dgnReg.size === generatedIds.size + 1)
    assert(dgnReg.referenceCount === generatedIds.size * 2 + 1)
    assert(persistDomainGraphNodesCalls === Set(notId))
    unregisterDomainGraphNode(id, sqId)
    assert(dgnReg.size === generatedIds.size + 1)
    assert(dgnReg.referenceCount === generatedIds.size + 1)
    assert(deleteDomainGraphNodesCalls === Set.empty)
    unregisterDomainGraphNode(notId, notSqId)
    assert(dgnReg.size === 0)
    assert(dgnReg.referenceCount === 0)
    assert(deleteDomainGraphNodesCalls === generatedIds + notId)
  }

  test("register/unregister ~ variation 2 subsection B") {
    val dgb = generate1[DomainGraphBranch](10)
    val sqId = StandingQueryId.fresh()
    val id = registerDomainGraphBranch(dgb, sqId)
    val generatedIds = DomainGraphNodePackage(id, dgnReg.getDomainGraphNode(_)).population.keySet
    assert(dgnReg.size === generatedIds.size)
    assert(dgnReg.referenceCount === generatedIds.size)
    assert(persistDomainGraphNodesCalls === generatedIds)
    persistDomainGraphNodesCalls = Set.empty
    val notDgb = Not(dgb)
    val notSqId = StandingQueryId.fresh()
    val notId = registerDomainGraphBranch(notDgb, notSqId)
    assert(dgnReg.size === generatedIds.size + 1)
    assert(dgnReg.referenceCount === generatedIds.size * 2 + 1)
    assert(persistDomainGraphNodesCalls === Set(notId))
    unregisterDomainGraphNode(notId, notSqId)
    assert(dgnReg.size === generatedIds.size)
    assert(dgnReg.referenceCount === generatedIds.size)
    assert(deleteDomainGraphNodesCalls === Set(notId))
    deleteDomainGraphNodesCalls = Set.empty
    unregisterDomainGraphNode(id, sqId)
    assert(dgnReg.size === 0)
    assert(dgnReg.referenceCount === 0)
    assert(deleteDomainGraphNodesCalls === generatedIds)
  }
}
