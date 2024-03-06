package com.thatdot.quine.graph

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphBranch, DomainGraphNode, DomainGraphNodePackage, Not}

class DomainGraphNodeRegistryTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 10)

  trait WithRegistry {
    val persistDomainGraphNodesCalls: mutable.Set[DomainGraphNodeId] = mutable.Set.empty[DomainGraphNodeId]
    val deleteDomainGraphNodesCalls: mutable.Set[DomainGraphNodeId] = mutable.Set.empty[DomainGraphNodeId]
    val dgnReg: DomainGraphNodeRegistry = new DomainGraphNodeRegistry(
      registerGaugeDomainGraphNodeCount = _ => (),
      persistDomainGraphNodes = p => Future.successful(persistDomainGraphNodesCalls ++= p.keys),
      removeDomainGraphNodes = r => Future.successful(deleteDomainGraphNodesCalls ++= r)
    )
    def registerDomainGraphBranch(dgb: DomainGraphBranch, sqId: StandingQueryId): DomainGraphNodeId = {
      val dgnPackage = dgb.toDomainGraphNodePackage
      Await.result(dgnReg.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId), 1 second)
      dgnPackage.dgnId
    }
    def unregisterDomainGraphNode(dgnId: DomainGraphNodeId, sqId: StandingQueryId): Unit = {
      val dgnPackage = DomainGraphNodePackage(dgnId, dgnReg.getDomainGraphNode(_))
      Await.result(dgnReg.unregisterDomainGraphNodePackage(dgnPackage, sqId), 1 second)
    }
    def getDomainGraphBranch(dgnId: DomainGraphNodeId): DomainGraphBranch =
      dgnReg.getDomainGraphBranch(dgnId).get

  }

  test("getDomainGraphNode ~ id does not exist") {
    new WithRegistry {
      assert(dgnReg.getDomainGraphNode(1L).isEmpty)
    }
  }

  test("getDomainGraphBranch ~ id does not exist") {
    new WithRegistry {
      assert(dgnReg.getDomainGraphBranch(1L).isEmpty)
    }
  }

  test("getDomainGraphNode ~ arbitrary data") {
    forAll { dgb: DomainGraphBranch =>
      new WithRegistry {
        private val sqId = StandingQueryId.fresh()
        val dgnId = registerDomainGraphBranch(dgb, sqId)
        assert(dgnReg.getDomainGraphNode(dgnId).nonEmpty)
      }
    }
  }

  test("getDomainGraphBranch ~ arbitrary data") {
    forAll { dgb: DomainGraphBranch =>
      new WithRegistry {
        private val sqId = StandingQueryId.fresh()
        val dgnId = registerDomainGraphBranch(dgb, sqId)
        assert(dgb === getDomainGraphBranch(dgnId))
      }
    }
  }

  test("standing query references ~ variation 1") {
    forAll { dgn: DomainGraphNode =>
      new WithRegistry {
        val id = DomainGraphNode.id(dgn)
        private val sqId = StandingQueryId.fresh()
        assert(dgnReg.put(id, dgn, sqId) === true)
        assert(dgnReg.getDomainGraphNode(id).nonEmpty)
        assert(dgnReg.size === 1)
        assert(dgnReg.remove(id, sqId) === true)
        assert(dgnReg.getDomainGraphNode(id).isEmpty)
        assert(dgnReg.size === 0)
        assert(dgnReg.remove(id, sqId) === false)
      }
    }
  }

  test("standing query references ~ variation 2") {
    forAll { dgn: DomainGraphNode =>
      new WithRegistry {
        val id = DomainGraphNode.id(dgn)
        private val sqId1 = StandingQueryId.fresh()
        private val sqId2 = StandingQueryId.fresh()
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
    }
  }

  test("register/unregister ~ variation 1") {
    forAll { dgb: DomainGraphBranch =>
      new WithRegistry {
        private val sqId = StandingQueryId.fresh()
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
    }
  }

  test("register/unregister ~ variation 2 subsection A") {
    forAll { dgb: DomainGraphBranch =>
      new WithRegistry {
        private val sqId = StandingQueryId.fresh()
        val id = registerDomainGraphBranch(dgb, sqId)
        val generatedIds = DomainGraphNodePackage(id, dgnReg.getDomainGraphNode(_)).population.keySet
        assert(dgnReg.size === generatedIds.size)
        assert(dgnReg.referenceCount === generatedIds.size)
        assert(persistDomainGraphNodesCalls === generatedIds)
        persistDomainGraphNodesCalls.clear()
        val notDgb = Not(dgb)
        private val notSqId = StandingQueryId.fresh()
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
    }
  }

  test("register/unregister ~ variation 2 subsection B") {
    forAll { dgb: DomainGraphBranch =>
      new WithRegistry {
        private val sqId = StandingQueryId.fresh()
        val id = registerDomainGraphBranch(dgb, sqId)
        val generatedIds = DomainGraphNodePackage(id, dgnReg.getDomainGraphNode(_)).population.keySet
        assert(dgnReg.size === generatedIds.size)
        assert(dgnReg.referenceCount === generatedIds.size)
        assert(persistDomainGraphNodesCalls === generatedIds)
        persistDomainGraphNodesCalls.clear()
        val notDgb = Not(dgb)
        private val notSqId = StandingQueryId.fresh()
        val notId = registerDomainGraphBranch(notDgb, notSqId)
        assert(dgnReg.size === generatedIds.size + 1)
        assert(dgnReg.referenceCount === generatedIds.size * 2 + 1)
        assert(persistDomainGraphNodesCalls === Set(notId))
        unregisterDomainGraphNode(notId, notSqId)
        assert(dgnReg.size === generatedIds.size)
        assert(dgnReg.referenceCount === generatedIds.size)
        assert(deleteDomainGraphNodesCalls === Set(notId))
        deleteDomainGraphNodesCalls.clear()
        unregisterDomainGraphNode(id, sqId)
        assert(dgnReg.size === 0)
        assert(dgnReg.referenceCount === 0)
        assert(deleteDomainGraphNodesCalls === generatedIds)
      }
    }
  }

}
