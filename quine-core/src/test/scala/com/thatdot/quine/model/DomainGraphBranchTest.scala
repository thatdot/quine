package com.thatdot.quine.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.ArbitraryInstances

class DomainGraphBranchTest extends AnyFunSuite with ArbitraryInstances with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 1000)

  test("DomainGraphBranch to/from DomainGraphNode") {
    forAll { dgb1: DomainGraphBranch =>
      val DomainGraphNodePackage(dgnId, descendants) = dgb1.toDomainGraphNodePackage
      val dgb2 = DomainGraphBranch.fromDomainGraphNodeId(dgnId, descendants.get)
      assert(dgb1 === dgb2.get)
    }
  }

}
