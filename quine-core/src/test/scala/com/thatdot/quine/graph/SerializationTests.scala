package com.thatdot.quine.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.codecs._

class SerializationTests
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryInstances
    with should.Matchers {

  // This doubles the default size and minimum successful tests
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 200)

  "Binary serialization" should "roundtrip QuineValue" in {
    forAll { (v: QuineValue) =>
      val bytes = QuineValue.writeMsgPack(v)
      assert(QuineValue.readMsgPack(bytes) == v && QuineValue.readMsgPackType(bytes) == v.quineType)
    }
  }

  it should "roundtrip NodeChangeEvent" in {
    forAll { (event: NodeChangeEvent) =>
      assert(NodeChangeEventCodec.format.read(NodeChangeEventCodec.format.write(event)).get == event)
    }
  }

  it should "roundtrip DomainIndexEvent" in {
    forAll { (event: DomainIndexEvent) =>
      assert(DomainIndexEventCodec.format.read(DomainIndexEventCodec.format.write(event)).get == event)
    }
  }

  it should "roundtrip DomainGraphNode" in {
    forAll { (dgn: DomainGraphNode) =>
      assert(DomainGraphNodeCodec.format.read(DomainGraphNodeCodec.format.write(dgn)).get == dgn)
    }
  }

  it should "roundtrip NodeSnapshot" in {
    forAll { (snapshot: NodeSnapshot) =>
      val converted = NodeSnapshot.snapshotCodec.format.read(NodeSnapshot.snapshotCodec.format.write(snapshot)).get
      /* Snapshot is equal up to type of edges iterator returned.*/
      assert(converted.copy(edges = converted.edges.toVector) == snapshot)
    }
  }

  it should "roundtrip StandingQuery" in {
    forAll { (sq: StandingQueryInfo) =>

      val roundTripped = StandingQueryCodec.format.read(StandingQueryCodec.format.write(sq)).get
      /*
      The value "shouldCalculateResultHashCode" is not stored in flatbuffers and is always deserialized to "false",
      so we omit from the comparison for randomly generated test values.
       */
      roundTripped shouldEqual sq.copy(shouldCalculateResultHashCode = false)
    }
  }

  it should "roundtrip StandingQueryState" in {
    forAll { (subs: MultipleValuesStandingQueryPartSubscription, sq: MultipleValuesStandingQueryState) =>
      assert(
        MultipleValuesStandingQueryStateCodec.format
          .read(MultipleValuesStandingQueryStateCodec.format.write(subs -> sq))
          .get == subs -> sq,
      )
    }

  }
}
