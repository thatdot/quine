package com.thatdot.quine.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQuerySubscribers
import com.thatdot.quine.graph.cypher.{Expr => CypherExpr, MultipleValuesStandingQueryState}
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.codecs.NodeEventCodec.eventWithTimeFormat
import com.thatdot.quine.persistor.codecs._

class SerializationTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  // This doubles the default size and minimum successful tests
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 200)

  "Binary serialization" should "roundtrip QuineValue" in {
    forAll { (v: QuineValue) =>
      val bytes = QuineValue.writeMsgPack(v)
      assert(QuineValue.readMsgPack(bytes) == v && QuineValue.readMsgPackType(bytes) == v.quineType)
    }
  }

  "Binary serialization" should "roundtrip Duration" in {
    forAll { (v: java.time.Duration) =>
      val qv = QuineValue.Duration(v)
      val bytes = QuineValue.writeMsgPack(qv)
      assert(QuineValue.readMsgPack(bytes) == qv && QuineValue.readMsgPackType(bytes) == qv.quineType)
    }
  }

  it should "eventWithTime format safely encapsulates event" in {
    forAll { (event: NodeEvent.WithTime) =>
      assert(eventWithTimeFormat.read(eventWithTimeFormat.write(event)).get == event)
    }
  }

  it should "roundtrip NodeChangeEvent" in {
    forAll { (event: NodeChangeEvent) =>
      assert(NodeEventCodec.format.read(NodeEventCodec.format.write(event)).get == event)
    }
  }

  it should "roundtrip DomainIndexEvent" in {
    forAll { (event: DomainIndexEvent) =>
      assert(DomainIndexEventCodec.format.read(DomainIndexEventCodec.format.write(event)).get == event)
    }
  }

  it should "roundtrip cypher.Expr" in {
    forAll { (e: CypherExpr) =>
      assert(CypherExprCodec.format.read(CypherExprCodec.format.write(e)).get == e)
    }
  }

  it should "roundtrip DomainGraphNode" in {
    forAll { (dgn: DomainGraphNode) =>
      assert(DomainGraphNodeCodec.format.read(DomainGraphNodeCodec.format.write(dgn)).get == dgn)
    }
  }

  it should "roundtrip NodeSnapshot" in {
    forAll { (snapshot: NodeSnapshot) =>
      val converted = SnapshotCodec.format.read(SnapshotCodec.format.write(snapshot)).get
      /* Snapshot is equal up to type of edges iterator returned.*/
      assert(converted.copy(edges = converted.edges.toVector) == snapshot)
    }
  }

  it should "roundtrip StandingQuery" in {
    forAll { (sq: StandingQuery) =>
      /*
      The value "shouldCalculateResultHashCode" is not stored in flatbuffers and is always deserialized to "false",
      so we omit from the comparison for randomly generated test values.
       */
      assert(
        StandingQueryCodec.format.read(StandingQueryCodec.format.write(sq)).get == sq
          .copy(shouldCalculateResultHashCode = false)
      )
    }
  }

  it should "roundtrip StandingQueryState" in {
    forAll { (subs: MultipleValuesStandingQuerySubscribers, sq: MultipleValuesStandingQueryState) =>
      assert(
        MultipleValuesStandingQueryStateCodec.format
          .read(MultipleValuesStandingQueryStateCodec.format.write(subs -> sq))
          .get == subs -> sq
      )
    }

  }
}
