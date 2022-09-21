package com.thatdot.quine.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.behavior.StandingQuerySubscribers
import com.thatdot.quine.graph.cypher.{Expr => CypherExpr, StandingQueryState => CypherStandingQueryState}
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.PersistenceCodecs

class SerializationTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  // This doubles the default size and minimum successful tests
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 200)

  import PersistenceCodecs._

  "Binary serialization" should "roundtrip QuineValue" in {
    forAll { (v: QuineValue) =>
      val bytes = QuineValue.writeMsgPack(v)
      assert(QuineValue.readMsgPack(bytes) == v && QuineValue.readMsgPackType(bytes) == v.quineType)
    }
  }

  it should "roundtrip NodeEvent.WithTime" in {
    forAll { (event: NodeEvent.WithTime) =>
      assert(eventWithTimeFormat.read(eventWithTimeFormat.write(event)).get == event)
    }
  }

  it should "roundtrip NodeChangeEvent" in {
    forAll { (event: NodeChangeEvent) =>
      assert(eventFormat.read(eventFormat.write(event)).get == event)
    }
  }

  it should "roundtrip DomainIndexEvent" in {
    forAll { (event: DomainIndexEvent) =>
      assert(eventFormat.read(eventFormat.write(event)).get == event)
    }
  }

  it should "roundtrip cypher.Expr" in {
    forAll { (e: CypherExpr) =>
      assert(cypherExprFormat.read(cypherExprFormat.write(e)).get == e)
    }
  }

  it should "roundtrip DomainGraphNode" in {
    forAll { (dgn: DomainGraphNode) =>
      assert(domainGraphNodeFormat.read(domainGraphNodeFormat.write(dgn)).get == dgn)
    }
  }

  it should "roundtrip NodeSnapshot" in {

    def toVector[T](i: Iterable[T]): Vector[T] = Vector() ++ i

    forAll { (snapshot: NodeSnapshot) =>
      val converted = nodeSnapshotFormat.read(nodeSnapshotFormat.write(snapshot)).get
      /* Snapshot is equal up to type of edges iterator returned.*/
      assert(converted.copy(edges = toVector(converted.edges)) == snapshot)
    }
  }

  it should "roundtrip StandingQuery" in {
    forAll { (sq: StandingQuery) =>
      /*
      The value "shouldCalculateResultHashCode" is not stored in flatbuffers and is always deserialized to "false",
      so we omit from the comparison for randomly generated test values.
       */
      assert(
        standingQueryFormat.read(standingQueryFormat.write(sq)).get == sq.copy(shouldCalculateResultHashCode = false)
      )
    }
  }

  it should "roundtrip StandingQueryState" in {
    forAll { (subs: StandingQuerySubscribers, sq: CypherStandingQueryState) =>
      assert(standingQueryStateFormat.read(standingQueryStateFormat.write(subs -> sq)).get == subs -> sq)
    }

  }
}
