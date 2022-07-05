package com.thatdot.quine.graph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.behavior.StandingQuerySubscribers
import com.thatdot.quine.graph.cypher.{Expr => CypherExpr, StandingQueryState => CypherStandingQueryState}
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.PersistenceCodecs

// WARNING: Using this can really make compile times explode!!!
// import org.scalacheck.ScalacheckShapeless._

class SerializationTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  // This doubles the default size and minimum successful tests
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 200, minSuccessful = 200)

  import PersistenceCodecs._

  "Binary serialization" should "roundtrip QuineValue" in {
    forAll { (v: QuineValue) =>
      val bytes = QuineValue.writeMsgPack(v)
      QuineValue.readMsgPack(bytes) == v && QuineValue.readMsgPackType(bytes) == v.quineType
    }
  }

  it should "roundtrip NodeChangeEvent.WithTime" in {
    forAll { (event: NodeChangeEvent.WithTime) =>
      eventWithTimeFormat.read(eventWithTimeFormat.write(event)).get == event
    }
  }

  it should "roundtrip NodeChangeEvent" in {
    forAll { (event: NodeChangeEvent) =>
      eventFormat.read(eventFormat.write(event)).get == event
    }
  }

  it should "roundtrip cypher.Expr" in {
    forAll { (e: CypherExpr) =>
      cypherExprFormat.read(cypherExprFormat.write(e)).get == e
    }
  }

  it should "roundtrip DomainGraphBranch" in {
    forAll { (dgb: DomainGraphBranch) =>
      dgbFormat.read(dgbFormat.write(dgb)).get == dgb
    }
  }

  it should "roundtrip NodeSnapshot" in {
    forAll { (snapshot: NodeSnapshot) =>
      nodeSnapshotFormat.read(nodeSnapshotFormat.write(snapshot)).get == snapshot
    }
  }

  it should "roundtrip StandingQuery" in {
    forAll { (sq: StandingQuery) =>
      standingQueryFormat.read(standingQueryFormat.write(sq)).get == sq
    }
  }

  it should "roundtrip StandingQueryState" in {
    forAll { (subs: StandingQuerySubscribers, sq: CypherStandingQueryState) =>
      standingQueryStateFormat.read(standingQueryStateFormat.write(subs -> sq)).get == subs -> sq
    }
  }
}
