package com.thatdot.quine.graph

import java.util.regex

import com.softwaremill.diffx.Diff
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.behavior.MultipleValuesStandingQuerySubscribers
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.codecs._

class SerializationTests
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryInstances
    with DiffShouldMatcher {
  // java.util.regex.Pattern or Array don't have a good equality method
  // So we define the Diff here in terms of things which do have good equality methods / existing Diffs:
  // the string pattern from which the Pattern was compiled, and the Seq version of the Array
  // Without these, we'd get false diffs / test failures from things with the same value not
  // getting reported as equal due to being different instances (not reference equals)
  implicit val regexPatternDiff: Diff[regex.Pattern] = Diff[String].contramap(_.pattern)
  implicit val byteArrayDiff: Diff[Array[Byte]] = Diff[Seq[Byte]].contramap(_.toSeq)

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
    forAll { (sq: StandingQuery) =>

      val roundTripped = StandingQueryCodec.format.read(StandingQueryCodec.format.write(sq)).get
      /*
      The value "shouldCalculateResultHashCode" is not stored in flatbuffers and is always deserialized to "false",
      so we omit from the comparison for randomly generated test values.
       */
      roundTripped shouldMatchTo sq.copy(shouldCalculateResultHashCode = false)
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
