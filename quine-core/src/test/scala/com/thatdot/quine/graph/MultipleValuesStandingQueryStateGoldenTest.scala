package com.thatdot.quine.graph

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.{behavior, cypher}
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec

/** Reads standing query state bytes written by an earlier version of the codec.
  *
  * The round-trip property in [[SerializationTests]] checks that the codec agrees with itself; this checks that it
  * still agrees with the format already on disk in every deployment. A change that alters both the writer and the
  * reader consistently passes the round trip and fails here, which is the point.
  *
  * A failure here means one of two things. Either the change to the format was accidental, in which case the format is
  * what needs fixing, or it was deliberate and accompanied by a migration, in which case the expectation belongs in
  * this test, stated as what the old bytes are now expected to mean.
  */
class MultipleValuesStandingQueryStateGoldenTest extends AnyFunSuite {

  private def decodedKinds(fixtures: Seq[MultipleValuesStandingQueryStateFixtures.Fixture]): Set[String] =
    fixtures.zipWithIndex.map { case (fixture, index) =>
      val (subscription, state) = MultipleValuesStandingQueryStateCodec.format
        .read(fixture.bytes)
        .fold(error => fail(s"Fixture $index failed to deserialize: ${fixture.description}", error), identity)
      assert(
        MultipleValuesStandingQueryStateFixtures.describe(subscription, state) == fixture.description,
        s"fixture $index decoded to a different value than when it was captured",
      )
      MultipleValuesStandingQueryStateFixtures.kindOf(state)
    }.toSet

  test("state written by an earlier version of the codec still reads back unchanged") {
    val kindsCovered = decodedKinds(MultipleValuesStandingQueryStateFixtures.load())
    val missing = MultipleValuesStandingQueryStateFixtures.expectedKinds.filterNot(kindsCovered)
    assert(missing.isEmpty, s"kinds of state with no old bytes to read: ${missing.mkString(", ")}")
  }

  test("a state this work did not change writes the same bytes it always did") {
    // Reading old bytes says the format is still understood. It does not say that what is written now is that same
    // format, and a change to the writer that nothing reads back differently would pass the test above unnoticed.
    //
    // Only fixtures whose encoding is deterministic: a subscription holds its subscribers in a set, and a state may
    // hold its rows in a map, neither of which iterates in a fixed order.
    var compared = Set.empty[String]
    deterministicFixtures.foreach { case (index, fixture, pair) =>
      val kind = MultipleValuesStandingQueryStateFixtures.kindOf(pair._2)
      compared += kind
      assert(
        MultipleValuesStandingQueryStateCodec.format.write(pair).toSeq == fixture.bytes.toSeq,
        s"fixture $index (${fixture.description}) is written differently now than when it was captured",
      )
    }
    // Named rather than counted, so that a fixture set which stops containing one of these fails here instead of
    // quietly comparing nothing.
    assert(compared == unchangedKinds, s"nothing to compare for: ${(unchangedKinds -- compared).mkString(", ")}")
  }

  test("a reciprocal holding its subscribers in the node is never written larger than it used to be") {
    // The reciprocal is the one state whose written bytes this work does change, in two ways that pull opposite
    // ways. It gained two appended fields, which must cost nothing while they hold their defaults (that is the
    // whole basis for saying an ordinary node's blob is unaffected), and it stopped writing `currently_matching`
    // from the state, which is deliberate and takes three bytes off a blob that used to carry it as true.
    //
    // So the claim that can be made against captured bytes is that the total never goes up. If the appended fields
    // ever stopped being elided, it would.
    var compared = 0
    reciprocalFixtures.foreach { case (index, fixture, pair) =>
      val rewritten = MultipleValuesStandingQueryStateCodec.format.write(pair)
      assert(
        rewritten.length <= fixture.bytes.length,
        s"fixture $index (${fixture.description}) is ${rewritten.length - fixture.bytes.length} bytes larger than " +
        "when it was captured, so a state keeping its subscribers in the node is paying for fields it does not use",
      )
      compared += 1
    }
    assert(compared > 0, "no reciprocal fixture was compared, so this measured nothing")
  }

  /** Kinds whose bytes this work does not change at all. */
  private val unchangedKinds: Set[String] = Set("localProperty", "unit", "localId")

  private type Loaded = (
    Int,
    MultipleValuesStandingQueryStateFixtures.Fixture,
    (behavior.MultipleValuesStandingQueryPartSubscription, cypher.MultipleValuesStandingQueryState),
  )

  private def allFixtures: Seq[Loaded] =
    (MultipleValuesStandingQueryStateFixtures.load() ++
      MultipleValuesStandingQueryStateFixtures.load(
        MultipleValuesStandingQueryStateFixtures.foldResourcePath,
      )).zipWithIndex
      .map { case (fixture, index) =>
        val pair = MultipleValuesStandingQueryStateCodec.format
          .read(fixture.bytes)
          .fold(error => fail(s"Fixture $index failed to deserialize: ${fixture.description}", error), identity)
        (index, fixture, pair)
      }

  private def deterministicFixtures: Seq[Loaded] =
    allFixtures.filter { case (_, _, pair @ (subscription, state)) =>
      val _ = pair
      subscription.subscribers.size <= 1 && unchangedKinds.contains(
        MultipleValuesStandingQueryStateFixtures.kindOf(state),
      )
    }

  private def reciprocalFixtures: Seq[Loaded] =
    allFixtures.filter { case (_, _, (subscription, state)) =>
      subscription.subscribers.size <= 1 &&
        MultipleValuesStandingQueryStateFixtures.kindOf(state) == "reciprocal"
    }

  test("reciprocal states keyed per half edge by an earlier funnel still read back unchanged") {
    val kindsCovered = decodedKinds(
      MultipleValuesStandingQueryStateFixtures.load(MultipleValuesStandingQueryStateFixtures.foldResourcePath),
    )
    // The scenario is the far side of a SubscribeAcrossEdge: the per-root reciprocals and the state they subscribe to.
    assert(kindsCovered == Set("reciprocal", "localProperty"))
  }
}
