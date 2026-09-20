package com.thatdot.quine.persistor

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.util.TestLogging._

/** The gate that decides whether a store written by one release may be opened by another.
  *
  * Everything about the upgrade story rests on this one comparison, and nothing tested it. A release that writes
  * something an earlier one cannot read has to move the *minor*, because that is the component `canReadFrom`
  * refuses to read backwards; a release that only appends a field an earlier reader skips past moves the patch.
  * 2.2.0 does the former — it adds a `DomainIndexEventUnion` member whose type byte an earlier decoder throws on,
  * and it stops writing a `Subscriber.related_queries` vector that an earlier schema marks `required`. So the
  * claims below are the ones the upgrade and, more importantly, the *refusal of the downgrade* depend on.
  *
  * The concrete pair is stated as a relation against the version 2.1.1 shipped rather than against a literal for
  * the current one, so that a later release moving the version again does not have to edit these, while a release
  * that forgets to move it when it should still fails here.
  */
class PersistenceVersionCompatibilityTest extends AnyFunSuite with Matchers {

  /** The value `PersistenceAgent.CurrentVersion` held in Quine 2.1.1, and therefore what a 2.1.1 store is
    * stamped with. Checked out and read from the `quine/2.1.1` tag rather than remembered.
    */
  private val stampedBy_2_1_1: Version = Version(13, 2, 0)

  private val awaitTimeout = 30.seconds

  // -- canReadFrom, as a truth table ----------------------------------------------------------------------------

  /** Every way two versions can be related, against whether the first may read a store the second wrote. */
  private val cases: List[(Version, Version, Boolean, String)] = List(
    (Version(13, 3, 0), Version(13, 3, 0), true, "the same version"),
    (Version(13, 3, 0), Version(13, 2, 0), true, "an older minor, which is what an upgrade reads"),
    (Version(13, 3, 0), Version(13, 0, 0), true, "a much older minor"),
    (Version(13, 2, 0), Version(13, 3, 0), false, "a newer minor, which is what a downgrade would read"),
    (Version(13, 3, 1), Version(13, 3, 0), true, "an older patch"),
    (Version(13, 3, 0), Version(13, 3, 1), true, "a newer patch, which is readable because patches only append"),
    (Version(13, 3, 0), Version(12, 3, 0), false, "an older major"),
    (Version(13, 3, 0), Version(14, 3, 0), false, "a newer major"),
    (Version(13, 3, 0), Version(14, 0, 0), false, "a newer major with an older minor"),
    (Version(13, 0, 0), Version(12, 9, 9), false, "an older major with a newer minor"),
  )

  cases.foreach { case (reader, onDisk, expected, label) =>
    test(s"a $reader ${if (expected) "reads" else "refuses"} a store written by $onDisk: $label") {
      reader.canReadFrom(onDisk) shouldBe expected
    }
  }

  // -- the concrete release pair --------------------------------------------------------------------------------

  test("the current format reads a store stamped by 2.1.1, which is what makes the upgrade possible") {
    PersistenceAgent.CurrentVersion.canReadFrom(stampedBy_2_1_1) shouldBe true
  }

  test("2.1.1 refuses a store stamped by the current format, which is what makes the downgrade fail at startup") {
    // The point of moving the minor. Without this, a 2.1.1 node would open a store holding 2.2.0 snapshots and
    // journal rows and fail one node at a time, at wake, on a `required` field that is no longer written and on
    // a union member it has no case for.
    stampedBy_2_1_1.canReadFrom(PersistenceAgent.CurrentVersion) shouldBe false
  }

  test("the current format is strictly newer than 2.1.1's, so a store's stamp is rewritten rather than left") {
    // `syncVersion` only rewrites the stamp when the running version is greater; equal or lower leaves it. An
    // upgrade must rewrite, or the store would go on claiming it can be read by 2.1.1.
    PersistenceAgent.CurrentVersion should be > stampedBy_2_1_1
  }

  // -- the bytes ------------------------------------------------------------------------------------------------

  test("a version round-trips through the three bytes the metadata row holds") {
    cases.flatMap { case (a, b, _, _) => List(a, b) }.distinct.foreach { v =>
      withClue(s"$v: ")(Version.fromBytes(v.toBytes) shouldBe Some(v))
    }
  }

  test("the bytes a 2.1.1 store holds parse to the version it stamped") {
    Version.fromBytes(Array[Byte](13, 2, 0)) shouldBe Some(stampedBy_2_1_1)
  }

  test("a metadata value of the wrong length is not mistaken for a version") {
    Version.fromBytes(Array.empty) shouldBe None
    Version.fromBytes(Array[Byte](13, 2)) shouldBe None
    Version.fromBytes(Array[Byte](13, 2, 0, 0)) shouldBe None
  }

  // -- syncVersion, against a real store ------------------------------------------------------------------------

  private val systemNames = Iterator.from(1)

  private def withPersistor(body: PrimePersistor => Any): Unit = {
    val system = ActorSystem(s"persistence-version-compatibility-${systemNames.next()}")
    try {
      val prime = new StatelessPrimePersistor(
        PersistenceConfig(),
        None,
        (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
      )(Materializer.matFromSystem(system), logConfig)
      val _ = body(prime)
    } finally {
      val _ = Await.result(system.terminate(), awaitTimeout)
    }
  }

  private val versionKey: String = PersistenceAgent.VersionMetadataKey

  private def stampOf(prime: PrimePersistor): Option[Version] =
    Await.result(prime.getMetaData(versionKey), awaitTimeout).flatMap(Version.fromBytes(_))

  private def sync(prime: PrimePersistor, running: Version, dataEmpty: Boolean): Unit =
    Await.result(
      prime.syncVersion("test data", versionKey, running, () => Future.successful(dataEmpty)),
      awaitTimeout,
    )

  test("an unstamped store is stamped with the running version") {
    withPersistor { prime =>
      stampOf(prime) shouldBe None
      sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = true)
      stampOf(prime) shouldBe Some(PersistenceAgent.CurrentVersion)
    }
  }

  test("a store stamped by 2.1.1 is accepted and restamped, with data present") {
    // The upgrade itself. `dataEmpty = false` because a real upgrade has data; the point is that it is accepted
    // without the emptiness escape hatch being needed.
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(stampedBy_2_1_1.toBytes)), awaitTimeout)
      sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = false)
      withClue("the store now claims the format it actually holds: ")(
        stampOf(prime) shouldBe Some(PersistenceAgent.CurrentVersion),
      )
    }
  }

  test("a store stamped by a newer minor is refused when it holds data") {
    // The downgrade, from the other side: 2.1.1 started against a store 2.2.0 has already opened.
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(PersistenceAgent.CurrentVersion.toBytes)), awaitTimeout)
      val failure = intercept[IncompatibleVersion](sync(prime, stampedBy_2_1_1, dataEmpty = false))
      failure.getMessage should include(PersistenceAgent.CurrentVersion.toString)
      withClue("and the stamp is left alone, so the store is still readable by the newer release: ")(
        stampOf(prime) shouldBe Some(PersistenceAgent.CurrentVersion),
      )
    }
  }

  test("a store stamped by a newer minor is reclaimed when it holds no data") {
    // The escape hatch, and the reason the refusal above passes `dataEmpty = false`: a store that was stamped
    // but never written to is not an upgrade problem, and an operator who starts the older release against one
    // should not have to clear it by hand.
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(PersistenceAgent.CurrentVersion.toBytes)), awaitTimeout)
      sync(prime, stampedBy_2_1_1, dataEmpty = true)
      stampOf(prime) shouldBe Some(stampedBy_2_1_1)
    }
  }

  test("a store stamped by an incompatible major is refused whichever way it goes") {
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(Version(12, 9, 9).toBytes)), awaitTimeout)
      val _ = intercept[IncompatibleVersion](sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = false))
    }
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(Version(14, 0, 0).toBytes)), awaitTimeout)
      val _ = intercept[IncompatibleVersion](sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = false))
    }
  }

  test("a stamp that is not three bytes fails rather than being read as some version") {
    withPersistor { prime =>
      Await.result(prime.setMetaData(versionKey, Some(Array[Byte](13, 3))), awaitTimeout)
      val failure = intercept[IllegalStateException](sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = false))
      failure.getMessage should include("cannot parse version")
    }
  }

  test("re-running the same version over its own stamp changes nothing") {
    withPersistor { prime =>
      sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = true)
      sync(prime, PersistenceAgent.CurrentVersion, dataEmpty = false)
      stampOf(prime) shouldBe Some(PersistenceAgent.CurrentVersion)
    }
  }
}
